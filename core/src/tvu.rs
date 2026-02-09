//! The `tvu` module implements the Transaction Validation Unit, a multi-stage transaction
//! validation pipeline in software.

use {
    crate::{
        admin_rpc_post_init::{KeyUpdaterType, KeyUpdaters},
        banking_trace::BankingTracer,
        block_creation_loop::ReplayHighestFrozen,
        bls_sigverify::bls_sigverify_service::BLSSigVerifyService,
        cluster_info_vote_listener::{
            DuplicateConfirmedSlotsReceiver, GossipVerifiedVoteHashReceiver, VerifiedVoteReceiver,
            VerifiedVoteSender, VoteTracker,
        },
        cluster_slots_service::{cluster_slots::ClusterSlots, ClusterSlotsService},
        commitment_service::AggregateCommitmentService,
        completed_data_sets_service::CompletedDataSetsSender,
        consensus::{tower_storage::TowerStorage, Tower},
        cost_update_service::CostUpdateService,
        drop_bank_service::DropBankService,
        repair::{
            block_id_repair_service::BlockIdRepairChannels,
            repair_service::{OutstandingShredRepairs, RepairInfo, RepairServiceChannels},
        },
        replay_stage::{ReplayReceivers, ReplaySenders, ReplayStage, ReplayStageConfig},
        shred_fetch_stage::{ShredFetchStage, SHRED_FETCH_CHANNEL_SIZE},
        validator::TurbineMode,
        voting_service::VotingService,
        warm_quic_cache_service::WarmQuicCacheService,
        window_service::{WindowService, WindowServiceChannels},
    },
    bytes::Bytes,
    crossbeam_channel::{bounded, unbounded, Receiver, Sender},
    solana_client::connection_cache::ConnectionCache,
    solana_clock::Slot,
    solana_geyser_plugin_manager::block_metadata_notifier_interface::BlockMetadataNotifierArc,
    solana_gossip::{
        cluster_info::ClusterInfo, duplicate_shred_handler::DuplicateShredHandler,
        duplicate_shred_listener::DuplicateShredListener,
    },
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::{
        block_location_lookup::BlockLocationLookup,
        blockstore::{Blockstore, UpdateParentReceiver, MAX_COMPLETED_SLOTS_IN_CHANNEL},
        blockstore_cleanup_service::BlockstoreCleanupService,
        blockstore_processor::TransactionStatusSender,
        entry_notifier_service::EntryNotifierSender,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_poh::{poh_controller::PohController, poh_recorder::PohRecorder},
    solana_pubkey::Pubkey,
    solana_rpc::{
        alpenglow_last_voted::AlpenglowLastVoted, max_slots::MaxSlots,
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions, slot_status_notifier::SlotStatusNotifier,
    },
    solana_runtime::{
        bank_forks::BankForks, commitment::BlockCommitmentCache,
        prioritization_fee_cache::PrioritizationFeeCache, snapshot_controller::SnapshotController,
        validated_block_finalization::ValidatedBlockFinalizationCert,
        vote_sender_types::ReplayVoteSender,
    },
    solana_streamer::{
        evicting_sender::EvictingSender,
        quic::{spawn_server, QuicServerParams, SpawnServerResult},
        streamer::StakedNodes,
    },
    solana_turbine::{retransmit_stage::RetransmitStage, xdp::XdpSender},
    solana_votor::{
        consensus_metrics::MAX_IN_FLIGHT_CONSENSUS_EVENTS,
        event::{LeaderWindowInfo, VotorEventReceiver, VotorEventSender},
        vote_history::VoteHistory,
        vote_history_storage::VoteHistoryStorage,
        voting_service::{VotingService as AlpenglowVotingService, VotingServiceOverride},
        votor::{Votor, VotorConfig},
    },
    solana_votor_messages::reward_certificate::{
        BuildRewardCertsRequest, BuildRewardCertsResponse,
    },
    std::{
        collections::HashSet,
        net::{SocketAddr, UdpSocket},
        num::NonZeroUsize,
        sync::{atomic::AtomicBool, Arc, RwLock},
        thread::{self, JoinHandle},
    },
    tokio::sync::mpsc::Sender as AsyncSender,
};

/// Sets the upper bound on the number of batches stored in the retransmit
/// stage ingress channel.
/// Allows for a max of 16k batches of up to 64 packets each
/// (PACKETS_PER_BATCH).
/// This translates to about 1 GB of RAM for packet storage in the worst case.
/// In reality this means about 200K shreds since most batches are not full.
const CHANNEL_SIZE_RETRANSMIT_INGRESS: usize = 16 * 1024;

pub struct Tvu {
    fetch_stage: ShredFetchStage,
    shred_sigverify: JoinHandle<()>,
    retransmit_stage: RetransmitStage,
    window_service: WindowService,
    cluster_slots_service: ClusterSlotsService,
    replay_stage: Option<ReplayStage>,
    blockstore_cleanup_service: Option<BlockstoreCleanupService>,
    cost_update_service: CostUpdateService,
    voting_service: VotingService,
    alpenglow_voting_service: AlpenglowVotingService,
    warm_quic_cache_service: Option<WarmQuicCacheService>,
    drop_bank_service: DropBankService,
    duplicate_shred_listener: DuplicateShredListener,
    alpenglow_sigverify_service: BLSSigVerifyService,
    alpenglow_quic_t: thread::JoinHandle<()>,
    votor: Votor,
    commitment_service: AggregateCommitmentService,
}

pub struct TvuSockets {
    pub fetch: Vec<UdpSocket>,
    pub repair: UdpSocket,
    pub retransmit: Vec<UdpSocket>,
    pub ancestor_hashes_requests: UdpSocket,
    pub alpenglow_quic: UdpSocket,
    pub block_id_repair: UdpSocket,
}

pub struct TvuConfig {
    pub max_ledger_shreds: Option<u64>,
    pub shred_version: u16,
    // Validators from which repairs are requested
    pub repair_validators: Option<HashSet<Pubkey>>,
    // Validators which should be given priority when serving repairs
    pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    pub wait_for_vote_to_start_leader: bool,
    pub replay_forks_threads: NonZeroUsize,
    pub replay_transactions_threads: NonZeroUsize,
    pub shred_sigverify_threads: NonZeroUsize,
    pub xdp_sender: Option<XdpSender>,
}

impl Default for TvuConfig {
    fn default() -> Self {
        Self {
            max_ledger_shreds: None,
            shred_version: 0,
            repair_validators: None,
            repair_whitelist: Arc::new(RwLock::new(HashSet::default())),
            wait_for_vote_to_start_leader: false,
            replay_forks_threads: NonZeroUsize::new(1).expect("1 is non-zero"),
            replay_transactions_threads: NonZeroUsize::new(1).expect("1 is non-zero"),
            shred_sigverify_threads: NonZeroUsize::new(1).expect("1 is non-zero"),
            xdp_sender: None,
        }
    }
}

/// Shared state from validator necessary to instantiate votor and related services
pub struct AlpenglowInitializationState {
    // Shared with block creation loop
    pub leader_window_info_sender: Sender<LeaderWindowInfo>,
    pub optimistic_parent_sender: Sender<LeaderWindowInfo>,
    pub replay_highest_frozen: Arc<ReplayHighestFrozen>,
    pub highest_parent_ready: Arc<RwLock<(Slot, (Slot, Hash))>>,

    // Main communication channel
    pub votor_event_sender: VotorEventSender,
    pub votor_event_receiver: VotorEventReceiver,

    // For BLS streamer setup
    pub staked_nodes: Arc<RwLock<StakedNodes>>,
    pub key_notifiers: Arc<RwLock<KeyUpdaters>>,
    pub alpenglow_quic_server_config: QuicServerParams,

    // For BLS voting service
    pub bls_connection_cache: Arc<ConnectionCache>,
    pub voting_service_test_override: Option<VotingServiceOverride>,
    pub alpenglow_last_voted: Arc<AlpenglowLastVoted>,

    // For rewards
    pub reward_certs_sender: Sender<BuildRewardCertsResponse>,
    pub build_reward_certs_receiver: Receiver<BuildRewardCertsRequest>,
    pub highest_finalized: Arc<RwLock<Option<ValidatedBlockFinalizationCert>>>,
}

impl Tvu {
    /// This service receives messages from a leader in the network and processes the transactions
    /// on the bank state.
    /// # Arguments
    /// * `cluster_info` - The cluster_info state.
    /// * `sockets` - fetch, repair, and retransmit sockets
    /// * `blockstore` - the ledger itself
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vote_account: &Pubkey,
        authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
        identity_keypair: Arc<Keypair>,
        bank_forks: &Arc<RwLock<BankForks>>,
        cluster_info: &Arc<ClusterInfo>,
        sockets: TvuSockets,
        blockstore: Arc<Blockstore>,
        ledger_signal_receiver: Receiver<bool>,
        update_parent_receiver: UpdateParentReceiver,
        rpc_subscriptions: Option<Arc<RpcSubscriptions>>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        poh_controller: PohController,
        tower: Tower,
        tower_storage: Arc<dyn TowerStorage>,
        vote_history: VoteHistory,
        vote_history_storage: Arc<dyn VoteHistoryStorage>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        exit: Arc<AtomicBool>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        turbine_mode: TurbineMode,
        transaction_status_sender: Option<TransactionStatusSender>,
        entry_notification_sender: Option<EntryNotifierSender>,
        vote_tracker: Arc<VoteTracker>,
        retransmit_slots_sender: Sender<Slot>,
        gossip_verified_vote_hash_receiver: GossipVerifiedVoteHashReceiver,
        verified_vote_sender: VerifiedVoteSender,
        verified_vote_receiver: VerifiedVoteReceiver,
        replay_vote_sender: ReplayVoteSender,
        completed_data_sets_sender: Option<CompletedDataSetsSender>,
        bank_notification_sender: Option<BankNotificationSenderConfig>,
        duplicate_confirmed_slots_receiver: DuplicateConfirmedSlotsReceiver,
        tvu_config: TvuConfig,
        max_slots: &Arc<MaxSlots>,
        block_metadata_notifier: Option<BlockMetadataNotifierArc>,
        wait_to_vote_slot: Option<Slot>,
        snapshot_controller: Option<Arc<SnapshotController>>,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Option<&Arc<ConnectionCache>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        banking_tracer: Arc<BankingTracer>,
        turbine_quic_endpoint_sender: AsyncSender<(SocketAddr, Bytes)>,
        turbine_quic_endpoint_receiver: Receiver<(Pubkey, SocketAddr, Bytes)>,
        repair_response_quic_receiver: Receiver<(Pubkey, SocketAddr, Bytes)>,
        repair_request_quic_sender: AsyncSender<(SocketAddr, Bytes)>,
        ancestor_hashes_request_quic_sender: AsyncSender<(SocketAddr, Bytes)>,
        ancestor_hashes_response_quic_receiver: Receiver<(Pubkey, SocketAddr, Bytes)>,
        outstanding_repair_requests: Arc<RwLock<OutstandingShredRepairs>>,
        cluster_slots: Arc<ClusterSlots>,
        wen_restart_repair_slots: Option<Arc<RwLock<Vec<Slot>>>>,
        slot_status_notifier: Option<SlotStatusNotifier>,
        vote_connection_cache: Arc<ConnectionCache>,
        alpenglow_init: AlpenglowInitializationState,
    ) -> Result<Self, String> {
        let AlpenglowInitializationState {
            leader_window_info_sender,
            replay_highest_frozen,
            highest_parent_ready,
            votor_event_sender,
            votor_event_receiver,
            staked_nodes,
            key_notifiers,
            alpenglow_quic_server_config,
            bls_connection_cache,
            voting_service_test_override,
            alpenglow_last_voted,
            reward_certs_sender,
            build_reward_certs_receiver,
            highest_finalized,
            optimistic_parent_sender,
        } = alpenglow_init;

        let (consensus_message_sender, consensus_message_receiver) =
            bounded(MAX_IN_FLIGHT_CONSENSUS_EVENTS);
        let (consensus_metrics_sender, consensus_metrics_receiver) =
            bounded(MAX_IN_FLIGHT_CONSENSUS_EVENTS);

        let in_wen_restart = wen_restart_repair_slots.is_some();
        let migration_status = bank_forks.read().unwrap().migration_status();

        let TvuSockets {
            repair: repair_socket,
            fetch: fetch_sockets,
            retransmit: retransmit_sockets,
            ancestor_hashes_requests: ancestor_hashes_socket,
            alpenglow_quic: alpenglow_quic_socket,
            block_id_repair,
        } = sockets;

        let (fetch_sender, fetch_receiver) = EvictingSender::new_bounded(SHRED_FETCH_CHANNEL_SIZE);
        let (bls_packet_sender, bls_packet_receiver) = bounded(MAX_IN_FLIGHT_CONSENSUS_EVENTS);

        let repair_socket = Arc::new(repair_socket);
        let ancestor_hashes_socket = Arc::new(ancestor_hashes_socket);
        let block_id_repair_socket = Arc::new(block_id_repair);
        let fetch_sockets: Vec<Arc<UdpSocket>> = fetch_sockets.into_iter().map(Arc::new).collect();
        let block_location_lookup = BlockLocationLookup::new_arc();
        let fetch_stage = ShredFetchStage::new(
            fetch_sockets,
            turbine_quic_endpoint_receiver,
            repair_response_quic_receiver,
            repair_socket.clone(),
            fetch_sender,
            tvu_config.shred_version,
            bank_forks.clone(),
            cluster_info.clone(),
            outstanding_repair_requests.clone(),
            turbine_mode,
            exit.clone(),
        );

        // Streamer for Alpenglow
        let SpawnServerResult {
            endpoints: _,
            thread: alpenglow_quic_t,
            key_updater: alpenglow_stream_key_updater,
        } = spawn_server(
            "solQuicAlpglw",
            "quic_streamer_alpenglow",
            vec![alpenglow_quic_socket],
            &identity_keypair,
            bls_packet_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            alpenglow_quic_server_config,
        )
        .unwrap();

        // At the moment there are roughly 1K validators and the sigverifier receives votes in batches and sends them to the consensus reward container in batches so hopefully using a channel of 2K slots would never block.
        let (reward_votes_sender, reward_votes_receiver) = bounded(2000);
        let alpenglow_sigverify_service = BLSSigVerifyService::new(
            bls_packet_receiver,
            bank_forks.read().unwrap().sharable_banks(),
            verified_vote_sender.clone(),
            reward_votes_sender,
            consensus_message_sender.clone(),
            consensus_metrics_sender.clone(),
            alpenglow_last_voted.clone(),
            cluster_info.clone(),
            leader_schedule_cache.clone(),
        );

        let mut key_notifiers = key_notifiers.write().unwrap();
        key_notifiers.add(KeyUpdaterType::Bls, alpenglow_stream_key_updater);

        let (verified_sender, verified_receiver) = unbounded();

        let (retransmit_sender, retransmit_receiver) =
            EvictingSender::new_bounded(CHANNEL_SIZE_RETRANSMIT_INGRESS);

        let shred_sigverify = solana_turbine::sigverify_shreds::spawn_shred_sigverify(
            cluster_info.clone(),
            bank_forks.clone(),
            leader_schedule_cache.clone(),
            fetch_receiver,
            retransmit_sender.clone(),
            verified_sender,
            block_location_lookup.clone(),
            tvu_config.shred_sigverify_threads,
        );

        let retransmit_stage = RetransmitStage::new(
            bank_forks.clone(),
            leader_schedule_cache.clone(),
            cluster_info.clone(),
            Arc::new(retransmit_sockets),
            turbine_quic_endpoint_sender,
            retransmit_receiver,
            max_slots.clone(),
            rpc_subscriptions.clone(),
            slot_status_notifier.clone(),
            tvu_config.xdp_sender,
            votor_event_sender.clone(),
            migration_status.clone(),
        );

        let (ancestor_duplicate_slots_sender, ancestor_duplicate_slots_receiver) = unbounded();
        let (duplicate_slots_sender, duplicate_slots_receiver) = unbounded();
        let (ancestor_hashes_replay_update_sender, ancestor_hashes_replay_update_receiver) =
            unbounded();
        let (dumped_slots_sender, dumped_slots_receiver) = unbounded();
        let (popular_pruned_forks_sender, popular_pruned_forks_receiver) = unbounded();

        let epoch_schedule = bank_forks
            .read()
            .unwrap()
            .working_bank()
            .epoch_schedule()
            .clone();
        let repair_info = RepairInfo {
            bank_forks: bank_forks.clone(),
            epoch_schedule,
            ancestor_duplicate_slots_sender,
            repair_validators: tvu_config.repair_validators,
            repair_whitelist: tvu_config.repair_whitelist,
            cluster_info: cluster_info.clone(),
            cluster_slots: cluster_slots.clone(),
            wen_restart_repair_slots,
            block_location_lookup: block_location_lookup.clone(),
        };

        // Create repair event channel for BlockIdRepairService
        let (repair_event_sender, repair_event_receiver) = bounded(100);

        // Create completed slots channel for BlockIdRepairService
        let (completed_slots_sender, completed_slots_receiver) =
            bounded(MAX_COMPLETED_SLOTS_IN_CHANNEL);
        blockstore.add_completed_slots_signal(completed_slots_sender);

        let block_id_repair_channels = BlockIdRepairChannels {
            repair_event_receiver,
            completed_slots_receiver,
        };

        // Create switch block event channel for ReplayStage
        // We emit a switch bank event when we observe a ParentReady.
        // This event is filtered out if there are no duplicate blocks in this slot.
        // However this filtering can only happen after we receive the shreds for the block.
        // We overprovision at 100 leader windows - we would require almost 3 minutes of slow
        // repair / turbine to hit the limit
        let (switch_bank_sender, switch_bank_receiver) = bounded(100);

        let window_service = {
            let repair_service_channels = RepairServiceChannels::new(
                repair_request_quic_sender,
                verified_vote_receiver,
                dumped_slots_receiver,
                popular_pruned_forks_sender,
                ancestor_hashes_request_quic_sender,
                ancestor_hashes_response_quic_receiver,
                ancestor_hashes_replay_update_receiver,
            );
            let window_service_channels = WindowServiceChannels::new(
                verified_receiver,
                retransmit_sender,
                completed_data_sets_sender,
                duplicate_slots_sender.clone(),
                repair_service_channels,
                block_id_repair_channels,
            );
            WindowService::new(
                blockstore.clone(),
                repair_socket,
                ancestor_hashes_socket,
                block_id_repair_socket,
                exit.clone(),
                repair_info,
                window_service_channels,
                leader_schedule_cache.clone(),
                outstanding_repair_requests,
            )
        };

        let (cluster_slots_update_sender, cluster_slots_update_receiver) = unbounded();
        let cluster_slots_service = ClusterSlotsService::new(
            blockstore.clone(),
            cluster_slots.clone(),
            bank_forks.clone(),
            cluster_info.clone(),
            cluster_slots_update_receiver,
            exit.clone(),
            migration_status.clone(),
        );

        let (cost_update_sender, cost_update_receiver) = unbounded();
        let (drop_bank_sender, drop_bank_receiver) = unbounded();
        let (voting_sender, voting_receiver) = unbounded();
        // The BLS sender channel should be mostly used during standstill handling,
        // there could be 10s/400ms = 25 slots, <=5 votes and <=5 certificates per slot,
        // we cap the channel at 512 to give some headroom.
        let (bls_sender, bls_receiver) = bounded(512);

        let (lockouts_sender, votor_commitment_sender, commitment_service) =
            AggregateCommitmentService::new(
                exit.clone(),
                block_commitment_cache.clone(),
                rpc_subscriptions.clone(),
            );

        let votor_config = VotorConfig {
            exit: exit.clone(),
            vote_account: *vote_account,
            wait_to_vote_slot,
            wait_for_vote_to_start_leader: tvu_config.wait_for_vote_to_start_leader,
            vote_history,
            vote_history_storage: vote_history_storage.clone(),
            authorized_voter_keypairs: authorized_voter_keypairs.clone(),
            blockstore: blockstore.clone(),
            bank_forks: bank_forks.clone(),
            cluster_info: cluster_info.clone(),
            leader_schedule_cache: leader_schedule_cache.clone(),
            rpc_subscriptions: rpc_subscriptions.clone(),
            consensus_metrics_sender,
            migration_status: migration_status.clone(),
            highest_finalized,
            snapshot_controller: snapshot_controller.clone(),
            bls_sender: bls_sender.clone(),
            commitment_sender: votor_commitment_sender,
            drop_bank_sender: drop_bank_sender.clone(),
            bank_notification_sender: bank_notification_sender.clone(),
            leader_window_info_sender,
            highest_parent_ready,
            event_sender: votor_event_sender.clone(),
            own_vote_sender: consensus_message_sender.clone(),
            reward_certs_sender,
            repair_event_sender,
            switch_bank_sender,
            event_receiver: votor_event_receiver,
            consensus_message_receiver,
            consensus_metrics_receiver,
            reward_votes_receiver,
            build_reward_certs_receiver,
        };
        let votor = Votor::new(votor_config);

        let replay_senders = ReplaySenders {
            rpc_subscriptions,
            slot_status_notifier,
            transaction_status_sender,
            entry_notification_sender,
            bank_notification_sender,
            ancestor_hashes_replay_update_sender,
            retransmit_slots_sender,
            replay_vote_sender,
            cluster_slots_update_sender,
            cost_update_sender,
            voting_sender,
            bls_sender,
            drop_bank_sender,
            block_metadata_notifier,
            dumped_slots_sender,
            votor_event_sender,
            own_vote_sender: consensus_message_sender,
            optimistic_parent_sender,
            lockouts_sender,
        };

        let replay_receivers = ReplayReceivers {
            ledger_signal_receiver,
            update_parent_receiver,
            duplicate_slots_receiver,
            ancestor_duplicate_slots_receiver,
            duplicate_confirmed_slots_receiver,
            gossip_verified_vote_hash_receiver,
            popular_pruned_forks_receiver,
            switch_bank_receiver,
        };

        let replay_stage_config = ReplayStageConfig {
            vote_account: *vote_account,
            authorized_voter_keypairs,
            exit: exit.clone(),
            leader_schedule_cache: leader_schedule_cache.clone(),
            block_commitment_cache,
            wait_for_vote_to_start_leader: tvu_config.wait_for_vote_to_start_leader,
            tower_storage: tower_storage.clone(),
            wait_to_vote_slot,
            replay_forks_threads: tvu_config.replay_forks_threads,
            replay_transactions_threads: tvu_config.replay_transactions_threads,
            blockstore: blockstore.clone(),
            bank_forks: bank_forks.clone(),
            cluster_info: cluster_info.clone(),
            poh_recorder: poh_recorder.clone(),
            poh_controller,
            tower,
            vote_tracker,
            cluster_slots,
            log_messages_bytes_limit,
            prioritization_fee_cache: prioritization_fee_cache.clone(),
            banking_tracer,
            snapshot_controller,
            replay_highest_frozen,
        };

        let voting_service = VotingService::new(
            voting_receiver,
            cluster_info.clone(),
            poh_recorder.clone(),
            tower_storage,
            vote_connection_cache.clone(),
        );

        let alpenglow_voting_service = AlpenglowVotingService::new(
            bls_receiver,
            cluster_info.clone(),
            vote_history_storage,
            bls_connection_cache,
            bank_forks.clone(),
            voting_service_test_override,
            alpenglow_last_voted,
            *vote_account,
        );

        let warm_quic_cache_service = create_cache_warmer_if_needed(
            connection_cache,
            vote_connection_cache,
            cluster_info,
            poh_recorder,
            &exit,
        );

        let cost_update_service = CostUpdateService::new(cost_update_receiver);

        let drop_bank_service = DropBankService::new(drop_bank_receiver);

        let replay_stage = if in_wen_restart {
            None
        } else {
            Some(ReplayStage::new(
                replay_stage_config,
                replay_senders,
                replay_receivers,
            )?)
        };

        let blockstore_cleanup_service = tvu_config.max_ledger_shreds.map(|max_ledger_shreds| {
            BlockstoreCleanupService::new(blockstore.clone(), max_ledger_shreds, exit.clone())
        });

        let duplicate_shred_listener = DuplicateShredListener::new(
            exit,
            cluster_info.clone(),
            DuplicateShredHandler::new(
                blockstore,
                leader_schedule_cache.clone(),
                bank_forks.clone(),
                duplicate_slots_sender,
                tvu_config.shred_version,
            ),
        );

        Ok(Tvu {
            fetch_stage,
            shred_sigverify,
            retransmit_stage,
            window_service,
            cluster_slots_service,
            replay_stage,
            blockstore_cleanup_service,
            cost_update_service,
            voting_service,
            alpenglow_voting_service,
            warm_quic_cache_service,
            drop_bank_service,
            duplicate_shred_listener,
            alpenglow_sigverify_service,
            alpenglow_quic_t,
            votor,
            commitment_service,
        })
    }

    pub fn join(self) -> thread::Result<()> {
        self.retransmit_stage.join()?;
        self.window_service.join()?;
        self.cluster_slots_service.join()?;
        self.fetch_stage.join()?;
        self.shred_sigverify.join()?;
        if self.blockstore_cleanup_service.is_some() {
            self.blockstore_cleanup_service.unwrap().join()?;
        }
        if self.replay_stage.is_some() {
            self.replay_stage.unwrap().join()?;
        }
        self.cost_update_service.join()?;
        self.voting_service.join()?;
        self.alpenglow_voting_service.join()?;
        if let Some(warmup_service) = self.warm_quic_cache_service {
            warmup_service.join()?;
        }
        self.drop_bank_service.join()?;
        self.duplicate_shred_listener.join()?;
        self.alpenglow_sigverify_service.join()?;
        self.alpenglow_quic_t.join()?;
        self.votor.join()?;
        self.commitment_service.join()?;
        Ok(())
    }
}

fn create_cache_warmer_if_needed(
    connection_cache: Option<&Arc<ConnectionCache>>,
    vote_connection_cache: Arc<ConnectionCache>,
    cluster_info: &Arc<ClusterInfo>,
    poh_recorder: &Arc<RwLock<PohRecorder>>,
    exit: &Arc<AtomicBool>,
) -> Option<WarmQuicCacheService> {
    let tpu_connection_cache = connection_cache.filter(|cache| cache.use_quic()).cloned();
    let vote_connection_cache = Some(vote_connection_cache).filter(|cache| cache.use_quic());

    (tpu_connection_cache.is_some() || vote_connection_cache.is_some()).then(|| {
        WarmQuicCacheService::new(
            tpu_connection_cache,
            vote_connection_cache,
            cluster_info.clone(),
            poh_recorder.clone(),
            exit.clone(),
        )
    })
}

#[cfg(test)]
pub mod tests {
    use {
        super::*,
        crate::{
            consensus::tower_storage::FileTowerStorage,
            repair::quic_endpoint::RepairQuicAsyncSenders,
        },
        serial_test::serial,
        solana_gossip::{cluster_info::ClusterInfo, node::Node},
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::BlockstoreSignals,
            blockstore_options::BlockstoreOptions,
            create_new_tmp_ledger,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
        },
        solana_poh::poh_recorder::create_test_recorder,
        solana_rpc::optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
        solana_runtime::bank::Bank,
        solana_signer::Signer,
        solana_streamer::socket::SocketAddrSpace,
        solana_tpu_client::tpu_client::{DEFAULT_TPU_CONNECTION_POOL_SIZE, DEFAULT_VOTE_USE_QUIC},
        solana_votor::vote_history_storage::FileVoteHistoryStorage,
        std::sync::atomic::{AtomicU64, Ordering},
    };

    fn test_tvu_exit(enable_wen_restart: bool) {
        agave_logger::setup();
        let leader = Node::new_localhost();
        let target1_keypair = Keypair::new();
        let target1 = Node::new_localhost_with_pubkey(&target1_keypair.pubkey());

        let starting_balance = 10_000;
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(starting_balance);

        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));

        let (turbine_quic_endpoint_sender, _turbine_quic_endpoint_receiver) =
            tokio::sync::mpsc::channel(/*capacity:*/ 128);
        let (_turbine_quic_endpoint_sender, turbine_quic_endpoint_receiver) = unbounded();
        let (_, repair_response_quic_receiver) = unbounded();
        let repair_quic_async_senders = RepairQuicAsyncSenders::new_dummy();
        let (_, ancestor_hashes_response_quic_receiver) = unbounded();
        //start cluster_info1
        let cluster_info1 = ClusterInfo::new(
            target1.info.clone(),
            target1_keypair.into(),
            SocketAddrSpace::Unspecified,
        );
        cluster_info1.insert_info(leader.info);
        let cref1 = Arc::new(cluster_info1);

        let (blockstore_path, _) = create_new_tmp_ledger!(&genesis_config);
        let BlockstoreSignals {
            blockstore,
            ledger_signal_receiver,
            update_parent_receiver,
            ..
        } = Blockstore::open_with_signal(&blockstore_path, BlockstoreOptions::default())
            .expect("Expected to successfully open ledger");
        let blockstore = Arc::new(blockstore);
        let bank = bank_forks.read().unwrap().working_bank();
        let (
            exit,
            poh_recorder,
            poh_controller,
            _transaction_recorder,
            poh_service,
            _entry_receiver,
        ) = create_test_recorder(bank.clone(), blockstore.clone(), None, None);
        let vote_keypair = Keypair::new();
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let (retransmit_slots_sender, _retransmit_slots_receiver) = unbounded();
        let (_gossip_verified_vote_hash_sender, gossip_verified_vote_hash_receiver) = unbounded();
        let (verified_vote_sender, verified_vote_receiver) = unbounded();
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let (_, gossip_confirmed_slots_receiver) = unbounded();
        let (leader_window_info_sender, _) = bounded(1);
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let ignored_prioritization_fee_cache = Arc::new(PrioritizationFeeCache::new(0u64));
        let outstanding_repair_requests = Arc::<RwLock<OutstandingShredRepairs>>::default();
        let cluster_slots = Arc::new(ClusterSlots::default_for_tests());
        let wen_restart_repair_slots = if enable_wen_restart {
            Some(Arc::new(RwLock::new(vec![])))
        } else {
            None
        };
        let connection_cache = if DEFAULT_VOTE_USE_QUIC {
            ConnectionCache::new_quic_for_tests(
                "connection_cache_vote_quic",
                DEFAULT_TPU_CONNECTION_POOL_SIZE,
            )
        } else {
            ConnectionCache::with_udp(
                "connection_cache_vote_udp",
                DEFAULT_TPU_CONNECTION_POOL_SIZE,
            )
        };
        let alpenglow_connection_cache = ConnectionCache::new_quic(
            "connection_cache_alpenglow",
            DEFAULT_TPU_CONNECTION_POOL_SIZE,
        );
        let (votor_event_sender, votor_event_receiver) = unbounded();
        let highest_parent_ready = Arc::new(RwLock::default());
        let (optimistic_parent_sender, _optimistic_parent_receiver) = unbounded();
        let (reward_certs_sender, _reward_certs_receiver) = bounded(1);
        let (_build_reward_certs_sender, build_reward_certs_receiver) = bounded(1);

        let tvu = Tvu::new(
            &vote_keypair.pubkey(),
            Arc::new(RwLock::new(vec![Arc::new(vote_keypair)])),
            Arc::new(Keypair::new()),
            &bank_forks,
            &cref1,
            {
                TvuSockets {
                    repair: target1.sockets.repair,
                    retransmit: target1.sockets.retransmit_sockets,
                    fetch: target1.sockets.tvu,
                    ancestor_hashes_requests: target1.sockets.ancestor_hashes_requests,
                    alpenglow_quic: target1.sockets.alpenglow,
                    block_id_repair: target1.sockets.block_id_repair,
                }
            },
            blockstore,
            ledger_signal_receiver,
            update_parent_receiver,
            Some(Arc::new(RpcSubscriptions::new_for_tests(
                exit.clone(),
                max_complete_transaction_status_slot,
                bank_forks.clone(),
                block_commitment_cache.clone(),
                OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
            ))),
            &poh_recorder,
            poh_controller,
            Tower::default(),
            Arc::new(FileTowerStorage::default()),
            VoteHistory::default(),
            Arc::new(FileVoteHistoryStorage::default()),
            &leader_schedule_cache,
            exit.clone(),
            block_commitment_cache,
            TurbineMode::default(),
            None,
            None,
            Arc::<VoteTracker>::default(),
            retransmit_slots_sender,
            gossip_verified_vote_hash_receiver,
            verified_vote_sender,
            verified_vote_receiver,
            replay_vote_sender,
            /*completed_data_sets_sender:*/ None,
            None,
            gossip_confirmed_slots_receiver,
            TvuConfig::default(),
            &Arc::new(MaxSlots::default()),
            None,
            None,
            None, // snapshot_controller
            None,
            Some(&Arc::new(ConnectionCache::new("connection_cache_test"))),
            &ignored_prioritization_fee_cache,
            BankingTracer::new_disabled(),
            turbine_quic_endpoint_sender,
            turbine_quic_endpoint_receiver,
            repair_response_quic_receiver,
            repair_quic_async_senders.repair_request_quic_sender,
            repair_quic_async_senders.ancestor_hashes_request_quic_sender,
            ancestor_hashes_response_quic_receiver,
            outstanding_repair_requests,
            cluster_slots,
            wen_restart_repair_slots,
            None,
            Arc::new(connection_cache),
            AlpenglowInitializationState {
                leader_window_info_sender,
                replay_highest_frozen: Arc::new(ReplayHighestFrozen::default()),
                highest_parent_ready,
                votor_event_sender,
                votor_event_receiver,
                staked_nodes: Arc::new(RwLock::new(StakedNodes::default())),
                key_notifiers: Arc::new(RwLock::new(KeyUpdaters::default())),
                alpenglow_quic_server_config: QuicServerParams::default_for_tests(),
                bls_connection_cache: Arc::new(alpenglow_connection_cache),
                voting_service_test_override: None,
                alpenglow_last_voted: Arc::new(AlpenglowLastVoted::default()),
                reward_certs_sender,
                build_reward_certs_receiver,
                highest_finalized: Arc::new(RwLock::new(None)),
                optimistic_parent_sender,
            },
        )
        .expect("assume success");
        if enable_wen_restart {
            assert!(tvu.replay_stage.is_none())
        } else {
            assert!(tvu.replay_stage.is_some())
        }
        exit.store(true, Ordering::Relaxed);
        tvu.join().unwrap();
        poh_service.join().unwrap();
    }

    #[test]
    #[serial]
    fn test_tvu_exit_no_wen_restart() {
        test_tvu_exit(false);
    }

    #[test]
    #[serial]
    fn test_tvu_exit_with_wen_restart() {
        test_tvu_exit(true);
    }
}
