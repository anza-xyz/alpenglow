//! The `tvu` module implements the Transaction Validation Unit, a multi-stage transaction
//! validation pipeline in software.

use {
    crate::{
        banking_trace::BankingTracer,
        block_creation_loop::ReplayHighestFrozen,
        cluster_info_vote_listener::{
            DuplicateConfirmedSlotsReceiver, GossipVerifiedVoteHashReceiver, VerifiedVoteReceiver,
            VoteTracker,
        },
        cluster_slots_service::{cluster_slots::ClusterSlots, ClusterSlotsService},
        completed_data_sets_service::CompletedDataSetsSender,
        consensus::{tower_storage::TowerStorage, Tower},
        cost_update_service::CostUpdateService,
        drop_bank_service::DropBankService,
        repair::repair_service::{OutstandingShredRepairs, RepairInfo, RepairServiceChannels},
        replay_stage::{ReplayReceivers, ReplaySenders, ReplayStage, ReplayStageConfig},
        shred_fetch_stage::ShredFetchStage,
        voting_service::{VotingService, VotingServiceOverride},
        warm_quic_cache_service::WarmQuicCacheService,
        window_service::{WindowService, WindowServiceChannels},
    },
    bytes::Bytes,
    crossbeam_channel::{bounded, unbounded, Receiver, Sender},
    solana_client::connection_cache::ConnectionCache,
    solana_geyser_plugin_manager::block_metadata_notifier_interface::BlockMetadataNotifierArc,
    solana_gossip::{
        cluster_info::ClusterInfo, duplicate_shred_handler::DuplicateShredHandler,
        duplicate_shred_listener::DuplicateShredListener,
    },
    solana_ledger::{
        blockstore::Blockstore, blockstore_cleanup_service::BlockstoreCleanupService,
        blockstore_processor::TransactionStatusSender, entry_notifier_service::EntryNotifierSender,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_poh::poh_recorder::PohRecorder,
    solana_rpc::{
        block_meta_service::BlockMetaSender, max_slots::MaxSlots,
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions, slot_status_notifier::SlotStatusNotifier,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender,
        bank_forks::BankForks,
        commitment::BlockCommitmentCache,
        prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::{
            BLSVerifiedMessageReceiver, BLSVerifiedMessageSender, ReplayVoteSender,
        },
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Keypair},
    solana_turbine::retransmit_stage::RetransmitStage,
    solana_votor::{
        event::{VotorEventReceiver, VotorEventSender},
        vote_history::VoteHistory,
        vote_history_storage::VoteHistoryStorage,
        votor::LeaderWindowNotifier,
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
    warm_quic_cache_service: Option<WarmQuicCacheService>,
    drop_bank_service: DropBankService,
    duplicate_shred_listener: DuplicateShredListener,
}

pub struct TvuSockets {
    pub fetch: Vec<UdpSocket>,
    pub repair: UdpSocket,
    pub retransmit: Vec<UdpSocket>,
    pub ancestor_hashes_requests: UdpSocket,
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
        }
    }
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
        bank_forks: &Arc<RwLock<BankForks>>,
        cluster_info: &Arc<ClusterInfo>,
        sockets: TvuSockets,
        blockstore: Arc<Blockstore>,
        ledger_signal_receiver: Receiver<bool>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        tower: Tower,
        tower_storage: Arc<dyn TowerStorage>,
        vote_history: VoteHistory,
        vote_history_storage: Arc<dyn VoteHistoryStorage>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        exit: Arc<AtomicBool>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        turbine_disabled: Arc<AtomicBool>,
        transaction_status_sender: Option<TransactionStatusSender>,
        block_meta_sender: Option<BlockMetaSender>,
        entry_notification_sender: Option<EntryNotifierSender>,
        vote_tracker: Arc<VoteTracker>,
        retransmit_slots_sender: Sender<Slot>,
        gossip_verified_vote_hash_receiver: GossipVerifiedVoteHashReceiver,
        verified_vote_receiver: VerifiedVoteReceiver,
        replay_vote_sender: ReplayVoteSender,
        completed_data_sets_sender: Option<CompletedDataSetsSender>,
        bank_notification_sender: Option<BankNotificationSenderConfig>,
        duplicate_confirmed_slots_receiver: DuplicateConfirmedSlotsReceiver,
        own_vote_sender: BLSVerifiedMessageSender,
        bls_verified_message_receiver: BLSVerifiedMessageReceiver,
        tvu_config: TvuConfig,
        max_slots: &Arc<MaxSlots>,
        block_metadata_notifier: Option<BlockMetadataNotifierArc>,
        wait_to_vote_slot: Option<Slot>,
        accounts_background_request_sender: AbsRequestSender,
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
        replay_highest_frozen: Arc<ReplayHighestFrozen>,
        leader_window_notifier: Arc<LeaderWindowNotifier>,
        voting_service_test_override: Option<VotingServiceOverride>,
        votor_event_sender: VotorEventSender,
        votor_event_receiver: VotorEventReceiver,
    ) -> Result<Self, String> {
        let in_wen_restart = wen_restart_repair_slots.is_some();

        let TvuSockets {
            repair: repair_socket,
            fetch: fetch_sockets,
            retransmit: retransmit_sockets,
            ancestor_hashes_requests: ancestor_hashes_socket,
        } = sockets;

        let (fetch_sender, fetch_receiver) = unbounded();

        let repair_socket = Arc::new(repair_socket);
        let ancestor_hashes_socket = Arc::new(ancestor_hashes_socket);
        let fetch_sockets: Vec<Arc<UdpSocket>> = fetch_sockets.into_iter().map(Arc::new).collect();
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
            turbine_disabled,
            exit.clone(),
        );

        let (verified_sender, verified_receiver) = unbounded();
        let (retransmit_sender, retransmit_receiver) = unbounded();
        let shred_sigverify = solana_turbine::sigverify_shreds::spawn_shred_sigverify(
            cluster_info.clone(),
            bank_forks.clone(),
            leader_schedule_cache.clone(),
            fetch_receiver,
            retransmit_sender.clone(),
            verified_sender,
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
            Some(rpc_subscriptions.clone()),
            slot_status_notifier.clone(),
        );

        let (ancestor_duplicate_slots_sender, ancestor_duplicate_slots_receiver) = unbounded();
        let (duplicate_slots_sender, duplicate_slots_receiver) = unbounded();
        let (ancestor_hashes_replay_update_sender, ancestor_hashes_replay_update_receiver) =
            unbounded();
        let (dumped_slots_sender, dumped_slots_receiver) = unbounded();
        let (popular_pruned_forks_sender, popular_pruned_forks_receiver) = unbounded();
        let (certificate_sender, certificate_receiver) = unbounded();
        let window_service = {
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
            };
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
            );
            WindowService::new(
                blockstore.clone(),
                repair_socket,
                ancestor_hashes_socket,
                exit.clone(),
                repair_info,
                window_service_channels,
                leader_schedule_cache.clone(),
                outstanding_repair_requests,
                certificate_receiver,
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
        );

        let (cost_update_sender, cost_update_receiver) = unbounded();
        let (drop_bank_sender, drop_bank_receiver) = unbounded();
        let (voting_sender, voting_receiver) = unbounded();
        // The BLS sender channel should be mostly used during standstill handling,
        // there could be 10s/400ms = 25 slots, <=5 votes and <=5 certificates per slot,
        // we cap the channel at 512 to give some headroom.
        let (bls_sender, bls_receiver) = bounded(512);

        let replay_senders = ReplaySenders {
            rpc_subscriptions: rpc_subscriptions.clone(),
            slot_status_notifier,
            accounts_background_request_sender,
            transaction_status_sender,
            block_meta_sender,
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
            certificate_sender,
            votor_event_sender,
            own_vote_sender,
        };

        let replay_receivers = ReplayReceivers {
            ledger_signal_receiver,
            duplicate_slots_receiver,
            ancestor_duplicate_slots_receiver,
            duplicate_confirmed_slots_receiver,
            gossip_verified_vote_hash_receiver,
            popular_pruned_forks_receiver,
            bls_verified_message_receiver,
            votor_event_receiver,
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
            tower,
            vote_history,
            vote_history_storage: vote_history_storage.clone(),
            vote_tracker,
            cluster_slots,
            log_messages_bytes_limit,
            prioritization_fee_cache: prioritization_fee_cache.clone(),
            banking_tracer,
            replay_highest_frozen,
            leader_window_notifier,
        };

        let voting_service = VotingService::new(
            voting_receiver,
            bls_receiver,
            cluster_info.clone(),
            poh_recorder.clone(),
            tower_storage,
            vote_history_storage.clone(),
            vote_connection_cache.clone(),
            bank_forks.clone(),
            voting_service_test_override,
        );

        let warm_quic_cache_service = create_cache_warmer_if_needed(
            connection_cache,
            vote_connection_cache,
            cluster_info,
            poh_recorder,
            &exit,
        );

        let cost_update_service = CostUpdateService::new(blockstore.clone(), cost_update_receiver);

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
            warm_quic_cache_service,
            drop_bank_service,
            duplicate_shred_listener,
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
        if let Some(warmup_service) = self.warm_quic_cache_service {
            warmup_service.join()?;
        }
        self.drop_bank_service.join()?;
        self.duplicate_shred_listener.join()?;
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
        solana_gossip::cluster_info::{ClusterInfo, Node},
        solana_ledger::{
            blockstore::BlockstoreSignals,
            blockstore_options::BlockstoreOptions,
            create_new_tmp_ledger,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
        },
        solana_poh::poh_recorder::create_test_recorder,
        solana_rpc::optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
        solana_runtime::bank::Bank,
        solana_sdk::signature::{Keypair, Signer},
        solana_streamer::socket::SocketAddrSpace,
        solana_tpu_client::tpu_client::{DEFAULT_TPU_CONNECTION_POOL_SIZE, DEFAULT_VOTE_USE_QUIC},
        solana_votor::vote_history_storage::FileVoteHistoryStorage,
        std::sync::atomic::{AtomicU64, Ordering},
    };

    fn test_tvu_exit(enable_wen_restart: bool) {
        solana_logger::setup();
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
            ..
        } = Blockstore::open_with_signal(&blockstore_path, BlockstoreOptions::default())
            .expect("Expected to successfully open ledger");
        let blockstore = Arc::new(blockstore);
        let bank = bank_forks.read().unwrap().working_bank();
        let (exit, poh_recorder, poh_service, _entry_receiver) =
            create_test_recorder(bank.clone(), blockstore.clone(), None, None);
        let vote_keypair = Keypair::new();
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let (retransmit_slots_sender, _retransmit_slots_receiver) = unbounded();
        let (_gossip_verified_vote_hash_sender, gossip_verified_vote_hash_receiver) = unbounded();
        let (_verified_vote_sender, verified_vote_receiver) = unbounded();
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let (_, gossip_confirmed_slots_receiver) = unbounded();
        let (bls_verified_message_sender, bls_verified_message_receiver) = unbounded();
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let ignored_prioritization_fee_cache = Arc::new(PrioritizationFeeCache::new(0u64));
        let outstanding_repair_requests = Arc::<RwLock<OutstandingShredRepairs>>::default();
        let cluster_slots = Arc::new(ClusterSlots::default());
        let wen_restart_repair_slots = if enable_wen_restart {
            Some(Arc::new(RwLock::new(vec![])))
        } else {
            None
        };
        let connection_cache = if DEFAULT_VOTE_USE_QUIC {
            ConnectionCache::new_quic(
                "connection_cache_vote_quic",
                DEFAULT_TPU_CONNECTION_POOL_SIZE,
            )
        } else {
            ConnectionCache::with_udp(
                "connection_cache_vote_udp",
                DEFAULT_TPU_CONNECTION_POOL_SIZE,
            )
        };
        let (votor_event_sender, votor_event_receiver) = unbounded();

        let tvu = Tvu::new(
            &vote_keypair.pubkey(),
            Arc::new(RwLock::new(vec![Arc::new(vote_keypair)])),
            &bank_forks,
            &cref1,
            {
                TvuSockets {
                    repair: target1.sockets.repair,
                    retransmit: target1.sockets.retransmit_sockets,
                    fetch: target1.sockets.tvu,
                    ancestor_hashes_requests: target1.sockets.ancestor_hashes_requests,
                }
            },
            blockstore,
            ledger_signal_receiver,
            &Arc::new(RpcSubscriptions::new_for_tests(
                exit.clone(),
                max_complete_transaction_status_slot,
                max_complete_rewards_slot,
                bank_forks.clone(),
                block_commitment_cache.clone(),
                OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
            )),
            &poh_recorder,
            Tower::default(),
            Arc::new(FileTowerStorage::default()),
            VoteHistory::default(),
            Arc::new(FileVoteHistoryStorage::default()),
            &leader_schedule_cache,
            exit.clone(),
            block_commitment_cache,
            Arc::<AtomicBool>::default(),
            None,
            None,
            None,
            Arc::<VoteTracker>::default(),
            retransmit_slots_sender,
            gossip_verified_vote_hash_receiver,
            verified_vote_receiver,
            replay_vote_sender,
            /*completed_data_sets_sender:*/ None,
            None,
            gossip_confirmed_slots_receiver,
            bls_verified_message_sender,
            bls_verified_message_receiver,
            TvuConfig::default(),
            &Arc::new(MaxSlots::default()),
            None,
            None,
            AbsRequestSender::default(),
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
            Arc::new(ReplayHighestFrozen::default()),
            Arc::new(LeaderWindowNotifier::default()),
            None,
            votor_event_sender,
            votor_event_receiver,
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
