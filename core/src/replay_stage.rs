//! The `replay_stage` replays transactions broadcast by the leader.
use {
    crate::{
        banking_stage::update_bank_forks_and_poh_recorder_for_new_tpu_bank,
        banking_trace::BankingTracer,
        block_creation_loop::ReplayHighestFrozen,
        cluster_info_vote_listener::{
            DuplicateConfirmedSlotsReceiver, GossipVerifiedVoteHashReceiver, VoteTracker,
        },
        cluster_slots_service::{cluster_slots::ClusterSlots, ClusterSlotsUpdateSender},
        commitment_service::{AggregateCommitmentService, TowerCommitmentAggregationData},
        consensus::{
            fork_choice::{select_vote_and_reset_forks, ForkChoice, SelectVoteAndResetForkResult},
            heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
            latest_validator_votes_for_frozen_banks::LatestValidatorVotesForFrozenBanks,
            progress_map::{ForkProgress, ProgressMap, PropagatedStats},
            tower_storage::{SavedTower, SavedTowerVersions, TowerStorage},
            tower_vote_state::TowerVoteState,
            BlockhashStatus, ComputedBankState, Stake, SwitchForkDecision, Tower, TowerError,
            VotedStakes, SWITCH_FORK_THRESHOLD,
        },
        cost_update_service::CostUpdate,
        repair::{
            ancestor_hashes_service::AncestorHashesReplayUpdateSender,
            cluster_slot_state_verifier::*,
            duplicate_repair_status::AncestorDuplicateSlotToRepair,
            repair_service::{
                AncestorDuplicateSlotsReceiver, DumpedSlotsSender, PopularPrunedForksReceiver,
            },
        },
        unfrozen_gossip_verified_vote_hashes::UnfrozenGossipVerifiedVoteHashes,
        voting_service::VoteOp,
        window_service::DuplicateSlotReceiver,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    rayon::{
        iter::{IntoParallelIterator, ParallelIterator},
        ThreadPool,
    },
    solana_accounts_db::contains::Contains,
    solana_entry::entry::VerifyRecyclers,
    solana_geyser_plugin_manager::block_metadata_notifier_interface::BlockMetadataNotifierArc,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        block_error::BlockError,
        blockstore::Blockstore,
        blockstore_processor::{
            self, BlockstoreProcessorError, ConfirmationProgress, ExecuteBatchesInternalMetrics,
            ReplaySlotStats, TransactionStatusSender,
        },
        entry_notifier_service::EntryNotifierSender,
        leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::first_of_consecutive_leader_slots,
    },
    solana_measure::measure::Measure,
    solana_poh::poh_recorder::{PohLeaderStatus, PohRecorder, GRACE_TICKS_FACTOR, MAX_GRACE_SLOTS},
    solana_rpc::{
        block_meta_service::BlockMetaSender,
        optimistically_confirmed_bank_tracker::{BankNotification, BankNotificationSenderConfig},
        rpc_subscriptions::RpcSubscriptions,
        slot_status_notifier::SlotStatusNotifier,
    },
    solana_rpc_client_api::response::SlotUpdate,
    solana_runtime::{
        accounts_background_service::AbsRequestSender,
        bank::{bank_hash_details, Bank, NewBankOptions},
        bank_forks::{BankForks, SetRootError, MAX_ROOT_DISTANCE_FOR_VOTE_ONLY},
        commitment::BlockCommitmentCache,
        installed_scheduler_pool::BankWithScheduler,
        prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::{
            BLSVerifiedMessageReceiver, BLSVerifiedMessageSender, ReplayVoteSender,
        },
    },
    solana_sdk::{
        clock::{BankId, Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        hash::Hash,
        pubkey::Pubkey,
        saturating_add_assign,
        signature::{Keypair, Signature, Signer},
        timing::timestamp,
        transaction::Transaction,
    },
    solana_timings::ExecuteTimings,
    solana_vote::vote_transaction::VoteTransaction,
    solana_votor::{
        event::{CompletedBlock, VotorEvent, VotorEventReceiver, VotorEventSender},
        root_utils,
        vote_history::VoteHistory,
        vote_history_storage::VoteHistoryStorage,
        voting_utils::{BLSOp, GenerateVoteTxResult},
        votor::{LeaderWindowNotifier, Votor, VotorConfig},
    },
    solana_votor_messages::bls_message::{Certificate, CertificateMessage},
    std::{
        collections::{HashMap, HashSet},
        num::NonZeroUsize,
        result,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub const MAX_ENTRY_RECV_PER_ITER: usize = 512;
pub const SUPERMINORITY_THRESHOLD: f64 = 1f64 / 3f64;
pub const MAX_UNCONFIRMED_SLOTS: usize = 5;
pub const DUPLICATE_LIVENESS_THRESHOLD: f64 = 0.1;
pub const DUPLICATE_THRESHOLD: f64 = 1.0 - SWITCH_FORK_THRESHOLD - DUPLICATE_LIVENESS_THRESHOLD;

pub(crate) const MAX_VOTE_SIGNATURES: usize = 200;
const MAX_VOTE_REFRESH_INTERVAL_MILLIS: usize = 5000;
const MAX_REPAIR_RETRY_LOOP_ATTEMPTS: usize = 10;

#[cfg(test)]
static_assertions::const_assert!(REFRESH_VOTE_BLOCKHEIGHT < solana_sdk::clock::MAX_PROCESSING_AGE);
// Give at least 4 leaders the chance to pack our vote
const REFRESH_VOTE_BLOCKHEIGHT: usize = 16;

#[derive(PartialEq, Eq, Debug)]
pub enum HeaviestForkFailures {
    LockedOut(u64),
    FailedThreshold(
        Slot,
        /* vote depth */ u64,
        /* Observed stake */ u64,
        /* Total stake */ u64,
    ),
    FailedSwitchThreshold(
        Slot,
        /* Observed stake */ u64,
        /* Total stake */ u64,
    ),
    NoPropagatedConfirmation(
        Slot,
        /* Observed stake */ u64,
        /* Total stake */ u64,
    ),
}

enum ForkReplayMode {
    Serial,
    Parallel(ThreadPool),
}

// Implement a destructor for the ReplayStage thread to signal it exited
// even on panics
pub(crate) struct Finalizer {
    exit_sender: Arc<AtomicBool>,
}

impl Finalizer {
    pub(crate) fn new(exit_sender: Arc<AtomicBool>) -> Self {
        Finalizer { exit_sender }
    }
}

// Implement a destructor for Finalizer.
impl Drop for Finalizer {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

struct ReplaySlotFromBlockstore {
    is_slot_dead: bool,
    bank_slot: Slot,
    replay_result: Option<Result<usize /* tx count */, BlockstoreProcessorError>>,
}

struct LastVoteRefreshTime {
    last_refresh_time: Instant,
    last_print_time: Instant,
}

#[derive(Default)]
struct SkippedSlotsInfo {
    last_retransmit_slot: u64,
    last_skipped_slot: u64,
}

pub struct TowerBFTStructures {
    pub heaviest_subtree_fork_choice: HeaviestSubtreeForkChoice,
    pub duplicate_slots_tracker: DuplicateSlotsTracker,
    pub duplicate_confirmed_slots: DuplicateConfirmedSlots,
    pub unfrozen_gossip_verified_vote_hashes: UnfrozenGossipVerifiedVoteHashes,
    pub epoch_slots_frozen_slots: EpochSlotsFrozenSlots,
}

struct PartitionInfo {
    partition_start_time: Option<Instant>,
}

impl PartitionInfo {
    fn new() -> Self {
        Self {
            partition_start_time: None,
        }
    }

    fn update(
        &mut self,
        partition_detected: bool,
        heaviest_slot: Slot,
        last_voted_slot: Slot,
        reset_bank_slot: Slot,
        heaviest_fork_failures: Vec<HeaviestForkFailures>,
    ) {
        if self.partition_start_time.is_none() && partition_detected {
            warn!(
                "PARTITION DETECTED waiting to join heaviest fork: {} last vote: {:?}, reset \
                 slot: {}",
                heaviest_slot, last_voted_slot, reset_bank_slot,
            );
            datapoint_info!(
                "replay_stage-partition-start",
                ("heaviest_slot", heaviest_slot as i64, i64),
                ("last_vote_slot", last_voted_slot as i64, i64),
                ("reset_slot", reset_bank_slot as i64, i64),
                (
                    "heaviest_fork_failure_first",
                    format!("{:?}", heaviest_fork_failures.first()),
                    String
                ),
                (
                    "heaviest_fork_failure_second",
                    format!("{:?}", heaviest_fork_failures.get(1)),
                    String
                ),
            );
            self.partition_start_time = Some(Instant::now());
        } else if self.partition_start_time.is_some() && !partition_detected {
            warn!(
                "PARTITION resolved heaviest fork: {} last vote: {:?}, reset slot: {}",
                heaviest_slot, last_voted_slot, reset_bank_slot
            );
            datapoint_info!(
                "replay_stage-partition-resolved",
                ("heaviest_slot", heaviest_slot as i64, i64),
                ("last_vote_slot", last_voted_slot as i64, i64),
                ("reset_slot", reset_bank_slot as i64, i64),
                (
                    "partition_duration_ms",
                    self.partition_start_time.unwrap().elapsed().as_millis() as i64,
                    i64
                ),
            );
            self.partition_start_time = None;
        }
    }
}

pub struct ReplayStageConfig {
    pub vote_account: Pubkey,
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub exit: Arc<AtomicBool>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    pub wait_for_vote_to_start_leader: bool,
    pub tower_storage: Arc<dyn TowerStorage>,
    // Stops voting until this slot has been reached. Should be used to avoid
    // duplicate voting which can lead to slashing.
    pub wait_to_vote_slot: Option<Slot>,
    pub replay_forks_threads: NonZeroUsize,
    pub replay_transactions_threads: NonZeroUsize,
    pub blockstore: Arc<Blockstore>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub cluster_info: Arc<ClusterInfo>,
    pub poh_recorder: Arc<RwLock<PohRecorder>>,
    pub tower: Tower,
    pub vote_history: VoteHistory,
    pub vote_history_storage: Arc<dyn VoteHistoryStorage>,
    pub vote_tracker: Arc<VoteTracker>,
    pub cluster_slots: Arc<ClusterSlots>,
    pub log_messages_bytes_limit: Option<usize>,
    pub prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    pub banking_tracer: Arc<BankingTracer>,
    pub replay_highest_frozen: Arc<ReplayHighestFrozen>,
    pub leader_window_notifier: Arc<LeaderWindowNotifier>,
}

pub struct ReplaySenders {
    pub rpc_subscriptions: Arc<RpcSubscriptions>,
    pub slot_status_notifier: Option<SlotStatusNotifier>,
    pub accounts_background_request_sender: AbsRequestSender,
    pub transaction_status_sender: Option<TransactionStatusSender>,
    pub block_meta_sender: Option<BlockMetaSender>,
    pub entry_notification_sender: Option<EntryNotifierSender>,
    pub bank_notification_sender: Option<BankNotificationSenderConfig>,
    pub ancestor_hashes_replay_update_sender: AncestorHashesReplayUpdateSender,
    pub retransmit_slots_sender: Sender<u64>,
    pub replay_vote_sender: ReplayVoteSender,
    pub cluster_slots_update_sender: Sender<Vec<u64>>,
    pub cost_update_sender: Sender<CostUpdate>,
    pub voting_sender: Sender<VoteOp>,
    pub bls_sender: Sender<BLSOp>,
    pub drop_bank_sender: Sender<Vec<BankWithScheduler>>,
    pub block_metadata_notifier: Option<BlockMetadataNotifierArc>,
    pub dumped_slots_sender: Sender<Vec<(u64, Hash)>>,
    pub certificate_sender: Sender<(Certificate, CertificateMessage)>,
    pub votor_event_sender: VotorEventSender,
    pub own_vote_sender: BLSVerifiedMessageSender,
}

pub struct ReplayReceivers {
    pub ledger_signal_receiver: Receiver<bool>,
    pub duplicate_slots_receiver: Receiver<u64>,
    pub ancestor_duplicate_slots_receiver: Receiver<AncestorDuplicateSlotToRepair>,
    pub duplicate_confirmed_slots_receiver: Receiver<Vec<(u64, Hash)>>,
    pub gossip_verified_vote_hash_receiver: Receiver<(Pubkey, u64, Hash)>,
    pub popular_pruned_forks_receiver: Receiver<Vec<u64>>,
    pub bls_verified_message_receiver: BLSVerifiedMessageReceiver,
    pub votor_event_receiver: VotorEventReceiver,
}

/// Timing information for the ReplayStage main processing loop
#[derive(Default)]
struct ReplayLoopTiming {
    last_submit: u64,
    loop_count: u64,
    collect_frozen_banks_elapsed_us: u64,
    compute_bank_stats_elapsed_us: u64,
    select_vote_and_reset_forks_elapsed_us: u64,
    start_leader_elapsed_us: u64,
    reset_bank_elapsed_us: u64,
    voting_elapsed_us: u64,
    generate_vote_us: u64,
    update_commitment_cache_us: u64,
    select_forks_elapsed_us: u64,
    compute_slot_stats_elapsed_us: u64,
    generate_new_bank_forks_elapsed_us: u64,
    replay_active_banks_elapsed_us: u64,
    wait_receive_elapsed_us: u64,
    heaviest_fork_failures_elapsed_us: u64,
    bank_count: u64,
    process_ancestor_hashes_duplicate_slots_elapsed_us: u64,
    process_duplicate_confirmed_slots_elapsed_us: u64,
    process_duplicate_slots_elapsed_us: u64,
    process_unfrozen_gossip_verified_vote_hashes_elapsed_us: u64,
    process_popular_pruned_forks_elapsed_us: u64,
    repair_correct_slots_elapsed_us: u64,
    retransmit_not_propagated_elapsed_us: u64,
    generate_new_bank_forks_read_lock_us: u64,
    generate_new_bank_forks_get_slots_since_us: u64,
    generate_new_bank_forks_loop_us: u64,
    generate_new_bank_forks_write_lock_us: u64,
    // When processing multiple forks concurrently, only captures the longest fork
    replay_blockstore_us: u64,
}
impl ReplayLoopTiming {
    #[allow(clippy::too_many_arguments)]
    fn update_non_alpenglow(
        &mut self,
        collect_frozen_banks_elapsed_us: u64,
        compute_bank_stats_elapsed_us: u64,
        select_vote_and_reset_forks_elapsed_us: u64,
        reset_bank_elapsed_us: u64,
        voting_elapsed_us: u64,
        select_forks_elapsed_us: u64,
        compute_slot_stats_elapsed_us: u64,
        heaviest_fork_failures_elapsed_us: u64,
        bank_count: u64,
        process_ancestor_hashes_duplicate_slots_elapsed_us: u64,
        process_duplicate_confirmed_slots_elapsed_us: u64,
        process_unfrozen_gossip_verified_vote_hashes_elapsed_us: u64,
        process_popular_pruned_forks_elapsed_us: u64,
        process_duplicate_slots_elapsed_us: u64,
        repair_correct_slots_elapsed_us: u64,
        retransmit_not_propagated_elapsed_us: u64,
    ) {
        self.collect_frozen_banks_elapsed_us += collect_frozen_banks_elapsed_us;
        self.compute_bank_stats_elapsed_us += compute_bank_stats_elapsed_us;
        self.select_vote_and_reset_forks_elapsed_us += select_vote_and_reset_forks_elapsed_us;
        self.reset_bank_elapsed_us += reset_bank_elapsed_us;
        self.voting_elapsed_us += voting_elapsed_us;
        self.select_forks_elapsed_us += select_forks_elapsed_us;
        self.compute_slot_stats_elapsed_us += compute_slot_stats_elapsed_us;
        self.heaviest_fork_failures_elapsed_us += heaviest_fork_failures_elapsed_us;
        self.bank_count += bank_count;
        self.process_ancestor_hashes_duplicate_slots_elapsed_us +=
            process_ancestor_hashes_duplicate_slots_elapsed_us;
        self.process_duplicate_confirmed_slots_elapsed_us +=
            process_duplicate_confirmed_slots_elapsed_us;
        self.process_unfrozen_gossip_verified_vote_hashes_elapsed_us +=
            process_unfrozen_gossip_verified_vote_hashes_elapsed_us;
        self.process_popular_pruned_forks_elapsed_us += process_popular_pruned_forks_elapsed_us;
        self.process_duplicate_slots_elapsed_us += process_duplicate_slots_elapsed_us;
        self.repair_correct_slots_elapsed_us += repair_correct_slots_elapsed_us;
        self.retransmit_not_propagated_elapsed_us += retransmit_not_propagated_elapsed_us;
    }

    fn update_common(
        &mut self,
        generate_new_bank_forks_elapsed_us: u64,
        replay_active_banks_elapsed_us: u64,
        start_leader_elapsed_us: u64,
        wait_receive_elapsed_us: u64,
    ) {
        self.loop_count += 1;
        self.generate_new_bank_forks_elapsed_us += generate_new_bank_forks_elapsed_us;
        self.replay_active_banks_elapsed_us += replay_active_banks_elapsed_us;
        self.start_leader_elapsed_us += start_leader_elapsed_us;
        self.wait_receive_elapsed_us += wait_receive_elapsed_us;

        self.maybe_submit();
    }

    fn maybe_submit(&mut self) {
        let now = timestamp();
        let elapsed_ms = now - self.last_submit;

        if elapsed_ms > 1000 {
            datapoint_info!(
                "replay-loop-voting-stats",
                ("generate_vote_us", self.generate_vote_us, i64),
                (
                    "update_commitment_cache_us",
                    self.update_commitment_cache_us,
                    i64
                ),
            );
            datapoint_info!(
                "replay-loop-timing-stats",
                ("loop_count", self.loop_count as i64, i64),
                ("total_elapsed_us", elapsed_ms * 1000, i64),
                (
                    "collect_frozen_banks_elapsed_us",
                    self.collect_frozen_banks_elapsed_us as i64,
                    i64
                ),
                (
                    "compute_bank_stats_elapsed_us",
                    self.compute_bank_stats_elapsed_us as i64,
                    i64
                ),
                (
                    "select_vote_and_reset_forks_elapsed_us",
                    self.select_vote_and_reset_forks_elapsed_us as i64,
                    i64
                ),
                (
                    "start_leader_elapsed_us",
                    self.start_leader_elapsed_us as i64,
                    i64
                ),
                (
                    "reset_bank_elapsed_us",
                    self.reset_bank_elapsed_us as i64,
                    i64
                ),
                ("voting_elapsed_us", self.voting_elapsed_us as i64, i64),
                (
                    "select_forks_elapsed_us",
                    self.select_forks_elapsed_us as i64,
                    i64
                ),
                (
                    "compute_slot_stats_elapsed_us",
                    self.compute_slot_stats_elapsed_us as i64,
                    i64
                ),
                (
                    "generate_new_bank_forks_elapsed_us",
                    self.generate_new_bank_forks_elapsed_us as i64,
                    i64
                ),
                (
                    "replay_active_banks_elapsed_us",
                    self.replay_active_banks_elapsed_us as i64,
                    i64
                ),
                (
                    "process_ancestor_hashes_duplicate_slots_elapsed_us",
                    self.process_ancestor_hashes_duplicate_slots_elapsed_us as i64,
                    i64
                ),
                (
                    "process_duplicate_confirmed_slots_elapsed_us",
                    self.process_duplicate_confirmed_slots_elapsed_us as i64,
                    i64
                ),
                (
                    "process_unfrozen_gossip_verified_vote_hashes_elapsed_us",
                    self.process_unfrozen_gossip_verified_vote_hashes_elapsed_us as i64,
                    i64
                ),
                (
                    "process_popular_pruned_forks_elapsed_us",
                    self.process_popular_pruned_forks_elapsed_us as i64,
                    i64
                ),
                (
                    "wait_receive_elapsed_us",
                    self.wait_receive_elapsed_us as i64,
                    i64
                ),
                (
                    "heaviest_fork_failures_elapsed_us",
                    self.heaviest_fork_failures_elapsed_us as i64,
                    i64
                ),
                ("bank_count", self.bank_count as i64, i64),
                (
                    "process_duplicate_slots_elapsed_us",
                    self.process_duplicate_slots_elapsed_us as i64,
                    i64
                ),
                (
                    "repair_correct_slots_elapsed_us",
                    self.repair_correct_slots_elapsed_us as i64,
                    i64
                ),
                (
                    "retransmit_not_propagated_elapsed_us",
                    self.retransmit_not_propagated_elapsed_us as i64,
                    i64
                ),
                (
                    "generate_new_bank_forks_read_lock_us",
                    self.generate_new_bank_forks_read_lock_us as i64,
                    i64
                ),
                (
                    "generate_new_bank_forks_get_slots_since_us",
                    self.generate_new_bank_forks_get_slots_since_us as i64,
                    i64
                ),
                (
                    "generate_new_bank_forks_loop_us",
                    self.generate_new_bank_forks_loop_us as i64,
                    i64
                ),
                (
                    "generate_new_bank_forks_write_lock_us",
                    self.generate_new_bank_forks_write_lock_us as i64,
                    i64
                ),
                (
                    "replay_blockstore_us",
                    self.replay_blockstore_us as i64,
                    i64
                ),
            );
            *self = ReplayLoopTiming::default();
            self.last_submit = now;
        }
    }
}

pub struct ReplayStage {
    t_replay: JoinHandle<()>,
    votor: Votor,
    commitment_service: AggregateCommitmentService,
}

impl ReplayStage {
    pub fn new(
        config: ReplayStageConfig,
        senders: ReplaySenders,
        receivers: ReplayReceivers,
    ) -> Result<Self, String> {
        let ReplayStageConfig {
            vote_account,
            authorized_voter_keypairs,
            exit,
            leader_schedule_cache,
            block_commitment_cache,
            wait_for_vote_to_start_leader,
            tower_storage,
            wait_to_vote_slot,
            replay_forks_threads,
            replay_transactions_threads,
            blockstore,
            bank_forks,
            cluster_info,
            poh_recorder,
            mut tower,
            vote_history,
            vote_history_storage,
            vote_tracker,
            cluster_slots,
            log_messages_bytes_limit,
            prioritization_fee_cache,
            banking_tracer,
            replay_highest_frozen,
            leader_window_notifier,
        } = config;

        let ReplaySenders {
            rpc_subscriptions,
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
        } = senders;

        let ReplayReceivers {
            ledger_signal_receiver,
            duplicate_slots_receiver,
            ancestor_duplicate_slots_receiver,
            duplicate_confirmed_slots_receiver,
            gossip_verified_vote_hash_receiver,
            popular_pruned_forks_receiver,
            bls_verified_message_receiver,
            votor_event_receiver,
        } = receivers;

        trace!("replay stage");

        // Start the replay stage loop
        let (lockouts_sender, commitment_sender, commitment_service) =
            AggregateCommitmentService::new(
                exit.clone(),
                block_commitment_cache.clone(),
                rpc_subscriptions.clone(),
            );

        // Alpenglow specific objects
        let votor_config = VotorConfig {
            exit: exit.clone(),
            vote_account,
            wait_to_vote_slot,
            wait_for_vote_to_start_leader,
            vote_history,
            vote_history_storage,
            authorized_voter_keypairs: authorized_voter_keypairs.clone(),
            blockstore: blockstore.clone(),
            bank_forks: bank_forks.clone(),
            cluster_info: cluster_info.clone(),
            leader_schedule_cache: leader_schedule_cache.clone(),
            rpc_subscriptions: rpc_subscriptions.clone(),
            accounts_background_request_sender: accounts_background_request_sender.clone(),
            bls_sender: bls_sender.clone(),
            commitment_sender: commitment_sender.clone(),
            drop_bank_sender: drop_bank_sender.clone(),
            bank_notification_sender: bank_notification_sender.clone(),
            leader_window_notifier,
            certificate_sender,
            event_sender: votor_event_sender.clone(),
            event_receiver: votor_event_receiver.clone(),
            own_vote_sender,
            bls_receiver: bls_verified_message_receiver,
        };
        let votor = Votor::new(votor_config);

        let mut first_alpenglow_slot = bank_forks
            .read()
            .unwrap()
            .root_bank()
            .feature_set
            .activated_slot(&solana_feature_set::secp256k1_program_enabled::id());

        let mut is_alpenglow_migration_complete = false;
        if let Some(first_alpenglow_slot) = first_alpenglow_slot {
            if bank_forks
                .read()
                .unwrap()
                .frozen_banks()
                .iter()
                .any(|(slot, _bank)| *slot >= first_alpenglow_slot)
            {
                info!("initiating alpenglow migration on startup");
                Self::initiate_alpenglow_migration(
                    &poh_recorder,
                    &mut is_alpenglow_migration_complete,
                );
                votor.start_migration();
            }
        }
        let mut highest_frozen_slot = bank_forks
            .read()
            .unwrap()
            .highest_frozen_bank()
            .map_or(0, |hfs| hfs.slot());
        *replay_highest_frozen.highest_frozen_slot.lock().unwrap() = highest_frozen_slot;

        let run_replay = move || {
            let verify_recyclers = VerifyRecyclers::default();
            let _exit = Finalizer::new(exit.clone());
            let mut identity_keypair = cluster_info.keypair().clone();
            let mut my_pubkey = identity_keypair.pubkey();

            if my_pubkey != tower.node_pubkey {
                // set-identity was called during the startup procedure, ensure the tower is consistent
                // before starting the loop. further calls to set-identity will reload the tower in the loop
                let my_old_pubkey = tower.node_pubkey;
                if !is_alpenglow_migration_complete {
                    tower = match Self::load_tower(
                        tower_storage.as_ref(),
                        &my_pubkey,
                        &vote_account,
                        &bank_forks,
                    ) {
                        Ok(tower) => tower,
                        Err(err) => {
                            error!(
                                "Unable to load new tower when attempting to change identity from {} \
                                to {} on ReplayStage startup, Exiting: {}",
                                my_old_pubkey, my_pubkey, err
                            );
                            // drop(_exit) will set the exit flag, eventually tearing down the entire process
                            return;
                        }
                    };
                    warn!(
                        "Identity changed during startup from {} to {}",
                        my_old_pubkey, my_pubkey
                    );
                }
            }
            let (mut progress, heaviest_subtree_fork_choice) =
                Self::initialize_progress_and_fork_choice_with_locked_bank_forks(
                    &bank_forks,
                    &my_pubkey,
                    &vote_account,
                    &blockstore,
                );
            let mut current_leader = None;
            let mut last_reset = Hash::default();
            let mut last_reset_bank_descendants = Vec::new();
            let mut partition_info = PartitionInfo::new();
            let mut skipped_slots_info = SkippedSlotsInfo::default();
            let mut replay_timing = ReplayLoopTiming::default();
            let duplicate_slots_tracker = DuplicateSlotsTracker::default();
            let duplicate_confirmed_slots: DuplicateConfirmedSlots =
                DuplicateConfirmedSlots::default();
            let epoch_slots_frozen_slots: EpochSlotsFrozenSlots = EpochSlotsFrozenSlots::default();
            let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
            let mut purge_repair_slot_counter = PurgeRepairSlotCounter::default();
            let unfrozen_gossip_verified_vote_hashes: UnfrozenGossipVerifiedVoteHashes =
                UnfrozenGossipVerifiedVoteHashes::default();
            let mut latest_validator_votes_for_frozen_banks: LatestValidatorVotesForFrozenBanks =
                LatestValidatorVotesForFrozenBanks::default();
            let mut tbft_structs = TowerBFTStructures {
                heaviest_subtree_fork_choice,
                duplicate_slots_tracker,
                duplicate_confirmed_slots,
                unfrozen_gossip_verified_vote_hashes,
                epoch_slots_frozen_slots,
            };
            let mut voted_signatures = Vec::new();
            let mut has_new_vote_been_rooted = !wait_for_vote_to_start_leader;
            let mut last_vote_refresh_time = LastVoteRefreshTime {
                last_refresh_time: Instant::now(),
                last_print_time: Instant::now(),
            };

            let (working_bank, in_vote_only_mode) = {
                let r_bank_forks = bank_forks.read().unwrap();
                (
                    r_bank_forks.working_bank(),
                    r_bank_forks.get_vote_only_mode_signal(),
                )
            };
            let mut last_threshold_failure_slot = 0;
            // Thread pool to (maybe) replay multiple threads in parallel
            let replay_mode = if replay_forks_threads.get() == 1 {
                ForkReplayMode::Serial
            } else {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(replay_forks_threads.get())
                    .thread_name(|i| format!("solReplayFork{i:02}"))
                    .build()
                    .expect("new rayon threadpool");
                ForkReplayMode::Parallel(pool)
            };
            // Thread pool to replay multiple transactions within one block in parallel
            let replay_tx_thread_pool = rayon::ThreadPoolBuilder::new()
                .num_threads(replay_transactions_threads.get())
                .thread_name(|i| format!("solReplayTx{i:02}"))
                .build()
                .expect("new rayon threadpool");

            if !is_alpenglow_migration_complete {
                // This reset is handled in block creation loop for alpenglow
                Self::reset_poh_recorder(
                    &my_pubkey,
                    &blockstore,
                    working_bank,
                    &poh_recorder,
                    &leader_schedule_cache,
                );
            }

            loop {
                // Stop getting entries if we get exit signal
                if exit.load(Ordering::Relaxed) {
                    break;
                }

                let mut generate_new_bank_forks_time =
                    Measure::start("generate_new_bank_forks_time");
                Self::generate_new_bank_forks(
                    &blockstore,
                    &bank_forks,
                    &leader_schedule_cache,
                    &rpc_subscriptions,
                    &slot_status_notifier,
                    &mut progress,
                    &mut replay_timing,
                );
                generate_new_bank_forks_time.stop();

                let mut tpu_has_bank = poh_recorder.read().unwrap().has_bank();

                let mut replay_active_banks_time = Measure::start("replay_active_banks_time");
                let (mut ancestors, mut descendants) = {
                    let r_bank_forks = bank_forks.read().unwrap();
                    (r_bank_forks.ancestors(), r_bank_forks.descendants())
                };
                let new_frozen_slots = Self::replay_active_banks(
                    &blockstore,
                    &bank_forks,
                    &my_pubkey,
                    &vote_account,
                    &mut progress,
                    transaction_status_sender.as_ref(),
                    block_meta_sender.as_ref(),
                    entry_notification_sender.as_ref(),
                    &verify_recyclers,
                    &replay_vote_sender,
                    &bank_notification_sender,
                    &rpc_subscriptions,
                    &slot_status_notifier,
                    &mut latest_validator_votes_for_frozen_banks,
                    &cluster_slots_update_sender,
                    &cost_update_sender,
                    &mut duplicate_slots_to_repair,
                    &ancestor_hashes_replay_update_sender,
                    block_metadata_notifier.clone(),
                    &mut replay_timing,
                    log_messages_bytes_limit,
                    &replay_mode,
                    &replay_tx_thread_pool,
                    &prioritization_fee_cache,
                    &mut purge_repair_slot_counter,
                    &poh_recorder,
                    first_alpenglow_slot,
                    (!is_alpenglow_migration_complete).then_some(&mut tbft_structs),
                    &mut is_alpenglow_migration_complete,
                    &votor_event_sender,
                );
                let did_complete_bank = !new_frozen_slots.is_empty();
                if is_alpenglow_migration_complete {
                    if let Some(highest) = new_frozen_slots.iter().max() {
                        if *highest > highest_frozen_slot {
                            highest_frozen_slot = *highest;
                            let mut l_highest_frozen =
                                replay_highest_frozen.highest_frozen_slot.lock().unwrap();
                            // Let the block creation loop know about this new frozen slot
                            *l_highest_frozen = *highest;
                            replay_highest_frozen.freeze_notification.notify_one();
                        }
                    }
                    if did_complete_bank {
                        let bank_forks_r = bank_forks.read().unwrap();
                        progress.handle_new_root(&bank_forks_r);
                    }
                }
                replay_active_banks_time.stop();

                let forks_root = bank_forks.read().unwrap().root();
                let start_leader_time = if !is_alpenglow_migration_complete {
                    // TODO(ashwin): This will be moved to the event coordinator once we figure out
                    // migration
                    for _ in votor_event_receiver.try_iter() {}

                    // Process cluster-agreed versions of duplicate slots for which we potentially
                    // have the wrong version. Our version was dead or pruned.
                    // Signalled by ancestor_hashes_service.
                    let mut process_ancestor_hashes_duplicate_slots_time =
                        Measure::start("process_ancestor_hashes_duplicate_slots");
                    Self::process_ancestor_hashes_duplicate_slots(
                        &my_pubkey,
                        &blockstore,
                        &ancestor_duplicate_slots_receiver,
                        &mut tbft_structs.duplicate_slots_tracker,
                        &tbft_structs.duplicate_confirmed_slots,
                        &mut tbft_structs.epoch_slots_frozen_slots,
                        &progress,
                        &mut tbft_structs.heaviest_subtree_fork_choice,
                        &bank_forks,
                        &mut duplicate_slots_to_repair,
                        &ancestor_hashes_replay_update_sender,
                        &mut purge_repair_slot_counter,
                    );
                    process_ancestor_hashes_duplicate_slots_time.stop();

                    // Check for any newly duplicate confirmed slots detected from gossip / replay
                    // Note: since this is tracked using both gossip & replay votes, stake is not
                    // rolled up from descendants.
                    let mut process_duplicate_confirmed_slots_time =
                        Measure::start("process_duplicate_confirmed_slots");
                    Self::process_duplicate_confirmed_slots(
                        &duplicate_confirmed_slots_receiver,
                        &blockstore,
                        &mut tbft_structs.duplicate_slots_tracker,
                        &mut tbft_structs.duplicate_confirmed_slots,
                        &mut tbft_structs.epoch_slots_frozen_slots,
                        &bank_forks,
                        &progress,
                        &mut tbft_structs.heaviest_subtree_fork_choice,
                        &mut duplicate_slots_to_repair,
                        &ancestor_hashes_replay_update_sender,
                        &mut purge_repair_slot_counter,
                    );
                    process_duplicate_confirmed_slots_time.stop();

                    // Ingest any new verified votes from gossip. Important for fork choice
                    // and switching proofs because these may be votes that haven't yet been
                    // included in a block, so we may not have yet observed these votes just
                    // by replaying blocks.
                    let mut process_unfrozen_gossip_verified_vote_hashes_time =
                        Measure::start("process_gossip_verified_vote_hashes");
                    Self::process_gossip_verified_vote_hashes(
                        &gossip_verified_vote_hash_receiver,
                        &mut tbft_structs.unfrozen_gossip_verified_vote_hashes,
                        &tbft_structs.heaviest_subtree_fork_choice,
                        &mut latest_validator_votes_for_frozen_banks,
                    );
                    for _ in gossip_verified_vote_hash_receiver.try_iter() {}
                    process_unfrozen_gossip_verified_vote_hashes_time.stop();

                    let mut process_popular_pruned_forks_time =
                        Measure::start("process_popular_pruned_forks_time");
                    // Check for "popular" (52+% stake aggregated across versions/descendants) forks
                    // that are pruned, which would not be detected by normal means.
                    // Signalled by `repair_service`.
                    Self::process_popular_pruned_forks(
                        &popular_pruned_forks_receiver,
                        &blockstore,
                        &mut tbft_structs.duplicate_slots_tracker,
                        &mut tbft_structs.epoch_slots_frozen_slots,
                        &bank_forks,
                        &mut tbft_structs.heaviest_subtree_fork_choice,
                        &mut duplicate_slots_to_repair,
                        &ancestor_hashes_replay_update_sender,
                        &mut purge_repair_slot_counter,
                    );
                    process_popular_pruned_forks_time.stop();

                    // Check to remove any duplicated slots from fork choice
                    let mut process_duplicate_slots_time =
                        Measure::start("process_duplicate_slots");
                    if !tpu_has_bank {
                        Self::process_duplicate_slots(
                            &blockstore,
                            &duplicate_slots_receiver,
                            &mut tbft_structs.duplicate_slots_tracker,
                            &tbft_structs.duplicate_confirmed_slots,
                            &mut tbft_structs.epoch_slots_frozen_slots,
                            &bank_forks,
                            &progress,
                            &mut tbft_structs.heaviest_subtree_fork_choice,
                            &mut duplicate_slots_to_repair,
                            &ancestor_hashes_replay_update_sender,
                            &mut purge_repair_slot_counter,
                        );
                    }
                    process_duplicate_slots_time.stop();

                    let mut collect_frozen_banks_time = Measure::start("frozen_banks");
                    let mut frozen_banks: Vec<_> = bank_forks
                        .read()
                        .unwrap()
                        .frozen_banks()
                        .into_iter()
                        .filter(|(slot, _)| *slot >= forks_root)
                        .map(|(_, bank)| bank)
                        .collect();
                    collect_frozen_banks_time.stop();

                    let mut compute_bank_stats_time = Measure::start("compute_bank_stats");
                    let newly_computed_slot_stats = Self::compute_bank_stats(
                        &vote_account,
                        &ancestors,
                        &mut frozen_banks,
                        &mut tower,
                        &mut progress,
                        &vote_tracker,
                        &cluster_slots,
                        &bank_forks,
                        &mut tbft_structs.heaviest_subtree_fork_choice,
                        &mut latest_validator_votes_for_frozen_banks,
                    );
                    compute_bank_stats_time.stop();

                    let mut compute_slot_stats_time = Measure::start("compute_slot_stats_time");
                    for slot in newly_computed_slot_stats {
                        let fork_stats = progress.get_fork_stats(slot).unwrap();
                        let duplicate_confirmed_forks = Self::tower_duplicate_confirmed_forks(
                            &tower,
                            &fork_stats.voted_stakes,
                            fork_stats.total_stake,
                            &progress,
                            &bank_forks,
                        );

                        Self::mark_slots_duplicate_confirmed(
                            &duplicate_confirmed_forks,
                            &blockstore,
                            &bank_forks,
                            &mut progress,
                            &mut tbft_structs.duplicate_slots_tracker,
                            &mut tbft_structs.heaviest_subtree_fork_choice,
                            &mut tbft_structs.epoch_slots_frozen_slots,
                            &mut duplicate_slots_to_repair,
                            &ancestor_hashes_replay_update_sender,
                            &mut purge_repair_slot_counter,
                            &mut tbft_structs.duplicate_confirmed_slots,
                        );
                    }
                    compute_slot_stats_time.stop();

                    let mut select_forks_time = Measure::start("select_forks_time");
                    let (heaviest_bank, heaviest_bank_on_same_voted_fork) = tbft_structs
                        .heaviest_subtree_fork_choice
                        .select_forks(&frozen_banks, &tower, &progress, &ancestors, &bank_forks);
                    select_forks_time.stop();

                    Self::check_for_vote_only_mode(
                        heaviest_bank.slot(),
                        forks_root,
                        &in_vote_only_mode,
                        &bank_forks,
                    );

                    let mut select_vote_and_reset_forks_time =
                        Measure::start("select_vote_and_reset_forks");
                    let SelectVoteAndResetForkResult {
                        vote_bank,
                        reset_bank,
                        heaviest_fork_failures,
                    } = select_vote_and_reset_forks(
                        &heaviest_bank,
                        heaviest_bank_on_same_voted_fork.as_ref(),
                        &ancestors,
                        &descendants,
                        &progress,
                        &mut tower,
                        &latest_validator_votes_for_frozen_banks,
                        &tbft_structs.heaviest_subtree_fork_choice,
                    );
                    select_vote_and_reset_forks_time.stop();

                    if vote_bank.is_none() {
                        Self::maybe_refresh_last_vote(
                            &mut tower,
                            &progress,
                            heaviest_bank_on_same_voted_fork,
                            &vote_account,
                            &identity_keypair,
                            &authorized_voter_keypairs.read().unwrap(),
                            &mut voted_signatures,
                            has_new_vote_been_rooted,
                            &mut last_vote_refresh_time,
                            &voting_sender,
                            wait_to_vote_slot,
                        );
                    }

                    let mut heaviest_fork_failures_time =
                        Measure::start("heaviest_fork_failures_time");
                    if tower.is_recent(heaviest_bank.slot()) && !heaviest_fork_failures.is_empty() {
                        Self::log_heaviest_fork_failures(
                            &heaviest_fork_failures,
                            &bank_forks,
                            &tower,
                            &progress,
                            &ancestors,
                            &heaviest_bank,
                            &mut last_threshold_failure_slot,
                        );
                    }
                    heaviest_fork_failures_time.stop();

                    let mut voting_time = Measure::start("voting_time");
                    // Vote on a fork
                    if let Some((ref vote_bank, ref switch_fork_decision)) = vote_bank {
                        if let Some(votable_leader) =
                            leader_schedule_cache.slot_leader_at(vote_bank.slot(), Some(vote_bank))
                        {
                            Self::log_leader_change(
                                &my_pubkey,
                                vote_bank.slot(),
                                &mut current_leader,
                                &votable_leader,
                            );
                        }

                        if let Err(e) = Self::handle_votable_bank(
                            vote_bank,
                            switch_fork_decision,
                            &bank_forks,
                            &mut tower,
                            &mut progress,
                            &vote_account,
                            &identity_keypair,
                            &authorized_voter_keypairs.read().unwrap(),
                            &blockstore,
                            &leader_schedule_cache,
                            &lockouts_sender,
                            &accounts_background_request_sender,
                            &rpc_subscriptions,
                            &block_commitment_cache,
                            &bank_notification_sender,
                            &mut voted_signatures,
                            &mut has_new_vote_been_rooted,
                            &mut replay_timing,
                            &voting_sender,
                            &drop_bank_sender,
                            wait_to_vote_slot,
                            &mut first_alpenglow_slot,
                            &mut tbft_structs,
                        ) {
                            error!("Unable to set root: {e}");
                            return;
                        }
                    }
                    voting_time.stop();

                    let mut reset_bank_time = Measure::start("reset_bank");
                    // Reset onto a fork
                    if let Some(reset_bank) = reset_bank {
                        if last_reset == reset_bank.last_blockhash() {
                            let reset_bank_descendants = Self::get_active_descendants(
                                reset_bank.slot(),
                                &progress,
                                &blockstore,
                            );
                            if reset_bank_descendants != last_reset_bank_descendants {
                                last_reset_bank_descendants = reset_bank_descendants;
                                poh_recorder
                                    .write()
                                    .unwrap()
                                    .update_start_bank_active_descendants(
                                        &last_reset_bank_descendants,
                                    );
                            }
                        } else {
                            info!(
                                "vote bank: {:?} reset bank: {:?}",
                                vote_bank.as_ref().map(|(b, switch_fork_decision)| (
                                    b.slot(),
                                    switch_fork_decision
                                )),
                                reset_bank.slot(),
                            );
                            let fork_progress = progress
                                .get(&reset_bank.slot())
                                .expect("bank to reset to must exist in progress map");
                            datapoint_info!(
                                "blocks_produced",
                                ("num_blocks_on_fork", fork_progress.num_blocks_on_fork, i64),
                                (
                                    "num_dropped_blocks_on_fork",
                                    fork_progress.num_dropped_blocks_on_fork,
                                    i64
                                ),
                            );

                            if my_pubkey != cluster_info.id() && !is_alpenglow_migration_complete {
                                identity_keypair = cluster_info.keypair().clone();
                                let my_old_pubkey = my_pubkey;
                                my_pubkey = identity_keypair.pubkey();

                                // Load the new identity's tower
                                tower = match Self::load_tower(
                                    tower_storage.as_ref(),
                                    &my_pubkey,
                                    &vote_account,
                                    &bank_forks,
                                ) {
                                    Ok(tower) => tower,
                                    Err(err) => {
                                        error!(
                                            "Unable to load new tower when attempting to change \
                                         identity from {} to {} on set-identity, Exiting: {}",
                                            my_old_pubkey, my_pubkey, err
                                        );
                                        // drop(_exit) will set the exit flag, eventually tearing down the entire process
                                        return;
                                    }
                                };
                                // Ensure the validator can land votes with the new identity before
                                // becoming leader
                                has_new_vote_been_rooted = !wait_for_vote_to_start_leader;
                                warn!("Identity changed from {} to {}", my_old_pubkey, my_pubkey);
                            }

                            Self::reset_poh_recorder(
                                &my_pubkey,
                                &blockstore,
                                reset_bank.clone(),
                                &poh_recorder,
                                &leader_schedule_cache,
                            );
                            last_reset = reset_bank.last_blockhash();
                            last_reset_bank_descendants = vec![];
                            tpu_has_bank = false;

                            if let Some(last_voted_slot) = tower.last_voted_slot() {
                                // If the current heaviest bank is not a descendant of the last voted slot,
                                // there must be a partition
                                partition_info.update(
                                    Self::is_partition_detected(
                                        &ancestors,
                                        last_voted_slot,
                                        heaviest_bank.slot(),
                                    ),
                                    heaviest_bank.slot(),
                                    last_voted_slot,
                                    reset_bank.slot(),
                                    heaviest_fork_failures,
                                );
                            }
                        }
                    }
                    reset_bank_time.stop();

                    let mut dump_then_repair_correct_slots_time =
                        Measure::start("dump_then_repair_correct_slots_time");
                    // Used for correctness check
                    let poh_bank = poh_recorder.read().unwrap().bank();
                    // Dump any duplicate slots that have been confirmed by the network in
                    // anticipation of repairing the confirmed version of the slot.
                    //
                    // Has to be before `maybe_start_leader()`. Otherwise, `ancestors` and `descendants`
                    // will be outdated, and we cannot assume `poh_bank` will be in either of these maps.
                    Self::dump_then_repair_correct_slots(
                        &mut duplicate_slots_to_repair,
                        &mut ancestors,
                        &mut descendants,
                        &mut progress,
                        &bank_forks,
                        &blockstore,
                        poh_bank.map(|bank| bank.slot()),
                        &mut purge_repair_slot_counter,
                        &dumped_slots_sender,
                        &my_pubkey,
                        &leader_schedule_cache,
                    );
                    dump_then_repair_correct_slots_time.stop();

                    let mut retransmit_not_propagated_time =
                        Measure::start("retransmit_not_propagated_time");
                    Self::retransmit_latest_unpropagated_leader_slot(
                        &poh_recorder,
                        &retransmit_slots_sender,
                        &mut progress,
                    );
                    retransmit_not_propagated_time.stop();

                    // From this point on, its not safe to use ancestors/descendants since maybe_start_leader
                    // may add a bank that will not included in either of these maps.
                    drop(ancestors);
                    drop(descendants);
                    replay_timing.update_non_alpenglow(
                        collect_frozen_banks_time.as_us(),
                        compute_bank_stats_time.as_us(),
                        select_vote_and_reset_forks_time.as_us(),
                        reset_bank_time.as_us(),
                        voting_time.as_us(),
                        select_forks_time.as_us(),
                        compute_slot_stats_time.as_us(),
                        heaviest_fork_failures_time.as_us(),
                        u64::from(did_complete_bank),
                        process_ancestor_hashes_duplicate_slots_time.as_us(),
                        process_duplicate_confirmed_slots_time.as_us(),
                        process_unfrozen_gossip_verified_vote_hashes_time.as_us(),
                        process_popular_pruned_forks_time.as_us(),
                        process_duplicate_slots_time.as_us(),
                        dump_then_repair_correct_slots_time.as_us(),
                        retransmit_not_propagated_time.as_us(),
                    );

                    let mut start_leader_time = Measure::start("start_leader_time");
                    if !tpu_has_bank {
                        Self::maybe_start_leader(
                            &my_pubkey,
                            &bank_forks,
                            &poh_recorder,
                            &leader_schedule_cache,
                            &rpc_subscriptions,
                            &slot_status_notifier,
                            &mut progress,
                            &retransmit_slots_sender,
                            &mut skipped_slots_info,
                            &banking_tracer,
                            has_new_vote_been_rooted,
                            transaction_status_sender.is_some(),
                            &first_alpenglow_slot,
                            &mut is_alpenglow_migration_complete,
                        );

                        let poh_bank = poh_recorder.read().unwrap().bank();
                        if let Some(bank) = poh_bank {
                            Self::log_leader_change(
                                &my_pubkey,
                                bank.slot(),
                                &mut current_leader,
                                &my_pubkey,
                            );
                        }
                    }
                    start_leader_time.stop();
                    start_leader_time
                } else {
                    let mut start_leader_time = Measure::start("start_leader_time");
                    start_leader_time.stop();
                    start_leader_time
                };

                let mut wait_receive_time = Measure::start("wait_receive_time");
                if !did_complete_bank {
                    // only wait for the signal if we did not just process a bank; maybe there are more slots available

                    let timer = Duration::from_millis(100);
                    let result = ledger_signal_receiver.recv_timeout(timer);
                    match result {
                        Err(RecvTimeoutError::Timeout) => (),
                        Err(_) => break,
                        Ok(_) => trace!("blockstore signal"),
                    };
                }
                wait_receive_time.stop();
                replay_timing.update_common(
                    generate_new_bank_forks_time.as_us(),
                    replay_active_banks_time.as_us(),
                    start_leader_time.as_us(),
                    wait_receive_time.as_us(),
                );
            }
        };
        let t_replay = Builder::new()
            .name("solReplayStage".to_string())
            .spawn(run_replay)
            .unwrap();

        Ok(Self {
            t_replay,
            votor,
            commitment_service,
        })
    }

    /// Loads the tower from `tower_storage` with identity `node_pubkey`.
    ///
    /// If the tower is missing or too old, a tower is constructed from bank forks.
    fn load_tower(
        tower_storage: &dyn TowerStorage,
        node_pubkey: &Pubkey,
        vote_account: &Pubkey,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<Tower, TowerError> {
        let tower = Tower::restore(tower_storage, node_pubkey).and_then(|restored_tower| {
            let root_bank = bank_forks.read().unwrap().root_bank();
            let slot_history = root_bank.get_slot_history();
            restored_tower.adjust_lockouts_after_replay(root_bank.slot(), &slot_history)
        });
        match tower {
            Ok(tower) => Ok(tower),
            Err(err) if err.is_file_missing() => {
                warn!(
                    "Failed to load tower, file missing for {node_pubkey}: {err}. Creating a new \
                     tower from bankforks."
                );
                Ok(Tower::new_from_bankforks(
                    &bank_forks.read().unwrap(),
                    node_pubkey,
                    vote_account,
                ))
            }
            Err(err) if err.is_too_old() => {
                warn!(
                    "Failed to load tower, too old for {node_pubkey}: {err}. Creating a new tower \
                     from bankforks."
                );
                Ok(Tower::new_from_bankforks(
                    &bank_forks.read().unwrap(),
                    node_pubkey,
                    vote_account,
                ))
            }
            Err(err) => Err(err),
        }
    }

    fn check_for_vote_only_mode(
        heaviest_bank_slot: Slot,
        forks_root: Slot,
        in_vote_only_mode: &AtomicBool,
        bank_forks: &RwLock<BankForks>,
    ) {
        if heaviest_bank_slot.saturating_sub(forks_root) > MAX_ROOT_DISTANCE_FOR_VOTE_ONLY {
            if !in_vote_only_mode.load(Ordering::Relaxed)
                && in_vote_only_mode
                    .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                let bank_forks = bank_forks.read().unwrap();
                datapoint_warn!(
                    "bank_forks-entering-vote-only-mode",
                    ("banks_len", bank_forks.len(), i64),
                    ("heaviest_bank", heaviest_bank_slot, i64),
                    ("root", bank_forks.root(), i64),
                );
            }
        } else if in_vote_only_mode.load(Ordering::Relaxed)
            && in_vote_only_mode
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let bank_forks = bank_forks.read().unwrap();
            datapoint_warn!(
                "bank_forks-exiting-vote-only-mode",
                ("banks_len", bank_forks.len(), i64),
                ("heaviest_bank", heaviest_bank_slot, i64),
                ("root", bank_forks.root(), i64),
            );
        }
    }

    fn maybe_retransmit_unpropagated_slots(
        metric_name: &'static str,
        retransmit_slots_sender: &Sender<Slot>,
        progress: &mut ProgressMap,
        latest_leader_slot: Slot,
    ) {
        let first_leader_group_slot = first_of_consecutive_leader_slots(latest_leader_slot);

        for slot in first_leader_group_slot..=latest_leader_slot {
            let is_propagated = progress.is_propagated(slot);
            if let Some(retransmit_info) = progress.get_retransmit_info_mut(slot) {
                if !is_propagated.expect(
                    "presence of retransmit_info ensures that propagation status is present",
                ) {
                    if retransmit_info.reached_retransmit_threshold() {
                        info!(
                            "Retrying retransmit: latest_leader_slot={} slot={} \
                             retransmit_info={:?}",
                            latest_leader_slot, slot, &retransmit_info,
                        );
                        datapoint_info!(
                            metric_name,
                            ("latest_leader_slot", latest_leader_slot, i64),
                            ("slot", slot, i64),
                            ("retry_iteration", retransmit_info.retry_iteration, i64),
                        );
                        let _ = retransmit_slots_sender.send(slot);
                        retransmit_info.increment_retry_iteration();
                    } else {
                        debug!(
                            "Bypass retransmit of slot={} retransmit_info={:?}",
                            slot, &retransmit_info
                        );
                    }
                }
            }
        }
    }

    fn retransmit_latest_unpropagated_leader_slot(
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        retransmit_slots_sender: &Sender<Slot>,
        progress: &mut ProgressMap,
    ) {
        let start_slot = poh_recorder.read().unwrap().start_slot();

        // It is possible that bank corresponding to `start_slot` has been
        // dumped, so we need to double check it exists before proceeding
        if !progress.contains(&start_slot) {
            warn!(
                "Poh start slot {start_slot}, is missing from progress map. This indicates that \
                 we are in the middle of a dump and repair. Skipping retransmission of \
                 unpropagated leader slots"
            );
            return;
        }

        if let (false, Some(latest_leader_slot)) =
            progress.get_leader_propagation_slot_must_exist(start_slot)
        {
            debug!(
                "Slot not propagated: start_slot={} latest_leader_slot={}",
                start_slot, latest_leader_slot
            );
            Self::maybe_retransmit_unpropagated_slots(
                "replay_stage-retransmit-timing-based",
                retransmit_slots_sender,
                progress,
                latest_leader_slot,
            );
        }
    }

    fn is_partition_detected(
        ancestors: &HashMap<Slot, HashSet<Slot>>,
        last_voted_slot: Slot,
        heaviest_slot: Slot,
    ) -> bool {
        last_voted_slot != heaviest_slot
            && !ancestors
                .get(&heaviest_slot)
                .map(|ancestors| ancestors.contains(&last_voted_slot))
                .unwrap_or(true)
    }

    fn get_active_descendants(
        slot: Slot,
        progress: &ProgressMap,
        blockstore: &Blockstore,
    ) -> Vec<Slot> {
        let Some(slot_meta) = blockstore.meta(slot).ok().flatten() else {
            return vec![];
        };

        slot_meta
            .next_slots
            .iter()
            .filter(|slot| !progress.is_dead(**slot).unwrap_or_default())
            .copied()
            .collect()
    }

    fn initialize_progress_and_fork_choice_with_locked_bank_forks(
        bank_forks: &RwLock<BankForks>,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
        blockstore: &Blockstore,
    ) -> (ProgressMap, HeaviestSubtreeForkChoice) {
        let (root_bank, frozen_banks, duplicate_slot_hashes) = {
            let bank_forks = bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let duplicate_slots = blockstore
                // It is important that the root bank is not marked as duplicate on initialization.
                // Although this bank could contain a duplicate proof, the fact that it was rooted
                // either during a previous run or artificially means that we should ignore any
                // duplicate proofs for the root slot, thus we start consuming duplicate proofs
                // from the root slot + 1
                .duplicate_slots_iterator(root_bank.slot().saturating_add(1))
                .unwrap();
            let duplicate_slot_hashes = duplicate_slots.filter_map(|slot| {
                let bank = bank_forks.get(slot)?;
                Some((slot, bank.hash()))
            });
            (
                root_bank,
                bank_forks.frozen_banks().values().cloned().collect(),
                duplicate_slot_hashes.collect::<Vec<(Slot, Hash)>>(),
            )
        };

        Self::initialize_progress_and_fork_choice(
            &root_bank,
            frozen_banks,
            my_pubkey,
            vote_account,
            duplicate_slot_hashes,
        )
    }

    pub fn initialize_progress_and_fork_choice(
        root_bank: &Bank,
        mut frozen_banks: Vec<Arc<Bank>>,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
        duplicate_slot_hashes: Vec<(Slot, Hash)>,
    ) -> (ProgressMap, HeaviestSubtreeForkChoice) {
        let mut progress = ProgressMap::default();

        frozen_banks.sort_by_key(|bank| bank.slot());

        // Initialize progress map with any root banks
        for bank in &frozen_banks {
            let prev_leader_slot = progress.get_bank_prev_leader_slot(bank);
            progress.insert(
                bank.slot(),
                ForkProgress::new_from_bank(bank, my_pubkey, vote_account, prev_leader_slot, 0, 0),
            );
        }
        let root = root_bank.slot();
        let mut heaviest_subtree_fork_choice = HeaviestSubtreeForkChoice::new_from_frozen_banks(
            (root, root_bank.hash()),
            &frozen_banks,
        );

        for slot_hash in duplicate_slot_hashes {
            heaviest_subtree_fork_choice.mark_fork_invalid_candidate(&slot_hash);
        }

        (progress, heaviest_subtree_fork_choice)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn dump_then_repair_correct_slots(
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestors: &mut HashMap<Slot, HashSet<Slot>>,
        descendants: &mut HashMap<Slot, HashSet<Slot>>,
        progress: &mut ProgressMap,
        bank_forks: &RwLock<BankForks>,
        blockstore: &Blockstore,
        poh_bank_slot: Option<Slot>,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
        dumped_slots_sender: &DumpedSlotsSender,
        my_pubkey: &Pubkey,
        leader_schedule_cache: &LeaderScheduleCache,
    ) {
        if duplicate_slots_to_repair.is_empty() {
            return;
        }

        let root_bank = bank_forks.read().unwrap().root_bank();
        let mut dumped = vec![];
        // TODO: handle if alternate version of descendant also got confirmed after ancestor was
        // confirmed, what happens then? Should probably keep track of dumped list and skip things
        // in `duplicate_slots_to_repair` that have already been dumped. Add test.
        duplicate_slots_to_repair.retain(|duplicate_slot, correct_hash| {
            // Should not dump duplicate slots if there is currently a poh bank building
            // on top of that slot, as BankingStage might still be referencing/touching that state
            // concurrently.
            // Luckily for us, because the fork choice rule removes duplicate slots from fork
            // choice, and this function is called after:
            // 1) We have picked a bank to reset to in `select_vote_and_reset_forks()`
            // 2) And also called `reset_poh_recorder()`
            // Then we should have reset to a fork that doesn't include the duplicate block,
            // which means any working bank in PohRecorder that was built on that duplicate fork
            // should have been cleared as well. However, if there is some violation of this guarantee,
            // then log here
            let is_poh_building_on_duplicate_fork = poh_bank_slot
                .map(|poh_bank_slot| {
                    ancestors
                        .get(&poh_bank_slot)
                        .expect("Poh bank should exist in BankForks and thus in ancestors map")
                        .contains(duplicate_slot)
                })
                .unwrap_or(false);

            let did_dump_repair = {
                if !is_poh_building_on_duplicate_fork {
                    let frozen_hash = bank_forks.read().unwrap().bank_hash(*duplicate_slot);
                    if let Some(frozen_hash) = frozen_hash {
                        if frozen_hash == *correct_hash {
                            warn!(
                                "Trying to dump slot {} with correct_hash {}",
                                *duplicate_slot, *correct_hash
                            );
                            return false;
                        } else if frozen_hash == Hash::default()
                            && !progress.is_dead(*duplicate_slot).expect(
                                "If slot exists in BankForks must exist in the progress map",
                            )
                        {
                            warn!(
                                "Trying to dump unfrozen slot {} that is not dead",
                                *duplicate_slot
                            );
                            return false;
                        }
                    } else {
                        warn!(
                            "Dumping slot {} which does not exist in bank forks (possibly pruned)",
                            *duplicate_slot
                        );
                    }

                    // Should not dump slots for which we were the leader
                    if Some(*my_pubkey)
                        == leader_schedule_cache.slot_leader_at(*duplicate_slot, None)
                    {
                        if let Some(bank) = bank_forks.read().unwrap().get(*duplicate_slot) {
                            bank_hash_details::write_bank_hash_details_file(&bank)
                                .map_err(|err| {
                                    warn!("Unable to write bank hash details file: {err}");
                                })
                                .ok();
                        } else {
                            warn!(
                                "Unable to get bank for slot {duplicate_slot} from bank forks \
                                 while attempting to write bank hash details file"
                            );
                        }
                        panic!(
                            "We are attempting to dump a block that we produced. This indicates \
                             that we are producing duplicate blocks, or that there is a bug in \
                             our runtime/replay code which causes us to compute different bank \
                             hashes than the rest of the cluster. We froze slot {duplicate_slot} \
                             with hash {frozen_hash:?} while the cluster hash is {correct_hash}"
                        );
                    }

                    let attempt_no = purge_repair_slot_counter
                        .entry(*duplicate_slot)
                        .and_modify(|x| *x += 1)
                        .or_insert(1);
                    if *attempt_no > MAX_REPAIR_RETRY_LOOP_ATTEMPTS {
                        panic!(
                            "We have tried to repair duplicate slot: {duplicate_slot} more than \
                             {MAX_REPAIR_RETRY_LOOP_ATTEMPTS} times and are unable to freeze a \
                             block with bankhash {correct_hash}, instead we have a block with \
                             bankhash {frozen_hash:?}. This is most likely a bug in the runtime. \
                             At this point manual intervention is needed to make progress. Exiting"
                        );
                    }

                    Self::purge_unconfirmed_duplicate_slot(
                        *duplicate_slot,
                        ancestors,
                        descendants,
                        progress,
                        &root_bank,
                        bank_forks,
                        blockstore,
                    );

                    dumped.push((*duplicate_slot, *correct_hash));

                    warn!(
                        "Notifying repair service to repair duplicate slot: {}, attempt {}",
                        *duplicate_slot, *attempt_no,
                    );
                    true
                } else {
                    warn!(
                        "PoH bank for slot {} is building on duplicate slot {}",
                        poh_bank_slot.unwrap(),
                        duplicate_slot
                    );
                    false
                }
            };

            // If we dumped/repaired, then no need to keep the slot in the set of pending work
            !did_dump_repair
        });

        // Notify repair of the dumped slots along with the correct hash
        trace!("Dumped {} slots", dumped.len());
        dumped_slots_sender.send(dumped).unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn process_ancestor_hashes_duplicate_slots(
        pubkey: &Pubkey,
        blockstore: &Blockstore,
        ancestor_duplicate_slots_receiver: &AncestorDuplicateSlotsReceiver,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        duplicate_confirmed_slots: &DuplicateConfirmedSlots,
        epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
        progress: &ProgressMap,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        bank_forks: &RwLock<BankForks>,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
    ) {
        let root = bank_forks.read().unwrap().root();
        for AncestorDuplicateSlotToRepair {
            slot_to_repair: (epoch_slots_frozen_slot, epoch_slots_frozen_hash),
            request_type,
        } in ancestor_duplicate_slots_receiver.try_iter()
        {
            warn!(
                "{} ReplayStage notified of duplicate slot from ancestor hashes service but we \
                 observed as {}: {:?}",
                pubkey,
                if request_type.is_pruned() {
                    "pruned"
                } else {
                    "dead"
                },
                (epoch_slots_frozen_slot, epoch_slots_frozen_hash),
            );
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new_from_state(
                epoch_slots_frozen_slot,
                epoch_slots_frozen_hash,
                duplicate_confirmed_slots,
                fork_choice,
                || progress.is_dead(epoch_slots_frozen_slot).unwrap_or(false),
                || {
                    bank_forks
                        .read()
                        .unwrap()
                        .get(epoch_slots_frozen_slot)
                        .map(|b| b.hash())
                },
                request_type.is_pruned(),
            );
            check_slot_agrees_with_cluster(
                epoch_slots_frozen_slot,
                root,
                blockstore,
                duplicate_slots_tracker,
                epoch_slots_frozen_slots,
                fork_choice,
                duplicate_slots_to_repair,
                ancestor_hashes_replay_update_sender,
                purge_repair_slot_counter,
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
            );
        }
    }

    fn purge_unconfirmed_duplicate_slot(
        duplicate_slot: Slot,
        ancestors: &mut HashMap<Slot, HashSet<Slot>>,
        descendants: &mut HashMap<Slot, HashSet<Slot>>,
        progress: &mut ProgressMap,
        root_bank: &Bank,
        bank_forks: &RwLock<BankForks>,
        blockstore: &Blockstore,
    ) {
        warn!("purging slot {}", duplicate_slot);

        // Doesn't need to be root bank, just needs a common bank to
        // access the status cache and accounts
        let slot_descendants = descendants.get(&duplicate_slot).cloned();
        if slot_descendants.is_none() {
            // Root has already moved past this slot, no need to purge it
            if root_bank.slot() <= duplicate_slot {
                blockstore.clear_unconfirmed_slot(duplicate_slot);
            }

            return;
        }

        // Clear the ancestors/descendants map to keep them
        // consistent
        let slot_descendants = slot_descendants.unwrap();
        Self::purge_ancestors_descendants(
            duplicate_slot,
            &slot_descendants,
            ancestors,
            descendants,
        );

        // Grab the Slot and BankId's of the banks we need to purge, then clear the banks
        // from BankForks
        let (slots_to_purge, removed_banks): (Vec<(Slot, BankId)>, Vec<BankWithScheduler>) = {
            let mut w_bank_forks = bank_forks.write().unwrap();
            w_bank_forks.dump_slots(
                slot_descendants
                    .iter()
                    .chain(std::iter::once(&duplicate_slot)),
            )
        };

        // Clear the accounts for these slots so that any ongoing RPC scans fail.
        // These have to be atomically cleared together in the same batch, in order
        // to prevent RPC from seeing inconsistent results in scans.
        root_bank.remove_unrooted_slots(&slots_to_purge);

        // Once the slots above have been purged, now it's safe to remove the banks from
        // BankForks, allowing the Bank::drop() purging to run and not race with the
        // `remove_unrooted_slots()` call.
        drop(removed_banks);

        for (slot, slot_id) in slots_to_purge {
            // Clear the slot signatures from status cache for this slot.
            // TODO: What about RPC queries that had already cloned the Bank for this slot
            // and are looking up the signature for this slot?
            root_bank.clear_slot_signatures(slot);

            // Remove cached entries of the programs that were deployed in this slot.
            root_bank.prune_program_cache_by_deployment_slot(slot);

            if let Some(bank_hash) = blockstore.get_bank_hash(slot) {
                // If a descendant was successfully replayed and chained from a duplicate it must
                // also be a duplicate. In this case we *need* to repair it, so we clear from
                // blockstore.
                warn!(
                    "purging duplicate descendant: {} with slot_id {} and bank hash {}, of slot {}",
                    slot, slot_id, bank_hash, duplicate_slot
                );
                // Clear the slot-related data in blockstore. This will:
                // 1) Clear old shreds allowing new ones to be inserted
                // 2) Clear the "dead" flag allowing ReplayStage to start replaying
                // this slot
                blockstore.clear_unconfirmed_slot(slot);
            } else if slot == duplicate_slot {
                warn!("purging duplicate slot: {} with slot_id {}", slot, slot_id);
                blockstore.clear_unconfirmed_slot(slot);
            } else {
                // If a descendant was unable to replay and chained from a duplicate, it is not
                // necessary to repair it. It is most likely that this block is fine, and will
                // replay on successful repair of the parent. If this block is also a duplicate, it
                // will be handled in the next round of repair/replay - so we just clear the dead
                // flag for now.
                warn!(
                    "not purging descendant {slot} of slot {duplicate_slot} as it is dead. \
                     resetting dead flag instead"
                );
                // Clear the "dead" flag allowing ReplayStage to start replaying
                // this slot once the parent is repaired
                blockstore.remove_dead_slot(slot).unwrap();
            }

            // Clear the progress map of these forks
            let _ = progress.remove(&slot);
        }
    }

    // Purge given slot and all its descendants from the `ancestors` and
    // `descendants` structures so that they're consistent with `BankForks`
    // and the `progress` map.
    fn purge_ancestors_descendants(
        slot: Slot,
        slot_descendants: &HashSet<Slot>,
        ancestors: &mut HashMap<Slot, HashSet<Slot>>,
        descendants: &mut HashMap<Slot, HashSet<Slot>>,
    ) {
        if !ancestors.contains_key(&slot) {
            // Slot has already been purged
            return;
        }

        // Purge this slot from each of its ancestors' `descendants` maps
        for a in ancestors
            .get(&slot)
            .expect("must exist based on earlier check")
        {
            descendants
                .get_mut(a)
                .expect("If exists in ancestor map must exist in descendants map")
                .retain(|d| *d != slot && !slot_descendants.contains(d));
        }
        ancestors
            .remove(&slot)
            .expect("must exist based on earlier check");

        // Purge all the descendants of this slot from both maps
        for descendant in slot_descendants {
            ancestors.remove(descendant).expect("must exist");
            descendants
                .remove(descendant)
                .expect("must exist based on earlier check");
        }
        descendants
            .remove(&slot)
            .expect("must exist based on earlier check");
    }

    #[allow(clippy::too_many_arguments)]
    fn process_popular_pruned_forks(
        popular_pruned_forks_receiver: &PopularPrunedForksReceiver,
        blockstore: &Blockstore,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
        bank_forks: &RwLock<BankForks>,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
    ) {
        let root = bank_forks.read().unwrap().root();
        for new_popular_pruned_slots in popular_pruned_forks_receiver.try_iter() {
            for new_popular_pruned_slot in new_popular_pruned_slots {
                if new_popular_pruned_slot <= root {
                    continue;
                }
                check_slot_agrees_with_cluster(
                    new_popular_pruned_slot,
                    root,
                    blockstore,
                    duplicate_slots_tracker,
                    epoch_slots_frozen_slots,
                    fork_choice,
                    duplicate_slots_to_repair,
                    ancestor_hashes_replay_update_sender,
                    purge_repair_slot_counter,
                    SlotStateUpdate::PopularPrunedFork,
                );
            }
        }
    }

    // Check for any newly duplicate confirmed slots by the cluster.
    // This only tracks duplicate slot confirmations on the exact
    // single slots and does not account for votes on their descendants. Used solely
    // for duplicate slot recovery.
    #[allow(clippy::too_many_arguments)]
    fn process_duplicate_confirmed_slots(
        duplicate_confirmed_slots_receiver: &DuplicateConfirmedSlotsReceiver,
        blockstore: &Blockstore,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        duplicate_confirmed_slots: &mut DuplicateConfirmedSlots,
        epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
        bank_forks: &RwLock<BankForks>,
        progress: &ProgressMap,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
    ) {
        let root = bank_forks.read().unwrap().root();
        for new_duplicate_confirmed_slots in duplicate_confirmed_slots_receiver.try_iter() {
            for (confirmed_slot, duplicate_confirmed_hash) in new_duplicate_confirmed_slots {
                if confirmed_slot <= root {
                    continue;
                } else if let Some(prev_hash) =
                    duplicate_confirmed_slots.insert(confirmed_slot, duplicate_confirmed_hash)
                {
                    assert_eq!(
                        prev_hash, duplicate_confirmed_hash,
                        "Additional duplicate confirmed notification for slot {confirmed_slot} \
                         with a different hash"
                    );
                    // Already processed this signal
                    continue;
                }

                let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
                    duplicate_confirmed_hash,
                    || progress.is_dead(confirmed_slot).unwrap_or(false),
                    || bank_forks.read().unwrap().bank_hash(confirmed_slot),
                );
                check_slot_agrees_with_cluster(
                    confirmed_slot,
                    root,
                    blockstore,
                    duplicate_slots_tracker,
                    epoch_slots_frozen_slots,
                    fork_choice,
                    duplicate_slots_to_repair,
                    ancestor_hashes_replay_update_sender,
                    purge_repair_slot_counter,
                    SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
                );
            }
        }
    }

    fn process_gossip_verified_vote_hashes(
        gossip_verified_vote_hash_receiver: &GossipVerifiedVoteHashReceiver,
        unfrozen_gossip_verified_vote_hashes: &mut UnfrozenGossipVerifiedVoteHashes,
        heaviest_subtree_fork_choice: &HeaviestSubtreeForkChoice,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    ) {
        for (pubkey, slot, hash) in gossip_verified_vote_hash_receiver.try_iter() {
            let is_frozen = heaviest_subtree_fork_choice.contains_block(&(slot, hash));
            // cluster_info_vote_listener will ensure it doesn't push duplicates
            unfrozen_gossip_verified_vote_hashes.add_vote(
                pubkey,
                slot,
                hash,
                is_frozen,
                latest_validator_votes_for_frozen_banks,
            )
        }
    }

    // Checks for and handle forks with duplicate slots.
    #[allow(clippy::too_many_arguments)]
    fn process_duplicate_slots(
        blockstore: &Blockstore,
        duplicate_slots_receiver: &DuplicateSlotReceiver,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        duplicate_confirmed_slots: &DuplicateConfirmedSlots,
        epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
        bank_forks: &RwLock<BankForks>,
        progress: &ProgressMap,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
    ) {
        let new_duplicate_slots: Vec<Slot> = duplicate_slots_receiver.try_iter().collect();
        let (root_slot, bank_hashes) = {
            let r_bank_forks = bank_forks.read().unwrap();
            let bank_hashes: Vec<Option<Hash>> = new_duplicate_slots
                .iter()
                .map(|duplicate_slot| r_bank_forks.bank_hash(*duplicate_slot))
                .collect();

            (r_bank_forks.root(), bank_hashes)
        };
        for (duplicate_slot, bank_hash) in
            new_duplicate_slots.into_iter().zip(bank_hashes.into_iter())
        {
            // WindowService should only send the signal once per slot
            let duplicate_state = DuplicateState::new_from_state(
                duplicate_slot,
                duplicate_confirmed_slots,
                fork_choice,
                || progress.is_dead(duplicate_slot).unwrap_or(false),
                || bank_hash,
            );
            check_slot_agrees_with_cluster(
                duplicate_slot,
                root_slot,
                blockstore,
                duplicate_slots_tracker,
                epoch_slots_frozen_slots,
                fork_choice,
                duplicate_slots_to_repair,
                ancestor_hashes_replay_update_sender,
                purge_repair_slot_counter,
                SlotStateUpdate::Duplicate(duplicate_state),
            );
        }
    }

    fn log_leader_change(
        my_pubkey: &Pubkey,
        bank_slot: Slot,
        current_leader: &mut Option<Pubkey>,
        new_leader: &Pubkey,
    ) {
        if let Some(ref current_leader) = current_leader {
            if current_leader != new_leader {
                let msg = if current_leader == my_pubkey {
                    ". I am no longer the leader"
                } else if new_leader == my_pubkey {
                    ". I am now the leader"
                } else {
                    ""
                };
                info!(
                    "LEADER CHANGE at slot: {} leader: {}{}",
                    bank_slot, new_leader, msg
                );
            }
        }
        current_leader.replace(new_leader.to_owned());
    }

    fn check_propagation_for_start_leader(
        poh_slot: Slot,
        parent_slot: Slot,
        progress_map: &ProgressMap,
    ) -> bool {
        // Assume `NUM_CONSECUTIVE_LEADER_SLOTS` = 4. Then `skip_propagated_check`
        // below is true if `poh_slot` is within the same `NUM_CONSECUTIVE_LEADER_SLOTS`
        // set of blocks as `latest_leader_slot`.
        //
        // Example 1 (`poh_slot` directly descended from `latest_leader_slot`):
        //
        // [B B B B] [B B B latest_leader_slot] poh_slot
        //
        // Example 2:
        //
        // [B latest_leader_slot B poh_slot]
        //
        // In this example, even if there's a block `B` on another fork between
        // `poh_slot` and `parent_slot`, because they're in the same
        // `NUM_CONSECUTIVE_LEADER_SLOTS` block, we still skip the propagated
        // check because it's still within the propagation grace period.
        //
        // We've already checked in start_leader() that parent_slot hasn't been
        // dumped, so we should get it in the progress map.
        if let Some(latest_leader_slot) =
            progress_map.get_latest_leader_slot_must_exist(parent_slot)
        {
            let skip_propagated_check =
                poh_slot - latest_leader_slot < NUM_CONSECUTIVE_LEADER_SLOTS;
            if skip_propagated_check {
                return true;
            }
        }

        // Note that `is_propagated(parent_slot)` doesn't necessarily check
        // propagation of `parent_slot`, it checks propagation of the latest ancestor
        // of `parent_slot` (hence the call to `get_latest_leader_slot()` in the
        // check above)
        progress_map
            .get_leader_propagation_slot_must_exist(parent_slot)
            .0
    }

    fn should_retransmit(poh_slot: Slot, last_retransmit_slot: &mut Slot) -> bool {
        if poh_slot < *last_retransmit_slot
            || poh_slot >= *last_retransmit_slot + NUM_CONSECUTIVE_LEADER_SLOTS
        {
            *last_retransmit_slot = poh_slot;
            true
        } else {
            false
        }
    }

    fn common_maybe_start_leader_checks(
        my_pubkey: &Pubkey,
        leader_schedule_cache: &LeaderScheduleCache,
        parent_bank: &Bank,
        bank_forks: &RwLock<BankForks>,
        maybe_my_leader_slot: Slot,
        has_new_vote_been_rooted: bool,
    ) -> bool {
        if !parent_bank.is_startup_verification_complete() {
            info!("startup verification incomplete, so skipping my leader slot");
            return false;
        }

        if bank_forks
            .read()
            .unwrap()
            .get(maybe_my_leader_slot)
            .is_some()
        {
            warn!(
                "{} already have bank in forks at {}?",
                my_pubkey, maybe_my_leader_slot
            );
            return false;
        }
        trace!(
            "{} my_leader_slot {} parent_slot {}",
            my_pubkey,
            maybe_my_leader_slot,
            parent_bank.slot(),
        );

        if let Some(next_leader) =
            leader_schedule_cache.slot_leader_at(maybe_my_leader_slot, Some(parent_bank))
        {
            if !has_new_vote_been_rooted {
                info!("Haven't landed a vote, so skipping my leader slot");
                return false;
            }

            trace!(
                "{} leader {} at poh slot: {}",
                my_pubkey,
                next_leader,
                maybe_my_leader_slot
            );

            // Poh: I guess I missed my slot
            // Alpenglow: It's not my slot yet
            if next_leader != *my_pubkey {
                return false;
            }
        } else {
            error!("{} No next leader found", my_pubkey);
            return false;
        }
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn poh_maybe_start_leader(
        my_pubkey: &Pubkey,
        my_leader_slot: Slot,
        parent_bank: &Arc<Bank>,
        poh_recorder: &RwLock<PohRecorder>,
        progress_map: &mut ProgressMap,
        skipped_slots_info: &mut SkippedSlotsInfo,
        bank_forks: &RwLock<BankForks>,
        retransmit_slots_sender: &Sender<Slot>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        track_transaction_indexes: bool,
        banking_tracer: &BankingTracer,
    ) -> bool {
        let parent_slot = parent_bank.slot();
        if !Self::check_propagation_for_start_leader(my_leader_slot, parent_slot, progress_map) {
            let latest_unconfirmed_leader_slot = progress_map
                .get_latest_leader_slot_must_exist(parent_slot)
                .expect(
                    "In order for propagated check to fail, latest leader must exist in \
                            progress map",
                );
            if my_leader_slot != skipped_slots_info.last_skipped_slot {
                datapoint_info!(
                    "replay_stage-skip_leader_slot",
                    ("slot", my_leader_slot, i64),
                    ("parent_slot", parent_slot, i64),
                    (
                        "latest_unconfirmed_leader_slot",
                        latest_unconfirmed_leader_slot,
                        i64
                    )
                );
                progress_map.log_propagated_stats(latest_unconfirmed_leader_slot, bank_forks);
                skipped_slots_info.last_skipped_slot = my_leader_slot;
            }
            if Self::should_retransmit(my_leader_slot, &mut skipped_slots_info.last_retransmit_slot)
            {
                Self::maybe_retransmit_unpropagated_slots(
                    "replay_stage-retransmit",
                    retransmit_slots_sender,
                    progress_map,
                    latest_unconfirmed_leader_slot,
                );
            }
            return false;
        }

        let root_slot = bank_forks.read().unwrap().root();
        datapoint_info!("replay_stage-my_leader_slot", ("slot", my_leader_slot, i64),);
        info!(
            "new fork:{} parent:{} (leader) root:{}",
            my_leader_slot, parent_slot, root_slot
        );

        let root_distance = my_leader_slot - root_slot;
        let vote_only_bank = if root_distance > MAX_ROOT_DISTANCE_FOR_VOTE_ONLY {
            datapoint_info!("vote-only-bank", ("slot", my_leader_slot, i64));
            true
        } else {
            false
        };

        let tpu_bank = Self::new_bank_from_parent_with_notify(
            parent_bank.clone(),
            my_leader_slot,
            root_slot,
            my_pubkey,
            rpc_subscriptions,
            slot_status_notifier,
            NewBankOptions { vote_only_bank },
        );
        // make sure parent is frozen for finalized hashes via the above
        // new()-ing of its child bank
        banking_tracer.hash_event(
            parent_bank.slot(),
            &parent_bank.last_blockhash(),
            &parent_bank.hash(),
        );

        update_bank_forks_and_poh_recorder_for_new_tpu_bank(
            bank_forks,
            poh_recorder,
            tpu_bank,
            track_transaction_indexes,
        );
        true
    }

    /// Checks if it is time for us to start producing a leader block.
    /// Fails if:
    /// - Current PoH has not satisfied criteria to start my leader block
    /// - Startup verification is not complete,
    /// - Bank forks already contains a bank for this leader slot
    /// - We have not landed a vote yet and the `wait_for_vote_to_start_leader` flag is set
    /// - We have failed the propagated check
    ///
    /// Returns whether a new working bank was created and inserted into bank forks.
    #[allow(clippy::too_many_arguments)]
    fn maybe_start_leader(
        my_pubkey: &Pubkey,
        bank_forks: &Arc<RwLock<BankForks>>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        progress_map: &mut ProgressMap,
        retransmit_slots_sender: &Sender<Slot>,
        skipped_slots_info: &mut SkippedSlotsInfo,
        banking_tracer: &Arc<BankingTracer>,
        has_new_vote_been_rooted: bool,
        track_transaction_indexes: bool,
        first_alpenglow_slot: &Option<Slot>,
        is_alpenglow_migration_complete: &mut bool,
    ) -> bool {
        // all the individual calls to poh_recorder.read() are designed to
        // increase granularity, decrease contention
        assert!(!poh_recorder.read().unwrap().has_bank());
        let (parent_slot, maybe_my_leader_slot) = {
            if !(*is_alpenglow_migration_complete) {
                // We need to check regular Poh in these situations
                match poh_recorder.read().unwrap().reached_leader_slot(my_pubkey) {
                    PohLeaderStatus::Reached {
                        poh_slot,
                        parent_slot,
                    } => (parent_slot, poh_slot),
                    PohLeaderStatus::NotReached => {
                        trace!("{} poh_recorder hasn't reached_leader_slot", my_pubkey);
                        return false;
                    }
                }
            } else {
                // Migration is already complete voting loop will handle the rest
                return false;
            }
        };

        // Check if migration is necessary
        if let Some(first_alpenglow_slot) = first_alpenglow_slot {
            if !(*is_alpenglow_migration_complete) && maybe_my_leader_slot >= *first_alpenglow_slot
            {
                // Initiate migration
                // TODO: need to keep the ticks around for parent slots in previous epoch
                // because reset below will delete those ticks
                info!(
                    "initiating alpenglow migration from maybe_start_leader() for slot {}",
                    maybe_my_leader_slot
                );
                Self::initiate_alpenglow_migration(poh_recorder, is_alpenglow_migration_complete);
            }
        }

        if *is_alpenglow_migration_complete {
            // Alpenglow voting loop will handle leader blocks from now on
            return false;
        }

        trace!("{} reached_leader_slot", my_pubkey);

        // TODO(ashwin): remove alpenglow stuff below
        let Some(parent_bank) = bank_forks.read().unwrap().get(parent_slot) else {
            if parent_slot >= first_alpenglow_slot.unwrap_or(u64::MAX) {
                warn!(
                    "We have a certificate for {parent_slot} that is not in bank_forks, we are running behind!"
                );
            } else {
                warn!(
                    "Poh recorder parent slot {parent_slot} is missing from bank_forks. This \
                    indicates that we are in the middle of a dump and repair. Unable to start leader"
                );
            }
            return false;
        };

        if parent_slot < *first_alpenglow_slot.as_ref().unwrap_or(&u64::MAX) {
            assert!(parent_bank.is_frozen());
        }

        // In Alpenglow we can potentially get a notarization certificate for a slot
        // we haven't yet replayed
        if !parent_bank.is_frozen() {
            return false;
        }

        if !Self::common_maybe_start_leader_checks(
            my_pubkey,
            leader_schedule_cache,
            &parent_bank,
            bank_forks,
            maybe_my_leader_slot,
            has_new_vote_been_rooted,
        ) {
            return false;
        }

        let my_leader_slot = maybe_my_leader_slot;
        if my_leader_slot < *first_alpenglow_slot.as_ref().unwrap_or(&u64::MAX) {
            // Alpenglow is not enabled yet for my leader slot, do the regular checks
            if !Self::poh_maybe_start_leader(
                my_pubkey,
                my_leader_slot,
                &parent_bank,
                poh_recorder,
                progress_map,
                skipped_slots_info,
                bank_forks,
                retransmit_slots_sender,
                slot_status_notifier,
                rpc_subscriptions,
                track_transaction_indexes,
                banking_tracer,
            ) {
                return false;
            }
        } else {
            return false;
        }

        // Starting my leader slot was a success
        datapoint_info!(
            "replay_stage-new_leader",
            ("slot", my_leader_slot, i64),
            ("leader", my_pubkey.to_string(), String),
        );
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_blockstore_into_bank(
        bank: &BankWithScheduler,
        blockstore: &Blockstore,
        replay_tx_thread_pool: &ThreadPool,
        replay_stats: &RwLock<ReplaySlotStats>,
        replay_progress: &RwLock<ConfirmationProgress>,
        transaction_status_sender: Option<&TransactionStatusSender>,
        entry_notification_sender: Option<&EntryNotifierSender>,
        replay_vote_sender: &ReplayVoteSender,
        verify_recyclers: &VerifyRecyclers,
        log_messages_bytes_limit: Option<usize>,
        prioritization_fee_cache: &PrioritizationFeeCache,
    ) -> result::Result<usize, BlockstoreProcessorError> {
        let mut w_replay_stats = replay_stats.write().unwrap();
        let mut w_replay_progress = replay_progress.write().unwrap();
        let tx_count_before = w_replay_progress.num_txs;
        // All errors must lead to marking the slot as dead, otherwise,
        // the `check_slot_agrees_with_cluster()` called by `replay_active_banks()`
        // will break!
        blockstore_processor::confirm_slot(
            blockstore,
            bank,
            replay_tx_thread_pool,
            &mut w_replay_stats,
            &mut w_replay_progress,
            false,
            transaction_status_sender,
            entry_notification_sender,
            Some(replay_vote_sender),
            verify_recyclers,
            false,
            log_messages_bytes_limit,
            prioritization_fee_cache,
        )?;
        let tx_count_after = w_replay_progress.num_txs;
        let tx_count = tx_count_after - tx_count_before;
        Ok(tx_count)
    }

    #[allow(clippy::too_many_arguments)]
    fn mark_dead_slot(
        blockstore: &Blockstore,
        bank: &Bank,
        root: Slot,
        err: &BlockstoreProcessorError,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        progress: &mut ProgressMap,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
        tbft_structs: &mut Option<&mut TowerBFTStructures>,
    ) {
        // Do not remove from progress map when marking dead! Needed by
        // `process_duplicate_confirmed_slots()`

        // Block producer can abandon the block if it detects a better one
        // while producing. Somewhat common and expected in a
        // network with variable network/machine configuration.
        let is_serious = !matches!(
            err,
            BlockstoreProcessorError::InvalidBlock(BlockError::TooFewTicks)
        );
        let slot = bank.slot();
        if is_serious {
            datapoint_error!(
                "replay-stage-mark_dead_slot",
                ("error", format!("error: {err:?}"), String),
                ("slot", slot, i64)
            );
        } else {
            datapoint_info!(
                "replay-stage-mark_dead_slot",
                ("error", format!("error: {err:?}"), String),
                ("slot", slot, i64)
            );
        }
        progress.get_mut(&slot).unwrap().is_dead = true;
        blockstore
            .set_dead_slot(slot)
            .expect("Failed to mark slot as dead in blockstore");

        blockstore.slots_stats.mark_dead(slot);

        let err = format!("error: {err:?}");

        if let Some(slot_status_notifier) = slot_status_notifier {
            slot_status_notifier
                .read()
                .unwrap()
                .notify_slot_dead(slot, err.clone());
        }

        rpc_subscriptions.notify_slot_update(SlotUpdate::Dead {
            slot,
            err,
            timestamp: timestamp(),
        });

        if let Some(TowerBFTStructures {
            heaviest_subtree_fork_choice,
            duplicate_slots_tracker,
            duplicate_confirmed_slots,
            epoch_slots_frozen_slots,
            ..
        }) = tbft_structs
        {
            let dead_state = DeadState::new_from_state(
                slot,
                duplicate_slots_tracker,
                duplicate_confirmed_slots,
                heaviest_subtree_fork_choice,
                epoch_slots_frozen_slots,
            );
            check_slot_agrees_with_cluster(
                slot,
                root,
                blockstore,
                duplicate_slots_tracker,
                epoch_slots_frozen_slots,
                heaviest_subtree_fork_choice,
                duplicate_slots_to_repair,
                ancestor_hashes_replay_update_sender,
                purge_repair_slot_counter,
                SlotStateUpdate::Dead(dead_state),
            );

            // If we previously marked this slot as duplicate in blockstore, let the state machine know
            if !duplicate_slots_tracker.contains(&slot)
                && blockstore.get_duplicate_slot(slot).is_some()
            {
                let duplicate_state = DuplicateState::new_from_state(
                    slot,
                    duplicate_confirmed_slots,
                    heaviest_subtree_fork_choice,
                    || true,
                    || None,
                );
                check_slot_agrees_with_cluster(
                    slot,
                    root,
                    blockstore,
                    duplicate_slots_tracker,
                    epoch_slots_frozen_slots,
                    heaviest_subtree_fork_choice,
                    duplicate_slots_to_repair,
                    ancestor_hashes_replay_update_sender,
                    purge_repair_slot_counter,
                    SlotStateUpdate::Duplicate(duplicate_state),
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_votable_bank(
        bank: &Arc<Bank>,
        switch_fork_decision: &SwitchForkDecision,
        bank_forks: &Arc<RwLock<BankForks>>,
        tower: &mut Tower,
        progress: &mut ProgressMap,
        vote_account_pubkey: &Pubkey,
        identity_keypair: &Keypair,
        authorized_voter_keypairs: &[Arc<Keypair>],
        blockstore: &Blockstore,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        lockouts_sender: &Sender<TowerCommitmentAggregationData>,
        accounts_background_request_sender: &AbsRequestSender,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        block_commitment_cache: &Arc<RwLock<BlockCommitmentCache>>,
        bank_notification_sender: &Option<BankNotificationSenderConfig>,
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: &mut bool,
        replay_timing: &mut ReplayLoopTiming,
        voting_sender: &Sender<VoteOp>,
        drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
        wait_to_vote_slot: Option<Slot>,
        first_alpenglow_slot: &mut Option<Slot>,
        tbft_structs: &mut TowerBFTStructures,
    ) -> Result<(), SetRootError> {
        if bank.is_empty() {
            datapoint_info!("replay_stage-voted_empty_bank", ("slot", bank.slot(), i64));
        }
        trace!("handle votable bank {}", bank.slot());
        let new_root = tower.record_bank_vote(bank);

        if let Some(new_root) = new_root {
            if first_alpenglow_slot.is_none() {
                *first_alpenglow_slot = bank_forks
                    .read()
                    .unwrap()
                    .root_bank()
                    .feature_set
                    .activated_slot(&solana_feature_set::secp256k1_program_enabled::id());
                if let Some(first_alpenglow_slot) = first_alpenglow_slot {
                    info!(
                        "alpenglow feature detected in root bank {}, to be enabled on slot {}",
                        new_root, first_alpenglow_slot
                    );
                }
            }
            let highest_super_majority_root = Some(
                block_commitment_cache
                    .read()
                    .unwrap()
                    .highest_super_majority_root(),
            );
            Self::check_and_handle_new_root(
                &identity_keypair.pubkey(),
                bank.parent_slot(),
                new_root,
                bank_forks,
                progress,
                blockstore,
                leader_schedule_cache,
                accounts_background_request_sender,
                rpc_subscriptions,
                highest_super_majority_root,
                bank_notification_sender,
                has_new_vote_been_rooted,
                vote_signatures,
                drop_bank_sender,
                tbft_structs,
            )?;
        }

        let mut update_commitment_cache_time = Measure::start("update_commitment_cache");
        // Send (voted) bank along with the updated vote account state for this node, the vote
        // state is always newer than the one in the bank by definition, because banks can't
        // contain vote transactions which are voting on its own slot.
        //
        // It should be acceptable to aggressively use the vote for our own _local view_ of
        // commitment aggregation, although it's not guaranteed that the new vote transaction is
        // observed by other nodes at this point.
        //
        // The justification stems from the assumption of the sensible voting behavior from the
        // consensus subsystem. That's because it means there would be a slashing possibility
        // otherwise.
        //
        // This behavior isn't significant normally for mainnet-beta, because staked nodes aren't
        // servicing RPC requests. However, this eliminates artificial 1-slot delay of the
        // `finalized` confirmation if a node is materially staked and servicing RPC requests at
        // the same time for development purposes.
        let node_vote_state = (*vote_account_pubkey, tower.vote_state.clone());
        Self::update_commitment_cache(
            bank.clone(),
            bank_forks.read().unwrap().root(),
            progress.get_fork_stats(bank.slot()).unwrap().total_stake,
            node_vote_state,
            lockouts_sender,
        );
        update_commitment_cache_time.stop();
        replay_timing.update_commitment_cache_us += update_commitment_cache_time.as_us();

        Self::push_vote(
            bank,
            vote_account_pubkey,
            identity_keypair,
            authorized_voter_keypairs,
            tower,
            switch_fork_decision,
            vote_signatures,
            *has_new_vote_been_rooted,
            replay_timing,
            voting_sender,
            wait_to_vote_slot,
        );
        Ok(())
    }

    fn generate_vote_tx(
        node_keypair: &Keypair,
        bank: &Bank,
        vote_account_pubkey: &Pubkey,
        authorized_voter_keypairs: &[Arc<Keypair>],
        vote: VoteTransaction,
        switch_fork_decision: &SwitchForkDecision,
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
        wait_to_vote_slot: Option<Slot>,
    ) -> GenerateVoteTxResult {
        if !bank.is_startup_verification_complete() {
            info!("startup verification incomplete, so unable to vote");
            return GenerateVoteTxResult::Failed;
        }

        if authorized_voter_keypairs.is_empty() {
            return GenerateVoteTxResult::NonVoting;
        }
        if let Some(slot) = wait_to_vote_slot {
            if bank.slot() < slot {
                return GenerateVoteTxResult::Failed;
            }
        }
        let vote_account = match bank.get_vote_account(vote_account_pubkey) {
            None => {
                warn!(
                    "Vote account {} does not exist.  Unable to vote",
                    vote_account_pubkey,
                );
                return GenerateVoteTxResult::Failed;
            }
            Some(vote_account) => vote_account,
        };
        let vote_state_view = match vote_account.vote_state_view() {
            None => {
                warn!(
                    "Vote account {} does not have a vote state.  Unable to vote",
                    vote_account_pubkey,
                );
                return GenerateVoteTxResult::Failed;
            }
            Some(vote_state_view) => vote_state_view,
        };
        if vote_state_view.node_pubkey() != &node_keypair.pubkey() {
            info!(
                "Vote account node_pubkey mismatch: {} (expected: {}).  Unable to vote",
                vote_state_view.node_pubkey(),
                node_keypair.pubkey()
            );
            return GenerateVoteTxResult::HotSpare;
        }

        let Some(authorized_voter_pubkey) = vote_state_view.get_authorized_voter(bank.epoch())
        else {
            warn!(
                "Vote account {} has no authorized voter for epoch {}.  Unable to vote",
                vote_account_pubkey,
                bank.epoch()
            );
            return GenerateVoteTxResult::Failed;
        };

        let authorized_voter_keypair = match authorized_voter_keypairs
            .iter()
            .find(|keypair| &keypair.pubkey() == authorized_voter_pubkey)
        {
            None => {
                warn!(
                    "The authorized keypair {authorized_voter_pubkey} for vote account \
                     {vote_account_pubkey} is not available.  Unable to vote"
                );
                return GenerateVoteTxResult::NonVoting;
            }
            Some(authorized_voter_keypair) => authorized_voter_keypair,
        };

        // Send our last few votes along with the new one
        // Compact the vote state update before sending
        let vote = match vote {
            VoteTransaction::VoteStateUpdate(vote_state_update) => {
                VoteTransaction::CompactVoteStateUpdate(vote_state_update)
            }
            vote => vote,
        };
        let vote_ix = switch_fork_decision
            .to_vote_instruction(
                vote,
                vote_account_pubkey,
                &authorized_voter_keypair.pubkey(),
            )
            .expect("Switch failure should not lead to voting");

        let mut vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));

        let blockhash = bank.last_blockhash();
        vote_tx.partial_sign(&[node_keypair], blockhash);
        vote_tx.partial_sign(&[authorized_voter_keypair.as_ref()], blockhash);

        if !has_new_vote_been_rooted {
            vote_signatures.push(vote_tx.signatures[0]);
            if vote_signatures.len() > MAX_VOTE_SIGNATURES {
                vote_signatures.remove(0);
            }
        } else {
            vote_signatures.clear();
        }

        GenerateVoteTxResult::Tx(vote_tx)
    }

    /// Potentially refresh the last vote if:
    /// - We are not a hotspare or non-voting validator and we have previously attempted to vote at least once
    /// - There is a `heaviest_bank_on_same_fork` on the previously voted fork
    /// - We have previously landed a vote on this fork for a slot `latest_landed_vote_slot`
    /// - Our latest vote attempt for `last_vote_slot` has not been cleared from the progress map
    /// - `latest_landed_vote_slot` < `last_vote_slot`
    /// - The difference in block height of `heaviest_bank_on_same_fork` and `last_vote_slot`
    ///   is at least `REFRESH_VOTE_BLOCKHEIGHT` as indicated by the blockhash queue
    /// - It has been at least `MAX_VOTE_REFRESH_INTERVAL_MILLIS` ms since our last refresh
    ///
    /// If the conditions are met, we update the timestamp and blockhash of our original vote
    /// for `last_vote_slot` and resend it to the cluster
    ///
    /// Returns true if the last vote was refreshed
    #[allow(clippy::too_many_arguments)]
    fn maybe_refresh_last_vote(
        tower: &mut Tower,
        progress: &ProgressMap,
        heaviest_bank_on_same_fork: Option<Arc<Bank>>,
        vote_account_pubkey: &Pubkey,
        identity_keypair: &Keypair,
        authorized_voter_keypairs: &[Arc<Keypair>],
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
        last_vote_refresh_time: &mut LastVoteRefreshTime,
        voting_sender: &Sender<VoteOp>,
        wait_to_vote_slot: Option<Slot>,
    ) -> bool {
        let Some(heaviest_bank_on_same_fork) = heaviest_bank_on_same_fork.as_ref() else {
            // Only refresh if blocks have been built on our last vote
            return false;
        };
        let Some(latest_landed_vote_slot) =
            progress.my_latest_landed_vote(heaviest_bank_on_same_fork.slot())
        else {
            // Need to land at least one vote in order to refresh
            return false;
        };
        let Some(last_voted_slot) = tower.last_voted_slot() else {
            // Need to have voted in order to refresh
            return false;
        };

        // If our last landed vote on this fork is greater than the vote recorded in our tower
        // this means that our tower is old AND on chain adoption has failed. Warn the operator
        // as they could be submitting slashable votes.
        if latest_landed_vote_slot > last_voted_slot
            && last_vote_refresh_time.last_print_time.elapsed().as_secs() >= 1
        {
            last_vote_refresh_time.last_print_time = Instant::now();
            warn!(
                "Last landed vote for slot {} in bank {} is greater than the current last vote \
                 for slot: {} tracked by tower. This indicates a bug in the on chain adoption logic",
                latest_landed_vote_slot,
                heaviest_bank_on_same_fork.slot(),
                last_voted_slot
            );
            datapoint_error!(
                "adoption_failure",
                ("latest_landed_vote_slot", latest_landed_vote_slot, i64),
                (
                    "heaviest_bank_on_fork",
                    heaviest_bank_on_same_fork.slot(),
                    i64
                ),
                ("last_voted_slot", last_voted_slot, i64)
            );
        }

        if latest_landed_vote_slot >= last_voted_slot {
            // Our vote or a subsequent vote landed do not refresh
            return false;
        }

        // If we are a non voting validator or have an incorrect setup preventing us from
        // generating vote txs, no need to refresh
        let last_vote_tx_blockhash = match tower.last_vote_tx_blockhash() {
            // Since the checks in vote generation are deterministic, if we were non voting or hot spare
            // on the original vote, the refresh will also fail. No reason to refresh.
            // On the fly adjustments via the cli will be picked up for the next vote.
            BlockhashStatus::NonVoting | BlockhashStatus::HotSpare => return false,
            // In this case we have not voted since restart, our setup is unclear.
            // We have a vote from our previous restart that is eligble for refresh, we must refresh.
            BlockhashStatus::Uninitialized => None,
            BlockhashStatus::Blockhash(blockhash) => Some(blockhash),
        };

        if last_vote_tx_blockhash.is_some()
            && heaviest_bank_on_same_fork
                .is_hash_valid_for_age(&last_vote_tx_blockhash.unwrap(), REFRESH_VOTE_BLOCKHEIGHT)
        {
            // Check the blockhash queue to see if enough blocks have been built on our last voted fork
            return false;
        }

        if last_vote_refresh_time
            .last_refresh_time
            .elapsed()
            .as_millis()
            < MAX_VOTE_REFRESH_INTERVAL_MILLIS as u128
        {
            // This avoids duplicate refresh in case there are multiple forks descending from our last voted fork
            // It also ensures that if the first refresh fails we will continue attempting to refresh at an interval no less
            // than MAX_VOTE_REFRESH_INTERVAL_MILLIS
            return false;
        }

        // All criteria are met, refresh the last vote using the blockhash of `heaviest_bank_on_same_fork`
        Self::refresh_last_vote(
            tower,
            heaviest_bank_on_same_fork,
            last_voted_slot,
            vote_account_pubkey,
            identity_keypair,
            authorized_voter_keypairs,
            vote_signatures,
            has_new_vote_been_rooted,
            last_vote_refresh_time,
            voting_sender,
            wait_to_vote_slot,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn refresh_last_vote(
        tower: &mut Tower,
        heaviest_bank_on_same_fork: &Bank,
        last_voted_slot: Slot,
        vote_account_pubkey: &Pubkey,
        identity_keypair: &Keypair,
        authorized_voter_keypairs: &[Arc<Keypair>],
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
        last_vote_refresh_time: &mut LastVoteRefreshTime,
        voting_sender: &Sender<VoteOp>,
        wait_to_vote_slot: Option<Slot>,
    ) -> bool {
        // Update timestamp for refreshed vote
        tower.refresh_last_vote_timestamp(heaviest_bank_on_same_fork.slot());

        let vote_tx_result = Self::generate_vote_tx(
            identity_keypair,
            heaviest_bank_on_same_fork,
            vote_account_pubkey,
            authorized_voter_keypairs,
            tower.last_vote(),
            &SwitchForkDecision::SameFork,
            vote_signatures,
            has_new_vote_been_rooted,
            wait_to_vote_slot,
        );

        if let GenerateVoteTxResult::Tx(vote_tx) = vote_tx_result {
            let recent_blockhash = vote_tx.message.recent_blockhash;
            tower.refresh_last_vote_tx_blockhash(recent_blockhash);

            // Send the votes to the TPU and gossip for network propagation
            let hash_string = format!("{recent_blockhash}");
            datapoint_info!(
                "refresh_vote",
                ("last_voted_slot", last_voted_slot, i64),
                ("target_bank_slot", heaviest_bank_on_same_fork.slot(), i64),
                ("target_bank_hash", hash_string, String),
            );
            voting_sender
                .send(VoteOp::RefreshVote {
                    tx: vote_tx,
                    last_voted_slot,
                })
                .unwrap_or_else(|err| warn!("Error: {:?}", err));
            last_vote_refresh_time.last_refresh_time = Instant::now();
            true
        } else if vote_tx_result.is_non_voting() {
            tower.mark_last_vote_tx_blockhash_non_voting();
            false
        } else if vote_tx_result.is_hot_spare() {
            tower.mark_last_vote_tx_blockhash_hot_spare();
            false
        } else {
            false
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_vote(
        bank: &Bank,
        vote_account_pubkey: &Pubkey,
        identity_keypair: &Keypair,
        authorized_voter_keypairs: &[Arc<Keypair>],
        tower: &mut Tower,
        switch_fork_decision: &SwitchForkDecision,
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
        replay_timing: &mut ReplayLoopTiming,
        voting_sender: &Sender<VoteOp>,
        wait_to_vote_slot: Option<Slot>,
    ) {
        let mut generate_time = Measure::start("generate_vote");
        let vote_tx_result = Self::generate_vote_tx(
            identity_keypair,
            bank,
            vote_account_pubkey,
            authorized_voter_keypairs,
            tower.last_vote(),
            switch_fork_decision,
            vote_signatures,
            has_new_vote_been_rooted,
            wait_to_vote_slot,
        );
        generate_time.stop();
        replay_timing.generate_vote_us += generate_time.as_us();
        if let GenerateVoteTxResult::Tx(vote_tx) = vote_tx_result {
            tower.refresh_last_vote_tx_blockhash(vote_tx.message.recent_blockhash);

            let saved_tower = SavedTower::new(tower, identity_keypair).unwrap_or_else(|err| {
                error!("Unable to create saved tower: {:?}", err);
                std::process::exit(1);
            });

            let tower_slots = tower.tower_slots();
            voting_sender
                .send(VoteOp::PushVote {
                    tx: vote_tx,
                    tower_slots,
                    saved_tower: SavedTowerVersions::from(saved_tower),
                })
                .unwrap_or_else(|err| warn!("Error: {:?}", err));
        } else if vote_tx_result.is_non_voting() {
            tower.mark_last_vote_tx_blockhash_non_voting();
        }
    }

    fn update_commitment_cache(
        bank: Arc<Bank>,
        root: Slot,
        total_stake: Stake,
        node_vote_state: (Pubkey, TowerVoteState),
        lockouts_sender: &Sender<TowerCommitmentAggregationData>,
    ) {
        if let Err(e) = lockouts_sender.send(TowerCommitmentAggregationData::new(
            bank,
            root,
            total_stake,
            node_vote_state,
        )) {
            trace!("lockouts_sender failed: {:?}", e);
        }
    }

    fn reset_poh_recorder(
        my_pubkey: &Pubkey,
        blockstore: &Blockstore,
        bank: Arc<Bank>,
        poh_recorder: &RwLock<PohRecorder>,
        leader_schedule_cache: &LeaderScheduleCache,
    ) {
        let slot = bank.slot();
        let tick_height = bank.tick_height();

        let next_leader_slot = leader_schedule_cache.next_leader_slot(
            my_pubkey,
            slot,
            &bank,
            Some(blockstore),
            GRACE_TICKS_FACTOR * MAX_GRACE_SLOTS,
        );

        poh_recorder.write().unwrap().reset(bank, next_leader_slot);

        let next_leader_msg = if let Some(next_leader_slot) = next_leader_slot {
            format!("My next leader slot is {}", next_leader_slot.0)
        } else {
            "I am not in the leader schedule yet".to_owned()
        };
        info!(
            "{my_pubkey} reset PoH to tick {tick_height} (within slot {slot}). {next_leader_msg}",
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_active_banks_concurrently(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        fork_thread_pool: &ThreadPool,
        replay_tx_thread_pool: &ThreadPool,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
        progress: &mut ProgressMap,
        transaction_status_sender: Option<&TransactionStatusSender>,
        entry_notification_sender: Option<&EntryNotifierSender>,
        verify_recyclers: &VerifyRecyclers,
        replay_vote_sender: &ReplayVoteSender,
        replay_timing: &mut ReplayLoopTiming,
        log_messages_bytes_limit: Option<usize>,
        active_bank_slots: &[Slot],
        prioritization_fee_cache: &PrioritizationFeeCache,
    ) -> Vec<ReplaySlotFromBlockstore> {
        // Make mutable shared structures thread safe.
        let progress = RwLock::new(progress);
        let longest_replay_time_us = AtomicU64::new(0);

        // Allow for concurrent replaying of slots from different forks.
        let replay_result_vec: Vec<ReplaySlotFromBlockstore> = fork_thread_pool.install(|| {
            active_bank_slots
                .into_par_iter()
                .map(|bank_slot| {
                    let bank_slot = *bank_slot;
                    let mut replay_result = ReplaySlotFromBlockstore {
                        is_slot_dead: false,
                        bank_slot,
                        replay_result: None,
                    };
                    let my_pubkey = &my_pubkey.clone();
                    trace!(
                        "Replay active bank: slot {}, thread_idx {}",
                        bank_slot,
                        fork_thread_pool.current_thread_index().unwrap_or_default()
                    );
                    let mut progress_lock = progress.write().unwrap();
                    if progress_lock
                        .get(&bank_slot)
                        .map(|p| p.is_dead)
                        .unwrap_or(false)
                    {
                        // If the fork was marked as dead, don't replay it
                        debug!("bank_slot {:?} is marked dead", bank_slot);
                        replay_result.is_slot_dead = true;
                        return replay_result;
                    }

                    let bank = bank_forks
                        .read()
                        .unwrap()
                        .get_with_scheduler(bank_slot)
                        .unwrap();
                    let parent_slot = bank.parent_slot();
                    let (num_blocks_on_fork, num_dropped_blocks_on_fork) = {
                        let stats = progress_lock
                            .get(&parent_slot)
                            .expect("parent of active bank must exist in progress map");
                        let num_blocks_on_fork = stats.num_blocks_on_fork + 1;
                        let new_dropped_blocks = bank.slot() - parent_slot - 1;
                        let num_dropped_blocks_on_fork =
                            stats.num_dropped_blocks_on_fork + new_dropped_blocks;
                        (num_blocks_on_fork, num_dropped_blocks_on_fork)
                    };
                    let prev_leader_slot = progress_lock.get_bank_prev_leader_slot(&bank);

                    let bank_progress = progress_lock.entry(bank.slot()).or_insert_with(|| {
                        ForkProgress::new_from_bank(
                            &bank,
                            my_pubkey,
                            &vote_account.clone(),
                            prev_leader_slot,
                            num_blocks_on_fork,
                            num_dropped_blocks_on_fork,
                        )
                    });

                    let replay_stats = bank_progress.replay_stats.clone();
                    let replay_progress = bank_progress.replay_progress.clone();
                    drop(progress_lock);

                    if bank.collector_id() != my_pubkey {
                        let mut replay_blockstore_time =
                            Measure::start("replay_blockstore_into_bank");
                        let blockstore_result = Self::replay_blockstore_into_bank(
                            &bank,
                            blockstore,
                            replay_tx_thread_pool,
                            &replay_stats,
                            &replay_progress,
                            transaction_status_sender,
                            entry_notification_sender,
                            &replay_vote_sender.clone(),
                            &verify_recyclers.clone(),
                            log_messages_bytes_limit,
                            prioritization_fee_cache,
                        );
                        replay_blockstore_time.stop();
                        replay_result.replay_result = Some(blockstore_result);
                        longest_replay_time_us
                            .fetch_max(replay_blockstore_time.as_us(), Ordering::Relaxed);
                    }
                    replay_result
                })
                .collect()
        });
        // Accumulating time across all slots could inflate this number and make it seem like an
        // overly large amount of time is being spent on blockstore compared to other activities.
        replay_timing.replay_blockstore_us += longest_replay_time_us.load(Ordering::Relaxed);

        replay_result_vec
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_active_bank(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        replay_tx_thread_pool: &ThreadPool,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
        progress: &mut ProgressMap,
        transaction_status_sender: Option<&TransactionStatusSender>,
        entry_notification_sender: Option<&EntryNotifierSender>,
        verify_recyclers: &VerifyRecyclers,
        replay_vote_sender: &ReplayVoteSender,
        replay_timing: &mut ReplayLoopTiming,
        log_messages_bytes_limit: Option<usize>,
        bank_slot: Slot,
        prioritization_fee_cache: &PrioritizationFeeCache,
    ) -> ReplaySlotFromBlockstore {
        let mut replay_result = ReplaySlotFromBlockstore {
            is_slot_dead: false,
            bank_slot,
            replay_result: None,
        };
        let my_pubkey = &my_pubkey.clone();
        trace!("Replay active bank: slot {}", bank_slot);
        if progress.get(&bank_slot).map(|p| p.is_dead).unwrap_or(false) {
            // If the fork was marked as dead, don't replay it
            debug!("bank_slot {:?} is marked dead", bank_slot);
            replay_result.is_slot_dead = true;
        } else {
            let Some(bank) = bank_forks.read().unwrap().get_with_scheduler(bank_slot) else {
                info!("Abandoning replay of unrooted slot {bank_slot}");
                return replay_result;
            };
            let parent_slot = bank.parent_slot();
            let prev_leader_slot = progress.get_bank_prev_leader_slot(&bank);
            let (num_blocks_on_fork, num_dropped_blocks_on_fork) = {
                let Some(stats) = progress.get(&parent_slot) else {
                    info!("Abandoning replay of unrooted slot {bank_slot}");
                    return replay_result;
                };
                let num_blocks_on_fork = stats.num_blocks_on_fork + 1;
                let new_dropped_blocks = bank.slot() - parent_slot - 1;
                let num_dropped_blocks_on_fork =
                    stats.num_dropped_blocks_on_fork + new_dropped_blocks;
                (num_blocks_on_fork, num_dropped_blocks_on_fork)
            };

            let bank_progress = progress.entry(bank.slot()).or_insert_with(|| {
                ForkProgress::new_from_bank(
                    &bank,
                    my_pubkey,
                    &vote_account.clone(),
                    prev_leader_slot,
                    num_blocks_on_fork,
                    num_dropped_blocks_on_fork,
                )
            });

            if bank.collector_id() != my_pubkey {
                let mut replay_blockstore_time = Measure::start("replay_blockstore_into_bank");
                let blockstore_result = Self::replay_blockstore_into_bank(
                    &bank,
                    blockstore,
                    replay_tx_thread_pool,
                    &bank_progress.replay_stats,
                    &bank_progress.replay_progress,
                    transaction_status_sender,
                    entry_notification_sender,
                    &replay_vote_sender.clone(),
                    &verify_recyclers.clone(),
                    log_messages_bytes_limit,
                    prioritization_fee_cache,
                );
                replay_blockstore_time.stop();
                replay_result.replay_result = Some(blockstore_result);
                replay_timing.replay_blockstore_us += replay_blockstore_time.as_us();
            }
        }
        replay_result
    }

    fn initiate_alpenglow_migration(
        poh_recorder: &RwLock<PohRecorder>,
        is_alpenglow_migration_complete: &mut bool,
    ) {
        info!("initiating alpenglow migration from replay");
        poh_recorder.write().unwrap().is_alpenglow_enabled = true;
        while !poh_recorder.read().unwrap().use_alpenglow_tick_producer {
            // Wait for PohService to migrate to alpenglow tick producer
            thread::sleep(Duration::from_millis(10));
        }
        *is_alpenglow_migration_complete = true;
        info!("alpenglow migration complete!");
    }

    #[allow(clippy::too_many_arguments)]
    fn process_replay_results(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        transaction_status_sender: Option<&TransactionStatusSender>,
        block_meta_sender: Option<&BlockMetaSender>,
        bank_notification_sender: &Option<BankNotificationSenderConfig>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
        cluster_slots_update_sender: &ClusterSlotsUpdateSender,
        cost_update_sender: &Sender<CostUpdate>,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        block_metadata_notifier: Option<BlockMetadataNotifierArc>,
        replay_result_vec: &[ReplaySlotFromBlockstore],
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
        my_pubkey: &Pubkey,
        first_alpenglow_slot: Option<Slot>,
        poh_recorder: &RwLock<PohRecorder>,
        is_alpenglow_migration_complete: &mut bool,
        mut tbft_structs: Option<&mut TowerBFTStructures>,
        votor_event_sender: &VotorEventSender,
    ) -> Vec<Slot> {
        // TODO: See if processing of blockstore replay results and bank completion can be made thread safe.
        let mut tx_count = 0;
        let mut execute_timings = ExecuteTimings::default();
        let mut new_frozen_slots = vec![];
        for replay_result in replay_result_vec {
            if replay_result.is_slot_dead {
                continue;
            }

            let bank_slot = replay_result.bank_slot;
            let Some(bank) = &bank_forks.read().unwrap().get_with_scheduler(bank_slot) else {
                info!("Abandoning replay of unrooted slot {bank_slot}");
                continue;
            };
            if let Some(replay_result) = &replay_result.replay_result {
                match replay_result {
                    Ok(replay_tx_count) => tx_count += replay_tx_count,
                    Err(err) => {
                        let root = bank_forks.read().unwrap().root();
                        Self::mark_dead_slot(
                            blockstore,
                            bank,
                            root,
                            err,
                            rpc_subscriptions,
                            slot_status_notifier,
                            progress,
                            duplicate_slots_to_repair,
                            ancestor_hashes_replay_update_sender,
                            purge_repair_slot_counter,
                            &mut tbft_structs,
                        );
                        // don't try to run the below logic to check if the bank is completed
                        continue;
                    }
                }
            }

            assert_eq!(bank_slot, bank.slot());
            if bank.is_complete() {
                if let Some(first_alpenglow_slot) = first_alpenglow_slot {
                    if !*is_alpenglow_migration_complete && bank.slot() >= first_alpenglow_slot {
                        info!(
                            "initiating alpenglow migration from replaying bank {}",
                            bank.slot()
                        );
                        Self::initiate_alpenglow_migration(
                            poh_recorder,
                            is_alpenglow_migration_complete,
                        );
                    }
                }
                let mut bank_complete_time = Measure::start("bank_complete_time");
                let bank_progress = progress
                    .get_mut(&bank.slot())
                    .expect("Bank fork progress entry missing for completed bank");

                let replay_stats = bank_progress.replay_stats.clone();
                let mut is_unified_scheduler_enabled = false;

                if let Some((result, completed_execute_timings)) =
                    bank.wait_for_completed_scheduler()
                {
                    // It's guaranteed that wait_for_completed_scheduler() returns Some(_), iff the
                    // unified scheduler is enabled for the bank.
                    is_unified_scheduler_enabled = true;
                    let metrics = ExecuteBatchesInternalMetrics::new_with_timings_from_all_threads(
                        completed_execute_timings,
                    );
                    replay_stats
                        .write()
                        .unwrap()
                        .batch_execute
                        .accumulate(metrics, is_unified_scheduler_enabled);

                    if let Err(err) = result {
                        let root = bank_forks.read().unwrap().root();
                        Self::mark_dead_slot(
                            blockstore,
                            bank,
                            root,
                            &BlockstoreProcessorError::InvalidTransaction(err),
                            rpc_subscriptions,
                            slot_status_notifier,
                            progress,
                            duplicate_slots_to_repair,
                            ancestor_hashes_replay_update_sender,
                            purge_repair_slot_counter,
                            &mut tbft_structs,
                        );
                        // don't try to run the remaining normal processing for the completed bank
                        continue;
                    }
                }

                // If the block does not have at least DATA_SHREDS_PER_FEC_BLOCK correctly retransmitted
                // shreds in the last FEC set, mark it dead.
                let block_id = match blockstore.check_last_fec_set_and_get_block_id(
                    bank.slot(),
                    bank.hash(),
                    false,
                    &bank.feature_set,
                ) {
                    Ok(block_id) => block_id,
                    Err(result_err) => {
                        if bank.collector_id() == my_pubkey {
                            // Our leader block has not finished shredding
                            None
                        } else {
                            let root = bank_forks.read().unwrap().root();
                            Self::mark_dead_slot(
                                blockstore,
                                bank,
                                root,
                                &result_err,
                                rpc_subscriptions,
                                slot_status_notifier,
                                progress,
                                duplicate_slots_to_repair,
                                ancestor_hashes_replay_update_sender,
                                purge_repair_slot_counter,
                                &mut tbft_structs,
                            );
                            continue;
                        }
                    }
                };

                if bank.block_id().is_none() {
                    bank.set_block_id(block_id);
                }

                let r_replay_stats = replay_stats.read().unwrap();
                let replay_progress = bank_progress.replay_progress.clone();
                let r_replay_progress = replay_progress.read().unwrap();
                debug!(
                    "bank {} has completed replay from blockstore, contribute to update cost with \
                     {:?}",
                    bank.slot(),
                    r_replay_stats.batch_execute.totals
                );
                new_frozen_slots.push(bank.slot());
                let _ = cluster_slots_update_sender.send(vec![bank_slot]);
                if let Some(transaction_status_sender) = transaction_status_sender {
                    transaction_status_sender.send_transaction_status_freeze_message(bank);
                }
                bank.freeze();
                datapoint_info!(
                    "bank_frozen",
                    ("slot", bank_slot, i64),
                    ("hash", bank.hash().to_string(), String),
                );
                // report cost tracker stats
                cost_update_sender
                    .send(CostUpdate::FrozenBank {
                        bank: bank.clone_without_scheduler(),
                    })
                    .unwrap_or_else(|err| {
                        warn!("cost_update_sender failed sending bank stats: {:?}", err)
                    });

                assert_ne!(bank.hash(), Hash::default());
                // Needs to be updated before `check_slot_agrees_with_cluster()` so that
                // any updates in `check_slot_agrees_with_cluster()` on fork choice take
                // effect

                bank_progress.fork_stats.bank_hash = Some(bank.hash());
                if let Some(TowerBFTStructures {
                    heaviest_subtree_fork_choice,
                    duplicate_slots_tracker,
                    duplicate_confirmed_slots,
                    epoch_slots_frozen_slots,
                    ..
                }) = &mut tbft_structs
                {
                    heaviest_subtree_fork_choice.add_new_leaf_slot(
                        (bank.slot(), bank.hash()),
                        Some((bank.parent_slot(), bank.parent_hash())),
                    );
                    heaviest_subtree_fork_choice.maybe_print_state();
                    let bank_frozen_state = BankFrozenState::new_from_state(
                        bank.slot(),
                        bank.hash(),
                        duplicate_slots_tracker,
                        duplicate_confirmed_slots,
                        heaviest_subtree_fork_choice,
                        epoch_slots_frozen_slots,
                    );
                    check_slot_agrees_with_cluster(
                        bank.slot(),
                        bank_forks.read().unwrap().root(),
                        blockstore,
                        duplicate_slots_tracker,
                        epoch_slots_frozen_slots,
                        heaviest_subtree_fork_choice,
                        duplicate_slots_to_repair,
                        ancestor_hashes_replay_update_sender,
                        purge_repair_slot_counter,
                        SlotStateUpdate::BankFrozen(bank_frozen_state),
                    );
                    // If we previously marked this slot as duplicate in blockstore, let the state machine know
                    if !duplicate_slots_tracker.contains(&bank.slot())
                        && blockstore.get_duplicate_slot(bank.slot()).is_some()
                    {
                        let duplicate_state = DuplicateState::new_from_state(
                            bank.slot(),
                            duplicate_confirmed_slots,
                            heaviest_subtree_fork_choice,
                            || false,
                            || Some(bank.hash()),
                        );
                        check_slot_agrees_with_cluster(
                            bank.slot(),
                            bank_forks.read().unwrap().root(),
                            blockstore,
                            duplicate_slots_tracker,
                            epoch_slots_frozen_slots,
                            heaviest_subtree_fork_choice,
                            duplicate_slots_to_repair,
                            ancestor_hashes_replay_update_sender,
                            purge_repair_slot_counter,
                            SlotStateUpdate::Duplicate(duplicate_state),
                        );
                    }
                }

                // For leader banks:
                // 1) Replay finishes before shredding, broadcast_stage will take care of
                //      notifying votor
                // 2) Shredding finishes before replay, we notify here
                //
                // For non leader banks (2) is always true, so notify here
                if *is_alpenglow_migration_complete && bank.block_id().is_some() {
                    // Leader blocks will not have a block id, broadcast stage will
                    // take care of notifying the voting loop
                    let _ = votor_event_sender.send(VotorEvent::Block(CompletedBlock {
                        slot: bank.slot(),
                        bank: bank.clone_without_scheduler(),
                    }));
                }

                if let Some(sender) = bank_notification_sender {
                    sender
                        .sender
                        .send(BankNotification::Frozen(bank.clone_without_scheduler()))
                        .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));
                }
                blockstore_processor::send_block_meta(bank, block_meta_sender);

                let bank_hash = bank.hash();
                if let Some(new_frozen_voters) = tbft_structs.as_mut().and_then(|tbft| {
                    tbft.unfrozen_gossip_verified_vote_hashes
                        .remove_slot_hash(bank.slot(), &bank_hash)
                }) {
                    for pubkey in new_frozen_voters {
                        latest_validator_votes_for_frozen_banks.check_add_vote(
                            pubkey,
                            bank.slot(),
                            Some(bank_hash),
                            false,
                        );
                    }
                }

                if let Some(ref block_metadata_notifier) = block_metadata_notifier {
                    let parent_blockhash = bank
                        .parent()
                        .map(|bank| bank.last_blockhash())
                        .unwrap_or_default();
                    block_metadata_notifier.notify_block_metadata(
                        bank.parent_slot(),
                        &parent_blockhash.to_string(),
                        bank.slot(),
                        &bank.last_blockhash().to_string(),
                        &bank.get_rewards_and_num_partitions(),
                        Some(bank.clock().unix_timestamp),
                        Some(bank.block_height()),
                        bank.executed_transaction_count(),
                        r_replay_progress.num_entries as u64,
                    )
                }
                bank_complete_time.stop();

                r_replay_stats.report_stats(
                    bank.slot(),
                    r_replay_progress.num_txs,
                    r_replay_progress.num_entries,
                    r_replay_progress.num_shreds,
                    bank_complete_time.as_us(),
                    is_unified_scheduler_enabled,
                );
                execute_timings.accumulate(&r_replay_stats.batch_execute.totals);
            } else {
                trace!(
                    "bank {} not completed tick_height: {}, max_tick_height: {}",
                    bank.slot(),
                    bank.tick_height(),
                    bank.max_tick_height()
                );
            }
        }

        new_frozen_slots
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_active_banks(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
        progress: &mut ProgressMap,
        transaction_status_sender: Option<&TransactionStatusSender>,
        block_meta_sender: Option<&BlockMetaSender>,
        entry_notification_sender: Option<&EntryNotifierSender>,
        verify_recyclers: &VerifyRecyclers,
        replay_vote_sender: &ReplayVoteSender,
        bank_notification_sender: &Option<BankNotificationSenderConfig>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
        cluster_slots_update_sender: &ClusterSlotsUpdateSender,
        cost_update_sender: &Sender<CostUpdate>,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        block_metadata_notifier: Option<BlockMetadataNotifierArc>,
        replay_timing: &mut ReplayLoopTiming,
        log_messages_bytes_limit: Option<usize>,
        replay_mode: &ForkReplayMode,
        replay_tx_thread_pool: &ThreadPool,
        prioritization_fee_cache: &PrioritizationFeeCache,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
        poh_recorder: &RwLock<PohRecorder>,
        first_alpenglow_slot: Option<Slot>,
        tbft_structs: Option<&mut TowerBFTStructures>,
        is_alpenglow_migration_complete: &mut bool,
        votor_event_sender: &VotorEventSender,
    ) -> Vec<Slot> /* completed slots */ {
        let active_bank_slots = bank_forks.read().unwrap().active_bank_slots();
        let num_active_banks = active_bank_slots.len();
        trace!(
            "{} active bank(s) to replay: {:?}",
            num_active_banks,
            active_bank_slots
        );
        if active_bank_slots.is_empty() {
            return vec![];
        }

        let replay_result_vec = match replay_mode {
            // Skip the overhead of the threadpool if there is only one bank to play
            ForkReplayMode::Parallel(fork_thread_pool) if num_active_banks > 1 => {
                Self::replay_active_banks_concurrently(
                    blockstore,
                    bank_forks,
                    fork_thread_pool,
                    replay_tx_thread_pool,
                    my_pubkey,
                    vote_account,
                    progress,
                    transaction_status_sender,
                    entry_notification_sender,
                    verify_recyclers,
                    replay_vote_sender,
                    replay_timing,
                    log_messages_bytes_limit,
                    &active_bank_slots,
                    prioritization_fee_cache,
                )
            }
            ForkReplayMode::Serial | ForkReplayMode::Parallel(_) => active_bank_slots
                .iter()
                .map(|bank_slot| {
                    Self::replay_active_bank(
                        blockstore,
                        bank_forks,
                        replay_tx_thread_pool,
                        my_pubkey,
                        vote_account,
                        progress,
                        transaction_status_sender,
                        entry_notification_sender,
                        verify_recyclers,
                        replay_vote_sender,
                        replay_timing,
                        log_messages_bytes_limit,
                        *bank_slot,
                        prioritization_fee_cache,
                    )
                })
                .collect(),
        };

        Self::process_replay_results(
            blockstore,
            bank_forks,
            progress,
            transaction_status_sender,
            block_meta_sender,
            bank_notification_sender,
            rpc_subscriptions,
            slot_status_notifier,
            latest_validator_votes_for_frozen_banks,
            cluster_slots_update_sender,
            cost_update_sender,
            duplicate_slots_to_repair,
            ancestor_hashes_replay_update_sender,
            block_metadata_notifier,
            &replay_result_vec,
            purge_repair_slot_counter,
            my_pubkey,
            first_alpenglow_slot,
            poh_recorder,
            is_alpenglow_migration_complete,
            tbft_structs,
            votor_event_sender,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compute_bank_stats(
        my_vote_pubkey: &Pubkey,
        ancestors: &HashMap<u64, HashSet<u64>>,
        frozen_banks: &mut [Arc<Bank>],
        tower: &mut Tower,
        progress: &mut ProgressMap,
        vote_tracker: &VoteTracker,
        cluster_slots: &ClusterSlots,
        bank_forks: &RwLock<BankForks>,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    ) -> Vec<Slot> {
        frozen_banks.sort_by_key(|bank| bank.slot());
        let mut new_stats = vec![];
        for bank in frozen_banks.iter() {
            let bank_slot = bank.slot();
            // Only time progress map should be missing a bank slot
            // is if this node was the leader for this slot as those banks
            // are not replayed in replay_active_banks()
            {
                let is_computed = progress
                    .get_fork_stats_mut(bank_slot)
                    .expect("All frozen banks must exist in the Progress map")
                    .computed;
                if !is_computed {
                    // Check if our tower is behind, if so adopt the on chain tower from this Bank
                    Self::adopt_on_chain_tower_if_behind(
                        my_vote_pubkey,
                        ancestors,
                        frozen_banks,
                        tower,
                        progress,
                        bank,
                        bank_forks,
                    );
                    let computed_bank_state = Tower::collect_vote_lockouts(
                        my_vote_pubkey,
                        bank_slot,
                        &bank.vote_accounts(),
                        ancestors,
                        |slot| progress.get_hash(slot),
                        latest_validator_votes_for_frozen_banks,
                    );
                    // Notify any listeners of the votes found in this newly computed
                    // bank
                    heaviest_subtree_fork_choice.compute_bank_stats(
                        bank,
                        tower,
                        latest_validator_votes_for_frozen_banks,
                    );
                    let ComputedBankState {
                        voted_stakes,
                        total_stake,
                        fork_stake,
                        lockout_intervals,
                        my_latest_landed_vote,
                        ..
                    } = computed_bank_state;
                    let stats = progress
                        .get_fork_stats_mut(bank_slot)
                        .expect("All frozen banks must exist in the Progress map");
                    stats.fork_stake = fork_stake;
                    stats.total_stake = total_stake;
                    stats.voted_stakes = voted_stakes;
                    stats.lockout_intervals = lockout_intervals;
                    stats.block_height = bank.block_height();
                    stats.my_latest_landed_vote = my_latest_landed_vote;
                    stats.computed = true;
                    new_stats.push(bank_slot);
                    datapoint_info!(
                        "bank_weight",
                        ("slot", bank_slot, i64),
                        ("fork_stake", stats.fork_stake, i64),
                        ("fork_weight", stats.fork_weight(), f64),
                    );

                    info!(
                        "{} slot_weight: {} {:.1}% {}",
                        my_vote_pubkey,
                        bank_slot,
                        100.0 * stats.fork_weight(), // percentage fork_stake in total_stake
                        bank.parent().map(|b| b.slot()).unwrap_or(0)
                    );
                }
            }

            Self::update_propagation_status(
                progress,
                bank_slot,
                bank_forks,
                vote_tracker,
                cluster_slots,
            );

            Self::cache_tower_stats(progress, tower, bank_slot, ancestors);
        }
        new_stats
    }

    fn adopt_on_chain_tower_if_behind(
        my_vote_pubkey: &Pubkey,
        ancestors: &HashMap<Slot, HashSet<Slot>>,
        frozen_banks: &[Arc<Bank>],
        tower: &mut Tower,
        progress: &mut ProgressMap,
        bank: &Arc<Bank>,
        bank_forks: &RwLock<BankForks>,
    ) {
        let Some(vote_account) = bank.get_vote_account(my_vote_pubkey) else {
            return;
        };
        let Some(vote_state_view) = vote_account.vote_state_view() else {
            return;
        };
        let mut bank_vote_state = TowerVoteState::from(vote_state_view);
        if bank_vote_state.last_voted_slot() <= tower.vote_state.last_voted_slot() {
            return;
        }
        info!(
            "Frozen bank vote state slot {:?} \
             is newer than our local vote state slot {:?}, \
             adopting the bank vote state as our own. \
             Bank votes: {:?}, root: {:?}, \
             Local votes: {:?}, root: {:?}",
            bank_vote_state.last_voted_slot(),
            tower.vote_state.last_voted_slot(),
            bank_vote_state.votes,
            bank_vote_state.root_slot,
            tower.vote_state.votes,
            tower.vote_state.root_slot
        );

        if let Some(local_root) = tower.vote_state.root_slot {
            if bank_vote_state
                .root_slot
                .map(|bank_root| local_root > bank_root)
                .unwrap_or(true)
            {
                // If the local root is larger than this on chain vote state
                // root (possible due to supermajority roots being set on
                // startup), then we need to adjust the tower
                bank_vote_state.root_slot = Some(local_root);
                bank_vote_state
                    .votes
                    .retain(|lockout| lockout.slot() > local_root);
                info!(
                    "Local root is larger than on chain root, \
                     overwrote bank root {:?} and updated votes {:?}",
                    bank_vote_state.root_slot, bank_vote_state.votes
                );

                if let Some(first_vote) = bank_vote_state.votes.front() {
                    assert!(ancestors
                        .get(&first_vote.slot())
                        .expect(
                            "Ancestors map must contain an entry for all slots on this fork \
                             greater than `local_root` and less than `bank_slot`"
                        )
                        .contains(&local_root));
                }
            }
        }

        // adopt the bank vote state
        tower.vote_state = bank_vote_state;

        let last_voted_slot = tower.vote_state.last_voted_slot().unwrap_or(
            // If our local root is higher than the highest slot in `bank_vote_state` due to
            // supermajority roots, then it's expected that the vote state will be empty.
            // In this case we use the root as our last vote. This root cannot be None, because
            // `tower.vote_state.last_voted_slot()` is None only if `tower.vote_state.root_slot`
            // is Some.
            tower
                .vote_state
                .root_slot
                .expect("root_slot cannot be None here"),
        );
        // This is safe because `last_voted_slot` is now equal to
        // `bank_vote_state.last_voted_slot()` or `local_root`.
        // Since this vote state is contained in `bank`, which we have frozen,
        // we must have frozen all slots contained in `bank_vote_state`,
        // and by definition we must have frozen `local_root`.
        //
        // If `bank` is a duplicate, since we are able to replay it successfully, any slots
        // in its vote state must also be part of the duplicate fork, and thus present in our
        // progress map.
        //
        // Finally if both `bank` and `bank_vote_state.last_voted_slot()` are duplicate,
        // we must have the compatible versions of both duplicates in order to replay `bank`
        // successfully, so we are once again guaranteed that `bank_vote_state.last_voted_slot()`
        // is present in bank forks and progress map.
        let block_id = {
            // The block_id here will only be relevant if we need to refresh this last vote.
            let bank = bank_forks
                .read()
                .unwrap()
                .get(last_voted_slot)
                .expect("Last voted slot that we are adopting must exist in bank forks");
            // Here we don't have to check if this is our leader bank, as since we are adopting this bank,
            // that means that it was created from a different instance (hot spare setup or a previous restart),
            // and thus we must have replayed and set the block_id from the shreds.
            // Note: since the new shred format is not rolled out everywhere, we have to provide a default
            bank.block_id().unwrap_or_default()
        };
        tower.update_last_vote_from_vote_state(
            progress
                .get_hash(last_voted_slot)
                .expect("Must exist for us to have frozen descendant"),
            bank.feature_set
                .is_active(&solana_feature_set::enable_tower_sync_ix::id()),
            block_id,
        );
        // Since we are updating our tower we need to update associated caches for previously computed
        // slots as well.
        for slot in frozen_banks.iter().map(|b| b.slot()) {
            if !progress
                .get_fork_stats(slot)
                .expect("All frozen banks must exist in fork stats")
                .computed
            {
                continue;
            }
            Self::cache_tower_stats(progress, tower, slot, ancestors);
        }
    }

    fn cache_tower_stats(
        progress: &mut ProgressMap,
        tower: &Tower,
        slot: Slot,
        ancestors: &HashMap<u64, HashSet<u64>>,
    ) {
        let stats = progress
            .get_fork_stats_mut(slot)
            .expect("All frozen banks must exist in the Progress map");

        stats.vote_threshold =
            tower.check_vote_stake_thresholds(slot, &stats.voted_stakes, stats.total_stake);
        stats.is_locked_out = tower.is_locked_out(
            slot,
            ancestors
                .get(&slot)
                .expect("Ancestors map should contain slot for is_locked_out() check"),
        );
        stats.has_voted = tower.has_voted(slot);
        stats.is_recent = tower.is_recent(slot);
    }

    fn update_propagation_status(
        progress: &mut ProgressMap,
        slot: Slot,
        bank_forks: &RwLock<BankForks>,
        vote_tracker: &VoteTracker,
        cluster_slots: &ClusterSlots,
    ) {
        // We would only reach here if the bank is in bank_forks, so it
        // isn't dumped and should exist in progress map.
        // If propagation has already been confirmed, return
        if progress.get_leader_propagation_slot_must_exist(slot).0 {
            return;
        }

        // Otherwise we have to check the votes for confirmation
        let propagated_stats = progress
            .get_propagated_stats_mut(slot)
            .unwrap_or_else(|| panic!("slot={slot} must exist in ProgressMap"));

        if propagated_stats.slot_vote_tracker.is_none() {
            propagated_stats.slot_vote_tracker = vote_tracker.get_slot_vote_tracker(slot);
        }
        let slot_vote_tracker = propagated_stats.slot_vote_tracker.clone();

        if propagated_stats.cluster_slot_pubkeys.is_none() {
            propagated_stats.cluster_slot_pubkeys = cluster_slots.lookup(slot);
        }
        let cluster_slot_pubkeys = propagated_stats.cluster_slot_pubkeys.clone();

        let newly_voted_pubkeys = slot_vote_tracker
            .as_ref()
            .and_then(|slot_vote_tracker| {
                slot_vote_tracker.write().unwrap().get_voted_slot_updates()
            })
            .unwrap_or_default();

        let cluster_slot_pubkeys = cluster_slot_pubkeys
            .map(|v| v.read().unwrap().keys().cloned().collect())
            .unwrap_or_default();

        Self::update_fork_propagated_threshold_from_votes(
            progress,
            newly_voted_pubkeys,
            cluster_slot_pubkeys,
            slot,
            &bank_forks.read().unwrap(),
        );
    }

    fn update_fork_propagated_threshold_from_votes(
        progress: &mut ProgressMap,
        mut newly_voted_pubkeys: Vec<Pubkey>,
        mut cluster_slot_pubkeys: Vec<Pubkey>,
        fork_tip: Slot,
        bank_forks: &BankForks,
    ) {
        // We would only reach here if the bank is in bank_forks, so it
        // isn't dumped and should exist in progress map.
        let mut current_leader_slot = progress.get_latest_leader_slot_must_exist(fork_tip);
        let mut did_newly_reach_threshold = false;
        let root = bank_forks.root();
        loop {
            // These cases mean confirmation of propagation on any earlier
            // leader blocks must have been reached
            if current_leader_slot.is_none() || current_leader_slot.unwrap() < root {
                break;
            }

            let leader_propagated_stats = progress
                .get_propagated_stats_mut(current_leader_slot.unwrap())
                .expect("current_leader_slot >= root, so must exist in the progress map");

            // If a descendant has reached propagation threshold, then
            // all its ancestor banks have also reached propagation
            // threshold as well (Validators can't have voted for a
            // descendant without also getting the ancestor block)
            if leader_propagated_stats.is_propagated || {
                // If there's no new validators to record, and there's no
                // newly achieved threshold, then there's no further
                // information to propagate backwards to past leader blocks
                newly_voted_pubkeys.is_empty()
                    && cluster_slot_pubkeys.is_empty()
                    && !did_newly_reach_threshold
            } {
                break;
            }

            // We only iterate through the list of leader slots by traversing
            // the linked list of 'prev_leader_slot`'s outlined in the
            // `progress` map
            assert!(leader_propagated_stats.is_leader_slot);
            let leader_bank = bank_forks
                .get(current_leader_slot.unwrap())
                .expect("Entry in progress map must exist in BankForks")
                .clone();

            did_newly_reach_threshold = Self::update_slot_propagated_threshold_from_votes(
                &mut newly_voted_pubkeys,
                &mut cluster_slot_pubkeys,
                &leader_bank,
                leader_propagated_stats,
                did_newly_reach_threshold,
            ) || did_newly_reach_threshold;

            // Now jump to process the previous leader slot
            current_leader_slot = leader_propagated_stats.prev_leader_slot;
        }
    }

    fn update_slot_propagated_threshold_from_votes(
        newly_voted_pubkeys: &mut Vec<Pubkey>,
        cluster_slot_pubkeys: &mut Vec<Pubkey>,
        leader_bank: &Bank,
        leader_propagated_stats: &mut PropagatedStats,
        did_child_reach_threshold: bool,
    ) -> bool {
        // Track whether this slot newly confirm propagation
        // throughout the network (switched from is_propagated == false
        // to is_propagated == true)
        let mut did_newly_reach_threshold = false;

        // If a child of this slot confirmed propagation, then
        // we can return early as this implies this slot must also
        // be propagated
        if did_child_reach_threshold {
            if !leader_propagated_stats.is_propagated {
                leader_propagated_stats.is_propagated = true;
                return true;
            } else {
                return false;
            }
        }

        if leader_propagated_stats.is_propagated {
            return false;
        }

        // Remove the vote/node pubkeys that we already know voted for this
        // slot. These vote accounts/validator identities are safe to drop
        // because they don't to be ported back any further because earlier
        // parents must have:
        // 1) Also recorded these pubkeys already, or
        // 2) Already reached the propagation threshold, in which case
        //    they no longer need to track the set of propagated validators
        newly_voted_pubkeys.retain(|vote_pubkey| {
            let exists = leader_propagated_stats
                .propagated_validators
                .contains(vote_pubkey);
            leader_propagated_stats.add_vote_pubkey(
                *vote_pubkey,
                leader_bank.epoch_vote_account_stake(vote_pubkey),
            );
            !exists
        });

        cluster_slot_pubkeys.retain(|node_pubkey| {
            let exists = leader_propagated_stats
                .propagated_node_ids
                .contains(node_pubkey);
            leader_propagated_stats.add_node_pubkey(node_pubkey, leader_bank);
            !exists
        });

        if leader_propagated_stats.total_epoch_stake == 0
            || leader_propagated_stats.propagated_validators_stake as f64
                / leader_propagated_stats.total_epoch_stake as f64
                > SUPERMINORITY_THRESHOLD
        {
            leader_propagated_stats.is_propagated = true;
            did_newly_reach_threshold = true
        }

        did_newly_reach_threshold
    }

    #[allow(clippy::too_many_arguments)]
    fn mark_slots_duplicate_confirmed(
        confirmed_slots: &[(Slot, Hash)],
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
        purge_repair_slot_counter: &mut PurgeRepairSlotCounter,
        duplicate_confirmed_slots: &mut DuplicateConfirmedSlots,
    ) {
        let root_slot = bank_forks.read().unwrap().root();
        for (slot, frozen_hash) in confirmed_slots.iter() {
            assert!(*frozen_hash != Hash::default());

            if *slot <= root_slot {
                continue;
            }

            progress.set_duplicate_confirmed_hash(*slot, *frozen_hash);
            if let Some(prev_hash) = duplicate_confirmed_slots.insert(*slot, *frozen_hash) {
                assert_eq!(
                    prev_hash, *frozen_hash,
                    "Additional duplicate confirmed notification for slot {slot} with a different \
                     hash"
                );
                // Already processed this signal
                continue;
            }

            let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
                *frozen_hash,
                || false,
                || Some(*frozen_hash),
            );
            check_slot_agrees_with_cluster(
                *slot,
                root_slot,
                blockstore,
                duplicate_slots_tracker,
                epoch_slots_frozen_slots,
                fork_choice,
                duplicate_slots_to_repair,
                ancestor_hashes_replay_update_sender,
                purge_repair_slot_counter,
                SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
            );
        }
    }

    fn tower_duplicate_confirmed_forks(
        tower: &Tower,
        voted_stakes: &VotedStakes,
        total_stake: Stake,
        progress: &ProgressMap,
        bank_forks: &RwLock<BankForks>,
    ) -> Vec<(Slot, Hash)> {
        let mut duplicate_confirmed_forks = vec![];
        for (slot, prog) in progress.iter() {
            if prog.fork_stats.duplicate_confirmed_hash.is_some() {
                continue;
            }
            // TODO(ashwin): expect once we share progress
            let Some(bank) = bank_forks.read().unwrap().get(*slot) else {
                continue;
            };
            let duration = prog
                .replay_stats
                .read()
                .unwrap()
                .started
                .elapsed()
                .as_millis();
            if !bank.is_frozen() {
                continue;
            }
            if tower.is_slot_duplicate_confirmed(*slot, voted_stakes, total_stake) {
                info!(
                    "validator fork duplicate confirmed {} {}ms",
                    *slot, duration
                );
                datapoint_info!(
                    "validator-duplicate-confirmation",
                    ("duration_ms", duration, i64)
                );
                duplicate_confirmed_forks.push((*slot, bank.hash()));
            } else {
                debug!(
                    "validator fork not confirmed {} {}ms {:?}",
                    *slot,
                    duration,
                    voted_stakes.get(slot)
                );
            }
        }
        duplicate_confirmed_forks
    }

    #[allow(clippy::too_many_arguments)]
    /// A wrapper around `root_utils::check_and_handle_new_root` which:
    /// - calls into `root_utils::set_bank_forks_root`
    /// - Executes `set_progress_and_tower_bft_root` to cleanup tower bft structs and the progress map
    fn check_and_handle_new_root(
        my_pubkey: &Pubkey,
        parent_slot: Slot,
        new_root: Slot,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        blockstore: &Blockstore,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        accounts_background_request_sender: &AbsRequestSender,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        highest_super_majority_root: Option<Slot>,
        bank_notification_sender: &Option<BankNotificationSenderConfig>,
        has_new_vote_been_rooted: &mut bool,
        voted_signatures: &mut Vec<Signature>,
        drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
        tbft_structs: &mut TowerBFTStructures,
    ) -> Result<(), SetRootError> {
        root_utils::check_and_handle_new_root(
            parent_slot,
            new_root,
            accounts_background_request_sender,
            highest_super_majority_root,
            bank_notification_sender,
            drop_bank_sender,
            blockstore,
            leader_schedule_cache,
            bank_forks,
            rpc_subscriptions,
            my_pubkey,
            has_new_vote_been_rooted,
            voted_signatures,
            move |bank_forks| {
                Self::set_progress_and_tower_bft_root(new_root, bank_forks, progress, tbft_structs)
            },
        )
    }

    // To avoid code duplication and keep compatibility with alpenglow, we add this
    // extra callback in the rooting path. This happens immediately after setting the bank forks root
    fn set_progress_and_tower_bft_root(
        new_root: Slot,
        bank_forks: &BankForks,
        progress: &mut ProgressMap,
        tbft_structs: &mut TowerBFTStructures,
    ) {
        progress.handle_new_root(bank_forks);
        let TowerBFTStructures {
            heaviest_subtree_fork_choice,
            duplicate_slots_tracker,
            duplicate_confirmed_slots,
            unfrozen_gossip_verified_vote_hashes,
            epoch_slots_frozen_slots,
            ..
        } = tbft_structs;
        heaviest_subtree_fork_choice.set_tree_root((new_root, bank_forks.root_bank().hash()));
        *duplicate_slots_tracker = duplicate_slots_tracker.split_off(&new_root);
        // duplicate_slots_tracker now only contains entries >= `new_root`

        *duplicate_confirmed_slots = duplicate_confirmed_slots.split_off(&new_root);
        // gossip_confirmed_slots now only contains entries >= `new_root`

        unfrozen_gossip_verified_vote_hashes.set_root(new_root);
        *epoch_slots_frozen_slots = epoch_slots_frozen_slots.split_off(&new_root);
        // epoch_slots_frozen_slots now only contains entries >= `new_root`
    }

    #[allow(clippy::too_many_arguments)]
    /// A wrapper around `root_utils::set_bank_forks_root` which additionally:
    /// - Executes `set_progress_and_tower_bft_root` to cleanup tower bft structs and the progress map
    pub fn handle_new_root(
        new_root: Slot,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        accounts_background_request_sender: &AbsRequestSender,
        highest_super_majority_root: Option<Slot>,
        has_new_vote_been_rooted: &mut bool,
        voted_signatures: &mut Vec<Signature>,
        drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
        tbft_structs: &mut TowerBFTStructures,
    ) -> Result<(), SetRootError> {
        root_utils::set_bank_forks_root(
            new_root,
            bank_forks,
            accounts_background_request_sender,
            highest_super_majority_root,
            has_new_vote_been_rooted,
            voted_signatures,
            drop_bank_sender,
            move |bank_forks| {
                Self::set_progress_and_tower_bft_root(new_root, bank_forks, progress, tbft_structs)
            },
        )?;
        Ok(())
    }

    fn generate_new_bank_forks(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        progress: &mut ProgressMap,
        replay_timing: &mut ReplayLoopTiming,
    ) {
        // Find the next slot that chains to the old slot
        let mut generate_new_bank_forks_read_lock =
            Measure::start("generate_new_bank_forks_read_lock");
        let forks = bank_forks.read().unwrap();
        generate_new_bank_forks_read_lock.stop();

        let frozen_banks = forks.frozen_banks();
        let frozen_bank_slots: Vec<u64> = frozen_banks
            .keys()
            .cloned()
            .filter(|s| *s >= forks.root())
            .collect();
        let mut generate_new_bank_forks_get_slots_since =
            Measure::start("generate_new_bank_forks_get_slots_since");
        let next_slots = blockstore
            .get_slots_since(&frozen_bank_slots)
            .expect("Db error");
        generate_new_bank_forks_get_slots_since.stop();

        // Filter out what we've already seen
        trace!("generate new forks {:?}", {
            let mut next_slots = next_slots.iter().collect::<Vec<_>>();
            next_slots.sort();
            next_slots
        });
        let mut generate_new_bank_forks_loop = Measure::start("generate_new_bank_forks_loop");
        let mut new_banks = HashMap::new();
        for (parent_slot, children) in next_slots {
            let parent_bank = frozen_banks
                .get(&parent_slot)
                .expect("missing parent in bank forks");
            for child_slot in children {
                if forks.get(child_slot).is_some() || new_banks.contains_key(&child_slot) {
                    trace!("child already active or frozen {}", child_slot);
                    continue;
                }
                let leader = leader_schedule_cache
                    .slot_leader_at(child_slot, Some(parent_bank))
                    .unwrap();
                info!(
                    "new fork:{} parent:{} root:{}",
                    child_slot,
                    parent_slot,
                    forks.root()
                );
                let child_bank = Self::new_bank_from_parent_with_notify(
                    parent_bank.clone(),
                    child_slot,
                    forks.root(),
                    &leader,
                    rpc_subscriptions,
                    slot_status_notifier,
                    NewBankOptions::default(),
                );
                // Set ticks for received banks, block creation loop will take care of leader banks
                blockstore_processor::set_alpenglow_ticks(&child_bank);
                let empty: Vec<Pubkey> = vec![];
                Self::update_fork_propagated_threshold_from_votes(
                    progress,
                    empty,
                    vec![leader],
                    parent_bank.slot(),
                    &forks,
                );
                new_banks.insert(child_slot, child_bank);
            }
        }
        drop(forks);
        generate_new_bank_forks_loop.stop();

        let mut generate_new_bank_forks_write_lock =
            Measure::start("generate_new_bank_forks_write_lock");
        if !new_banks.is_empty() {
            let mut forks = bank_forks.write().unwrap();
            let root = forks.root();
            for (slot, bank) in new_banks {
                if slot < root {
                    continue;
                }
                if forks.get(bank.parent_slot()).is_none() {
                    continue;
                }
                forks.insert(bank);
            }
        }
        generate_new_bank_forks_write_lock.stop();
        saturating_add_assign!(
            replay_timing.generate_new_bank_forks_read_lock_us,
            generate_new_bank_forks_read_lock.as_us()
        );
        saturating_add_assign!(
            replay_timing.generate_new_bank_forks_get_slots_since_us,
            generate_new_bank_forks_get_slots_since.as_us()
        );
        saturating_add_assign!(
            replay_timing.generate_new_bank_forks_loop_us,
            generate_new_bank_forks_loop.as_us()
        );
        saturating_add_assign!(
            replay_timing.generate_new_bank_forks_write_lock_us,
            generate_new_bank_forks_write_lock.as_us()
        );
    }

    pub(crate) fn new_bank_from_parent_with_notify(
        parent: Arc<Bank>,
        slot: u64,
        root_slot: u64,
        leader: &Pubkey,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        new_bank_options: NewBankOptions,
    ) -> Bank {
        rpc_subscriptions.notify_slot(slot, parent.slot(), root_slot);
        if let Some(slot_status_notifier) = slot_status_notifier {
            slot_status_notifier
                .read()
                .unwrap()
                .notify_created_bank(slot, parent.slot());
        }
        Bank::new_from_parent_with_options(parent, leader, slot, new_bank_options)
    }

    fn log_heaviest_fork_failures(
        heaviest_fork_failures: &Vec<HeaviestForkFailures>,
        bank_forks: &Arc<RwLock<BankForks>>,
        tower: &Tower,
        progress: &ProgressMap,
        ancestors: &HashMap<Slot, HashSet<Slot>>,
        heaviest_bank: &Arc<Bank>,
        last_threshold_failure_slot: &mut Slot,
    ) {
        info!(
            "Couldn't vote on heaviest fork: {:?}, heaviest_fork_failures: {:?}",
            heaviest_bank.slot(),
            heaviest_fork_failures
        );

        for failure in heaviest_fork_failures {
            match failure {
                HeaviestForkFailures::NoPropagatedConfirmation(slot, ..) => {
                    // If failure is NoPropagatedConfirmation, then inside select_vote_and_reset_forks
                    // we already confirmed it's in progress map, we should see it in progress map
                    // here because we don't have dump and repair in between.
                    if let Some(latest_leader_slot) =
                        progress.get_latest_leader_slot_must_exist(*slot)
                    {
                        progress.log_propagated_stats(latest_leader_slot, bank_forks);
                    }
                }
                &HeaviestForkFailures::FailedThreshold(
                    slot,
                    depth,
                    observed_stake,
                    total_stake,
                ) => {
                    if slot > *last_threshold_failure_slot {
                        *last_threshold_failure_slot = slot;
                        let in_partition = if let Some(last_voted_slot) = tower.last_voted_slot() {
                            Self::is_partition_detected(
                                ancestors,
                                last_voted_slot,
                                heaviest_bank.slot(),
                            )
                        } else {
                            false
                        };
                        datapoint_info!(
                            "replay_stage-threshold-failure",
                            ("slot", slot as i64, i64),
                            ("depth", depth as i64, i64),
                            ("observed_stake", observed_stake as i64, i64),
                            ("total_stake", total_stake as i64, i64),
                            ("in_partition", in_partition, bool),
                        );
                    }
                }
                // These are already logged in the partition info
                HeaviestForkFailures::LockedOut(_)
                | HeaviestForkFailures::FailedSwitchThreshold(_, _, _) => (),
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.commitment_service.join()?;
        self.votor.join()?;
        self.t_replay.join().map(|_| ())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use {
        super::*,
        crate::{
            consensus::{
                progress_map::{ValidatorStakeInfo, RETRANSMIT_BASE_DELAY_MS},
                tower_storage::{FileTowerStorage, NullTowerStorage},
                tree_diff::TreeDiff,
                ThresholdDecision, Tower, VOTE_THRESHOLD_DEPTH,
            },
            replay_stage::ReplayStage,
            vote_simulator::{self, VoteSimulator},
        },
        blockstore_processor::{
            confirm_full_slot, fill_blockstore_slot_with_ticks, process_bank_0, ProcessOptions,
        },
        crossbeam_channel::unbounded,
        itertools::Itertools,
        solana_client::connection_cache::ConnectionCache,
        solana_entry::entry::{self, Entry},
        solana_gossip::{cluster_info::Node, crds::Cursor},
        solana_ledger::{
            blockstore::{entries_to_test_shreds, make_slot_entries, BlockstoreError},
            create_new_tmp_ledger,
            genesis_utils::{create_genesis_config, create_genesis_config_with_leader},
            get_tmp_ledger_path, get_tmp_ledger_path_auto_delete,
            shred::{Shred, ShredFlags, LEGACY_SHRED_DATA_CAPACITY},
        },
        solana_rpc::{
            optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
            rpc::{create_test_transaction_entries, populate_blockstore_for_tests},
            slot_status_notifier::SlotStatusNotifierInterface,
        },
        solana_runtime::{
            accounts_background_service::AbsRequestSender,
            commitment::{BlockCommitment, VOTE_THRESHOLD_SIZE},
            genesis_utils::{GenesisConfigInfo, ValidatorVoteKeypairs},
        },
        solana_sdk::{
            clock::NUM_CONSECUTIVE_LEADER_SLOTS,
            genesis_config,
            hash::{hash, Hash},
            instruction::InstructionError,
            poh_config::PohConfig,
            signature::{Keypair, Signer},
            system_transaction,
            transaction::TransactionError,
        },
        solana_streamer::socket::SocketAddrSpace,
        solana_tpu_client::tpu_client::{DEFAULT_TPU_CONNECTION_POOL_SIZE, DEFAULT_VOTE_USE_QUIC},
        solana_transaction_status::VersionedTransactionWithStatusMeta,
        solana_vote::vote_transaction,
        solana_vote_program::vote_state::{self, TowerSync, VoteStateVersions},
        std::{
            fs::remove_dir_all,
            iter,
            sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
        },
        tempfile::tempdir,
        test_case::test_case,
        trees::{tr, Tree},
    };

    fn new_bank_from_parent_with_bank_forks(
        bank_forks: &RwLock<BankForks>,
        parent: Arc<Bank>,
        collector_id: &Pubkey,
        slot: Slot,
    ) -> Arc<Bank> {
        let bank = Bank::new_from_parent(parent, collector_id, slot);
        bank.set_block_id(Some(Hash::new_unique()));
        bank_forks
            .write()
            .unwrap()
            .insert(bank)
            .clone_without_scheduler()
    }

    #[test]
    fn test_is_partition_detected() {
        let (VoteSimulator { bank_forks, .. }, _) = setup_default_forks(1, None::<GenerateVotes>);
        let ancestors = bank_forks.read().unwrap().ancestors();
        // Last vote 1 is an ancestor of the heaviest slot 3, no partition
        assert!(!ReplayStage::is_partition_detected(&ancestors, 1, 3));
        // Last vote 1 is an ancestor of the from heaviest slot 1, no partition
        assert!(!ReplayStage::is_partition_detected(&ancestors, 3, 3));
        // Last vote 2 is not an ancestor of the heaviest slot 3,
        // partition detected!
        assert!(ReplayStage::is_partition_detected(&ancestors, 2, 3));
        // Last vote 4 is not an ancestor of the heaviest slot 3,
        // partition detected!
        assert!(ReplayStage::is_partition_detected(&ancestors, 4, 3));
    }

    pub struct ReplayBlockstoreComponents {
        pub blockstore: Arc<Blockstore>,
        validator_node_to_vote_keys: HashMap<Pubkey, Pubkey>,
        pub(crate) my_pubkey: Pubkey,
        cluster_info: ClusterInfo,
        pub(crate) leader_schedule_cache: Arc<LeaderScheduleCache>,
        poh_recorder: RwLock<PohRecorder>,
        tower: Tower,
        rpc_subscriptions: Arc<RpcSubscriptions>,
        pub vote_simulator: VoteSimulator,
    }

    pub fn replay_blockstore_components(
        forks: Option<Tree<Slot>>,
        num_validators: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> ReplayBlockstoreComponents {
        // Setup blockstore
        let (vote_simulator, blockstore) = setup_forks_from_tree(
            forks.unwrap_or_else(|| tr(0)),
            num_validators,
            generate_votes,
        );

        let VoteSimulator {
            ref validator_keypairs,
            ref bank_forks,
            ..
        } = vote_simulator;

        let blockstore = Arc::new(blockstore);
        let validator_node_to_vote_keys: HashMap<Pubkey, Pubkey> = validator_keypairs
            .iter()
            .map(|(_, keypairs)| {
                (
                    keypairs.node_keypair.pubkey(),
                    keypairs.vote_keypair.pubkey(),
                )
            })
            .collect();

        // ClusterInfo
        let my_keypairs = validator_keypairs.values().next().unwrap();
        let my_pubkey = my_keypairs.node_keypair.pubkey();
        let cluster_info = ClusterInfo::new(
            Node::new_localhost_with_pubkey(&my_pubkey).info,
            Arc::new(my_keypairs.node_keypair.insecure_clone()),
            SocketAddrSpace::Unspecified,
        );
        assert_eq!(my_pubkey, cluster_info.id());

        // Leader schedule cache
        let root_bank = bank_forks.read().unwrap().root_bank();
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&root_bank));

        // PohRecorder
        let working_bank = bank_forks.read().unwrap().working_bank();
        let poh_recorder = RwLock::new(
            PohRecorder::new(
                working_bank.tick_height(),
                working_bank.last_blockhash(),
                working_bank.clone(),
                None,
                working_bank.ticks_per_slot(),
                blockstore.clone(),
                &leader_schedule_cache,
                &PohConfig::default(),
                Arc::new(AtomicBool::new(false)),
            )
            .0,
        );

        // Tower
        let my_vote_pubkey = my_keypairs.vote_keypair.pubkey();
        let tower = Tower::new_from_bankforks(
            &bank_forks.read().unwrap(),
            &cluster_info.id(),
            &my_vote_pubkey,
        );

        // RpcSubscriptions
        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(bank_forks);
        let exit = Arc::new(AtomicBool::new(false));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            exit,
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks.clone(),
            Arc::new(RwLock::new(BlockCommitmentCache::default())),
            optimistically_confirmed_bank,
        ));

        ReplayBlockstoreComponents {
            blockstore,
            validator_node_to_vote_keys,
            my_pubkey,
            cluster_info,
            leader_schedule_cache,
            poh_recorder,
            tower,
            rpc_subscriptions,
            vote_simulator,
        }
    }

    #[test]
    fn test_child_slots_of_same_parent() {
        let ReplayBlockstoreComponents {
            blockstore,
            validator_node_to_vote_keys,
            vote_simulator,
            leader_schedule_cache,
            rpc_subscriptions,
            ..
        } = replay_blockstore_components(None, 1, None::<GenerateVotes>);

        let VoteSimulator {
            mut progress,
            bank_forks,
            ..
        } = vote_simulator;

        // Insert a non-root bank so that the propagation logic will update this
        // bank
        let bank1 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &leader_schedule_cache.slot_leader_at(1, None).unwrap(),
            1,
        );
        progress.insert(
            1,
            ForkProgress::new_from_bank(
                &bank1,
                bank1.collector_id(),
                validator_node_to_vote_keys
                    .get(bank1.collector_id())
                    .unwrap(),
                Some(0),
                0,
                0,
            ),
        );
        assert!(progress.get_propagated_stats(1).unwrap().is_leader_slot);
        bank1.freeze();
        bank_forks.write().unwrap().insert(bank1);

        // Insert shreds for slot NUM_CONSECUTIVE_LEADER_SLOTS,
        // chaining to slot 1
        let (shreds, _) = make_slot_entries(
            NUM_CONSECUTIVE_LEADER_SLOTS, // slot
            1,                            // parent_slot
            8,                            // num_entries
            true,                         // merkle_variant
        );
        blockstore.insert_shreds(shreds, None, false).unwrap();
        assert!(bank_forks
            .read()
            .unwrap()
            .get(NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_none());
        let mut replay_timing = ReplayLoopTiming::default();
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &mut replay_timing,
        );
        assert!(bank_forks
            .read()
            .unwrap()
            .get(NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_some());

        // Insert shreds for slot 2 * NUM_CONSECUTIVE_LEADER_SLOTS,
        // chaining to slot 1
        let (shreds, _) = make_slot_entries(
            2 * NUM_CONSECUTIVE_LEADER_SLOTS,
            1,
            8,
            true, // merkle_variant
        );
        blockstore.insert_shreds(shreds, None, false).unwrap();
        assert!(bank_forks
            .read()
            .unwrap()
            .get(2 * NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_none());
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &mut replay_timing,
        );
        assert!(bank_forks
            .read()
            .unwrap()
            .get(NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_some());
        assert!(bank_forks
            .read()
            .unwrap()
            .get(2 * NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_some());

        // // There are 20 equally staked accounts, of which 3 have built
        // banks above or at bank 1. Because 3/20 < SUPERMINORITY_THRESHOLD,
        // we should see 3 validators in bank 1's propagated_validator set.
        let expected_leader_slots = vec![
            1,
            NUM_CONSECUTIVE_LEADER_SLOTS,
            2 * NUM_CONSECUTIVE_LEADER_SLOTS,
        ];
        for slot in expected_leader_slots {
            let leader = leader_schedule_cache.slot_leader_at(slot, None).unwrap();
            let vote_key = validator_node_to_vote_keys.get(&leader).unwrap();
            assert!(progress
                .get_propagated_stats(1)
                .unwrap()
                .propagated_validators
                .contains(vote_key));
        }
    }

    #[test]
    fn test_handle_new_root() {
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank0 = Bank::new_for_tests(&genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);

        let root = 3;
        let root_bank = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &Pubkey::default(),
            root,
        );
        root_bank.freeze();
        let root_hash = root_bank.hash();
        bank_forks.write().unwrap().insert(root_bank);

        let mut progress = ProgressMap::default();
        for i in 0..=root {
            progress.insert(i, ForkProgress::new(Hash::default(), None, None, 0, 0));
        }

        let duplicate_slots_tracker: DuplicateSlotsTracker =
            vec![root - 1, root, root + 1].into_iter().collect();
        let duplicate_confirmed_slots: DuplicateConfirmedSlots = vec![root - 1, root, root + 1]
            .into_iter()
            .map(|s| (s, Hash::default()))
            .collect();
        let unfrozen_gossip_verified_vote_hashes: UnfrozenGossipVerifiedVoteHashes =
            UnfrozenGossipVerifiedVoteHashes {
                votes_per_slot: vec![root - 1, root, root + 1]
                    .into_iter()
                    .map(|s| (s, HashMap::new()))
                    .collect(),
            };
        let epoch_slots_frozen_slots: EpochSlotsFrozenSlots = vec![root - 1, root, root + 1]
            .into_iter()
            .map(|slot| (slot, Hash::default()))
            .collect();
        let (drop_bank_sender, _drop_bank_receiver) = unbounded();
        let mut tbft_structs = TowerBFTStructures {
            heaviest_subtree_fork_choice: HeaviestSubtreeForkChoice::new((root, root_hash)),
            duplicate_slots_tracker,
            duplicate_confirmed_slots,
            unfrozen_gossip_verified_vote_hashes,
            epoch_slots_frozen_slots,
        };
        ReplayStage::handle_new_root(
            root,
            &bank_forks,
            &mut progress,
            &AbsRequestSender::default(),
            None,
            &mut true,
            &mut Vec::new(),
            &drop_bank_sender,
            &mut tbft_structs,
        )
        .unwrap();
        assert_eq!(bank_forks.read().unwrap().root(), root);
        assert_eq!(progress.len(), 1);
        assert!(progress.get(&root).is_some());
        // root - 1 is filtered out
        assert_eq!(
            tbft_structs
                .duplicate_slots_tracker
                .into_iter()
                .collect::<Vec<Slot>>(),
            vec![root, root + 1]
        );
        assert_eq!(
            tbft_structs
                .duplicate_confirmed_slots
                .keys()
                .cloned()
                .collect::<Vec<Slot>>(),
            vec![root, root + 1]
        );
        assert_eq!(
            tbft_structs
                .unfrozen_gossip_verified_vote_hashes
                .votes_per_slot
                .keys()
                .cloned()
                .collect::<Vec<Slot>>(),
            vec![root, root + 1]
        );
        assert_eq!(
            tbft_structs
                .epoch_slots_frozen_slots
                .into_keys()
                .collect::<Vec<Slot>>(),
            vec![root, root + 1]
        );
    }

    #[test]
    fn test_handle_new_root_ahead_of_highest_super_majority_root() {
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank0 = Bank::new_for_tests(&genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);
        let confirmed_root = 1;
        let fork = 2;
        let bank1 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &Pubkey::default(),
            confirmed_root,
        );
        bank_forks.write().unwrap().insert(bank1);
        let bank2 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(confirmed_root).unwrap(),
            &Pubkey::default(),
            fork,
        );
        bank_forks.write().unwrap().insert(bank2);
        let root = 3;
        let root_bank = Bank::new_from_parent(
            bank_forks.read().unwrap().get(confirmed_root).unwrap(),
            &Pubkey::default(),
            root,
        );
        root_bank.freeze();
        let root_hash = root_bank.hash();
        bank_forks.write().unwrap().insert(root_bank);
        let mut progress = ProgressMap::default();
        for i in 0..=root {
            progress.insert(i, ForkProgress::new(Hash::default(), None, None, 0, 0));
        }
        let (drop_bank_sender, _drop_bank_receiver) = unbounded();
        let mut tbft_structs = TowerBFTStructures {
            heaviest_subtree_fork_choice: HeaviestSubtreeForkChoice::new((root, root_hash)),
            duplicate_slots_tracker: DuplicateSlotsTracker::default(),
            duplicate_confirmed_slots: DuplicateConfirmedSlots::default(),
            unfrozen_gossip_verified_vote_hashes: UnfrozenGossipVerifiedVoteHashes::default(),
            epoch_slots_frozen_slots: EpochSlotsFrozenSlots::default(),
        };
        ReplayStage::handle_new_root(
            root,
            &bank_forks,
            &mut progress,
            &AbsRequestSender::default(),
            Some(confirmed_root),
            &mut true,
            &mut Vec::new(),
            &drop_bank_sender,
            &mut tbft_structs,
        )
        .unwrap();
        assert_eq!(bank_forks.read().unwrap().root(), root);
        assert!(bank_forks.read().unwrap().get(confirmed_root).is_some());
        assert!(bank_forks.read().unwrap().get(fork).is_none());
        assert_eq!(progress.len(), 2);
        assert!(progress.get(&root).is_some());
        assert!(progress.get(&confirmed_root).is_some());
        assert!(progress.get(&fork).is_none());
    }

    #[test]
    fn test_dead_fork_transaction_error() {
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let missing_keypair = Keypair::new();
        let missing_keypair2 = Keypair::new();

        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            let entry = entry::next_entry(
                &blockhash,
                hashes_per_tick.saturating_sub(1),
                vec![
                    system_transaction::transfer(&keypair1, &keypair2.pubkey(), 2, blockhash), // should be fine,
                    system_transaction::transfer(
                        &missing_keypair,
                        &missing_keypair2.pubkey(),
                        2,
                        blockhash,
                    ), // should cause AccountNotFound error
                ],
            );
            entries_to_test_shreds(
                &[entry],
                slot,
                slot.saturating_sub(1), // parent_slot
                false,                  // is_full_slot
                0,                      // version
                true,                   // merkle_variant
            )
        });

        assert_matches!(
            res,
            Err(BlockstoreProcessorError::InvalidTransaction(
                TransactionError::AccountNotFound
            ))
        );
    }

    #[test]
    fn test_dead_fork_entry_verification_failure() {
        let keypair2 = Keypair::new();
        let res = check_dead_fork(|genesis_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let bad_hash = hash(&[2; 30]);
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            let entry = entry::next_entry(
                // Use wrong blockhash so that the entry causes an entry verification failure
                &bad_hash,
                hashes_per_tick.saturating_sub(1),
                vec![system_transaction::transfer(
                    genesis_keypair,
                    &keypair2.pubkey(),
                    2,
                    blockhash,
                )],
            );
            entries_to_test_shreds(
                &[entry],
                slot,
                slot.saturating_sub(1), // parent_slot
                false,                  // is_full_slot
                0,                      // version
                true,                   // merkle_variant
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::InvalidEntryHash);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_invalid_tick_hash_count() {
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            assert!(hashes_per_tick > 0);

            let too_few_hashes_tick = Entry::new(&blockhash, hashes_per_tick - 1, vec![]);
            entries_to_test_shreds(
                &[too_few_hashes_tick],
                slot,
                slot.saturating_sub(1),
                false,
                0,
                true, // merkle_variant
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::InvalidTickHashCount);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_invalid_slot_tick_count() {
        solana_logger::setup();
        // Too many ticks per slot
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            entries_to_test_shreds(
                &entry::create_ticks(bank.ticks_per_slot() + 1, hashes_per_tick, blockhash),
                slot,
                slot.saturating_sub(1),
                false,
                0,
                true, // merkle_variant
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::TooManyTicks);
        } else {
            panic!();
        }

        // Too few ticks per slot
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            entries_to_test_shreds(
                &entry::create_ticks(bank.ticks_per_slot() - 1, hashes_per_tick, blockhash),
                slot,
                slot.saturating_sub(1),
                true,
                0,
                true, // merkle_variant
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::TooFewTicks);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_invalid_last_tick() {
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            entries_to_test_shreds(
                &entry::create_ticks(bank.ticks_per_slot(), hashes_per_tick, blockhash),
                slot,
                slot.saturating_sub(1),
                false,
                0,
                true, // merkle_variant
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::InvalidLastTick);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_trailing_entry() {
        let keypair = Keypair::new();
        let res = check_dead_fork(|funded_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            let mut entries =
                entry::create_ticks(bank.ticks_per_slot(), hashes_per_tick, blockhash);
            let last_entry_hash = entries.last().unwrap().hash;
            let tx = system_transaction::transfer(funded_keypair, &keypair.pubkey(), 2, blockhash);
            let trailing_entry = entry::next_entry(&last_entry_hash, 1, vec![tx]);
            entries.push(trailing_entry);
            entries_to_test_shreds(
                &entries,
                slot,
                slot.saturating_sub(1), // parent_slot
                true,                   // is_full_slot
                0,                      // version
                true,                   // merkle_variant
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::TrailingEntry);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_entry_deserialize_failure() {
        // Insert entry that causes deserialization failure
        let res = check_dead_fork(|_, bank| {
            let gibberish = [0xa5u8; LEGACY_SHRED_DATA_CAPACITY];
            let parent_offset = bank.slot() - bank.parent_slot();
            let shred = Shred::new_from_data(
                bank.slot(),
                0, // index,
                parent_offset as u16,
                &gibberish,
                ShredFlags::DATA_COMPLETE_SHRED,
                0, // reference_tick
                0, // version
                0, // fec_set_index
            );
            vec![shred]
        });

        assert_matches!(
            res,
            Err(BlockstoreProcessorError::FailedToLoadEntries(
                BlockstoreError::InvalidShredData(_)
            ),)
        );
    }

    struct SlotStatusNotifierForTest {
        dead_slots: Arc<Mutex<HashSet<Slot>>>,
    }

    impl SlotStatusNotifierForTest {
        pub fn new(dead_slots: Arc<Mutex<HashSet<Slot>>>) -> Self {
            Self { dead_slots }
        }
    }

    impl SlotStatusNotifierInterface for SlotStatusNotifierForTest {
        fn notify_slot_confirmed(&self, _slot: Slot, _parent: Option<Slot>) {}

        fn notify_slot_processed(&self, _slot: Slot, _parent: Option<Slot>) {}

        fn notify_slot_rooted(&self, _slot: Slot, _parent: Option<Slot>) {}

        fn notify_first_shred_received(&self, _slot: Slot) {}

        fn notify_completed(&self, _slot: Slot) {}

        fn notify_created_bank(&self, _slot: Slot, _parent: Slot) {}

        fn notify_slot_dead(&self, slot: Slot, _error: String) {
            self.dead_slots.lock().unwrap().insert(slot);
        }
    }

    // Given a shred and a fatal expected error, check that replaying that shred causes causes the fork to be
    // marked as dead. Returns the error for caller to verify.
    fn check_dead_fork<F>(shred_to_insert: F) -> result::Result<(), BlockstoreProcessorError>
    where
        F: Fn(&Keypair, Arc<Bank>) -> Vec<Shred>,
    {
        let ledger_path = get_tmp_ledger_path!();
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let res = {
            let ReplayBlockstoreComponents {
                blockstore,
                vote_simulator,
                ..
            } = replay_blockstore_components(Some(tr(0)), 1, None);
            let VoteSimulator {
                mut progress,
                bank_forks,
                mut tbft_structs,
                validator_keypairs,
                ..
            } = vote_simulator;

            let bank0 = bank_forks.read().unwrap().get(0).unwrap();
            assert!(bank0.is_frozen());
            assert_eq!(bank0.tick_height(), bank0.max_tick_height());
            let bank1 = Bank::new_from_parent(bank0, &Pubkey::default(), 1);
            bank_forks.write().unwrap().insert(bank1);
            let bank1 = bank_forks.read().unwrap().get_with_scheduler(1).unwrap();
            let bank1_progress = progress
                .entry(bank1.slot())
                .or_insert_with(|| ForkProgress::new(bank1.last_blockhash(), None, None, 0, 0));
            let shreds = shred_to_insert(
                &validator_keypairs.values().next().unwrap().node_keypair,
                bank1.clone(),
            );
            blockstore.insert_shreds(shreds, None, false).unwrap();
            let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
            let exit = Arc::new(AtomicBool::new(false));
            let replay_tx_thread_pool = rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .thread_name(|i| format!("solReplayTest{i:02}"))
                .build()
                .expect("new rayon threadpool");
            let res = ReplayStage::replay_blockstore_into_bank(
                &bank1,
                &blockstore,
                &replay_tx_thread_pool,
                &bank1_progress.replay_stats,
                &bank1_progress.replay_progress,
                None,
                None,
                &replay_vote_sender,
                &VerifyRecyclers::default(),
                None,
                &PrioritizationFeeCache::new(0u64),
            );
            let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
            let max_complete_rewards_slot = Arc::new(AtomicU64::default());
            let rpc_subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
                exit,
                max_complete_transaction_status_slot,
                max_complete_rewards_slot,
                bank_forks.clone(),
                block_commitment_cache,
                OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
            ));
            let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
                unbounded();
            let dead_slots = Arc::new(Mutex::new(HashSet::default()));

            let slot_status_notifier: Option<SlotStatusNotifier> = Some(Arc::new(RwLock::new(
                SlotStatusNotifierForTest::new(dead_slots.clone()),
            )));

            if let Err(err) = &res {
                ReplayStage::mark_dead_slot(
                    &blockstore,
                    &bank1,
                    0,
                    err,
                    &rpc_subscriptions,
                    &slot_status_notifier,
                    &mut progress,
                    &mut DuplicateSlotsToRepair::default(),
                    &ancestor_hashes_replay_update_sender,
                    &mut PurgeRepairSlotCounter::default(),
                    &mut Some(&mut tbft_structs),
                );
            }
            assert!(dead_slots.lock().unwrap().contains(&bank1.slot()));
            // Check that the erroring bank was marked as dead in the progress map
            assert!(progress
                .get(&bank1.slot())
                .map(|b| b.is_dead)
                .unwrap_or(false));

            // Check that the erroring bank was marked as dead in blockstore
            assert!(blockstore.is_dead(bank1.slot()));
            res.map(|_| ())
        };
        let _ignored = remove_dir_all(ledger_path);
        res
    }

    #[test]
    fn test_replay_commitment_cache() {
        fn leader_vote(vote_slot: Slot, bank: &Bank, pubkey: &Pubkey) -> (Pubkey, TowerVoteState) {
            let mut leader_vote_account = bank.get_account(pubkey).unwrap();
            let mut vote_state = vote_state::from(&leader_vote_account).unwrap();
            vote_state::process_slot_vote_unchecked(&mut vote_state, vote_slot);
            let versioned = VoteStateVersions::new_current(vote_state.clone());
            vote_state::to(&versioned, &mut leader_vote_account).unwrap();
            bank.store_account(pubkey, &leader_vote_account);
            (*pubkey, TowerVoteState::from(vote_state))
        }

        let leader_pubkey = solana_pubkey::new_rand();
        let leader_lamports = 3;
        let genesis_config_info =
            create_genesis_config_with_leader(50, &leader_pubkey, leader_lamports);
        let mut genesis_config = genesis_config_info.genesis_config;
        let leader_voting_pubkey = genesis_config_info.voting_keypair.pubkey();
        genesis_config.epoch_schedule.warmup = false;
        genesis_config.ticks_per_slot = 4;
        let bank0 = Bank::new_for_tests(&genesis_config);
        for _ in 0..genesis_config.ticks_per_slot {
            bank0.register_default_tick_for_test();
        }
        bank0.freeze();
        let bank_forks = BankForks::new_rw_arc(bank0);

        let exit = Arc::new(AtomicBool::new(false));
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            exit.clone(),
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks.clone(),
            block_commitment_cache.clone(),
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
        ));
        let (lockouts_sender, _commitment_sender, _) = AggregateCommitmentService::new(
            exit,
            block_commitment_cache.clone(),
            rpc_subscriptions,
        );

        assert!(block_commitment_cache
            .read()
            .unwrap()
            .get_block_commitment(0)
            .is_none());
        assert!(block_commitment_cache
            .read()
            .unwrap()
            .get_block_commitment(1)
            .is_none());

        for i in 1..=3 {
            let prev_bank = bank_forks.read().unwrap().get(i - 1).unwrap();
            let slot = prev_bank.slot() + 1;
            let bank = new_bank_from_parent_with_bank_forks(
                bank_forks.as_ref(),
                prev_bank,
                &Pubkey::default(),
                slot,
            );
            let _res = bank.transfer(
                10,
                &genesis_config_info.mint_keypair,
                &solana_pubkey::new_rand(),
            );
            for _ in 0..genesis_config.ticks_per_slot {
                bank.register_default_tick_for_test();
            }

            let arc_bank = bank_forks.read().unwrap().get(i).unwrap();
            let node_vote_state = leader_vote(i - 1, &arc_bank, &leader_voting_pubkey);
            ReplayStage::update_commitment_cache(
                arc_bank.clone(),
                0,
                leader_lamports,
                node_vote_state,
                &lockouts_sender,
            );
            arc_bank.freeze();
        }

        for _ in 0..10 {
            let done = {
                let bcc = block_commitment_cache.read().unwrap();
                bcc.get_block_commitment(0).is_some()
                    && bcc.get_block_commitment(1).is_some()
                    && bcc.get_block_commitment(2).is_some()
            };
            if done {
                break;
            } else {
                thread::sleep(Duration::from_millis(200));
            }
        }

        let mut expected0 = BlockCommitment::default();
        expected0.increase_confirmation_stake(3, leader_lamports);
        assert_eq!(
            block_commitment_cache
                .read()
                .unwrap()
                .get_block_commitment(0)
                .unwrap(),
            &expected0,
        );
        let mut expected1 = BlockCommitment::default();
        expected1.increase_confirmation_stake(2, leader_lamports);
        assert_eq!(
            block_commitment_cache
                .read()
                .unwrap()
                .get_block_commitment(1)
                .unwrap(),
            &expected1
        );
        let mut expected2 = BlockCommitment::default();
        expected2.increase_confirmation_stake(1, leader_lamports);
        assert_eq!(
            block_commitment_cache
                .read()
                .unwrap()
                .get_block_commitment(2)
                .unwrap(),
            &expected2
        );
    }

    #[test]
    fn test_write_persist_transaction_status() {
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(solana_sdk::native_token::sol_to_lamports(1000.0));
        genesis_config.rent.lamports_per_byte_year = 50;
        genesis_config.rent.exemption_threshold = 2.0;
        let (ledger_path, _) = create_new_tmp_ledger!(&genesis_config);
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to successfully open database ledger");
            let blockstore = Arc::new(blockstore);

            let keypair1 = Keypair::new();
            let keypair2 = Keypair::new();
            let keypair3 = Keypair::new();

            let (bank0, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
            bank0
                .transfer(
                    bank0.get_minimum_balance_for_rent_exemption(0),
                    &mint_keypair,
                    &keypair2.pubkey(),
                )
                .unwrap();

            let bank1 = bank_forks
                .write()
                .unwrap()
                .insert(Bank::new_from_parent(bank0, &Pubkey::default(), 1))
                .clone_without_scheduler();
            let slot = bank1.slot();

            let (entries, test_signatures) = create_test_transaction_entries(
                vec![&mint_keypair, &keypair1, &keypair2, &keypair3],
                bank1.clone(),
            );
            populate_blockstore_for_tests(
                entries,
                bank1,
                blockstore.clone(),
                Arc::new(AtomicU64::default()),
            );

            let mut test_signatures_iter = test_signatures.into_iter();
            let confirmed_block = blockstore.get_rooted_block(slot, false).unwrap();
            let actual_tx_results: Vec<_> = confirmed_block
                .transactions
                .into_iter()
                .map(|VersionedTransactionWithStatusMeta { transaction, meta }| {
                    (transaction.signatures[0], meta.status)
                })
                .collect();
            let expected_tx_results = vec![
                (test_signatures_iter.next().unwrap(), Ok(())),
                (
                    test_signatures_iter.next().unwrap(),
                    Err(TransactionError::InstructionError(
                        0,
                        InstructionError::Custom(1),
                    )),
                ),
            ];
            assert_eq!(actual_tx_results, expected_tx_results);
            assert!(test_signatures_iter.next().is_none());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_compute_bank_stats_confirmed() {
        let vote_keypairs = ValidatorVoteKeypairs::new_rand();
        let my_node_pubkey = vote_keypairs.node_keypair.pubkey();
        let my_vote_pubkey = vote_keypairs.vote_keypair.pubkey();
        let keypairs: HashMap<_, _> = vec![(my_node_pubkey, vote_keypairs)].into_iter().collect();

        let (bank_forks, mut progress, mut heaviest_subtree_fork_choice) =
            vote_simulator::initialize_state(&keypairs, 10_000);
        let mut latest_validator_votes_for_frozen_banks =
            LatestValidatorVotesForFrozenBanks::default();
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();
        let my_keypairs = keypairs.get(&my_node_pubkey).unwrap();
        let tower_sync = TowerSync::new_from_slots(vec![0], bank0.hash(), None);
        let vote_tx = vote_transaction::new_tower_sync_transaction(
            tower_sync,
            bank0.last_blockhash(),
            &my_keypairs.node_keypair,
            &my_keypairs.vote_keypair,
            &my_keypairs.vote_keypair,
            None,
        );

        // Test confirmations
        let ancestors = bank_forks.read().unwrap().ancestors();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let mut tower = Tower::new_for_tests(0, 0.67);
        let newly_computed = ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &mut tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // bank 0 has no votes, should not send any votes on the channel
        assert_eq!(newly_computed, vec![0]);
        // The only vote is in bank 1, and bank_forks does not currently contain
        // bank 1, so no slot should be confirmed.
        {
            let fork_progress = progress.get(&0).unwrap();
            let confirmed_forks = ReplayStage::tower_duplicate_confirmed_forks(
                &tower,
                &fork_progress.fork_stats.voted_stakes,
                fork_progress.fork_stats.total_stake,
                &progress,
                &bank_forks,
            );

            assert!(confirmed_forks.is_empty());
        }

        let bank1 = new_bank_from_parent_with_bank_forks(
            bank_forks.as_ref(),
            bank0.clone(),
            &my_node_pubkey,
            1,
        );
        bank1.process_transaction(&vote_tx).unwrap();
        bank1.freeze();

        // Insert the bank that contains a vote for slot 0, which confirms slot 0
        progress.insert(
            1,
            ForkProgress::new(bank0.last_blockhash(), None, None, 0, 0),
        );
        let ancestors = bank_forks.read().unwrap().ancestors();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let newly_computed = ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &mut tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Bank 1 had one vote
        assert_eq!(newly_computed, vec![1]);
        {
            let fork_progress = progress.get(&1).unwrap();
            let confirmed_forks = ReplayStage::tower_duplicate_confirmed_forks(
                &tower,
                &fork_progress.fork_stats.voted_stakes,
                fork_progress.fork_stats.total_stake,
                &progress,
                &bank_forks,
            );
            // No new stats should have been computed
            assert_eq!(confirmed_forks, vec![(0, bank0.hash())]);
        }

        let ancestors = bank_forks.read().unwrap().ancestors();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let newly_computed = ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &mut tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );
        // No new stats should have been computed
        assert!(newly_computed.is_empty());
    }

    #[test]
    fn test_same_weight_select_lower_slot() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(1);
        let mut tower = Tower::default();

        // Create the tree of banks in a BankForks object
        let forks = tr(0) / (tr(1)) / (tr(2));
        vote_simulator.fill_bank_forks(forks, &HashMap::new(), true);
        let mut frozen_banks: Vec<_> = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let heaviest_subtree_fork_choice =
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice;
        let mut latest_validator_votes_for_frozen_banks =
            LatestValidatorVotesForFrozenBanks::default();
        let ancestors = vote_simulator.bank_forks.read().unwrap().ancestors();

        let my_vote_pubkey = vote_simulator.vote_pubkeys[0];
        ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &mut tower,
            &mut vote_simulator.progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &vote_simulator.bank_forks,
            heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        let bank1 = vote_simulator.bank_forks.read().unwrap().get(1).unwrap();
        let bank2 = vote_simulator.bank_forks.read().unwrap().get(2).unwrap();
        assert_eq!(
            heaviest_subtree_fork_choice
                .stake_voted_subtree(&(1, bank1.hash()))
                .unwrap(),
            heaviest_subtree_fork_choice
                .stake_voted_subtree(&(2, bank2.hash()))
                .unwrap()
        );

        let (heaviest_bank, _) = heaviest_subtree_fork_choice.select_forks(
            &frozen_banks,
            &tower,
            &vote_simulator.progress,
            &ancestors,
            &vote_simulator.bank_forks,
        );

        // Should pick the lower of the two equally weighted banks
        assert_eq!(heaviest_bank.slot(), 1);
    }

    #[test]
    fn test_child_bank_heavier() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(1);
        let my_node_pubkey = vote_simulator.node_pubkeys[0];
        let mut tower = Tower::default();

        // Create the tree of banks in a BankForks object
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3))));

        // Set the voting behavior
        let mut cluster_votes = HashMap::new();
        let votes = vec![2];
        cluster_votes.insert(my_node_pubkey, votes.clone());
        vote_simulator.fill_bank_forks(forks, &cluster_votes, true);

        // Fill banks with votes
        for vote in votes {
            assert!(vote_simulator
                .simulate_vote(vote, &my_node_pubkey, &mut tower,)
                .is_empty());
        }

        let mut frozen_banks: Vec<_> = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        let my_vote_pubkey = vote_simulator.vote_pubkeys[0];
        ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &vote_simulator.bank_forks.read().unwrap().ancestors(),
            &mut frozen_banks,
            &mut tower,
            &mut vote_simulator.progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &vote_simulator.bank_forks,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
        );

        frozen_banks.sort_by_key(|bank| bank.slot());
        for pair in frozen_banks.windows(2) {
            let first = vote_simulator
                .progress
                .get_fork_stats(pair[0].slot())
                .unwrap()
                .fork_weight();
            let second = vote_simulator
                .progress
                .get_fork_stats(pair[1].slot())
                .unwrap()
                .fork_weight();
            assert!(second >= first);
        }
        for bank in frozen_banks {
            // The only leaf should always be chosen over parents
            assert_eq!(
                vote_simulator
                    .tbft_structs
                    .heaviest_subtree_fork_choice
                    .best_slot(&(bank.slot(), bank.hash()))
                    .unwrap()
                    .0,
                3
            );
        }
    }

    #[test]
    fn test_should_retransmit() {
        let poh_slot = 4;
        let mut last_retransmit_slot = 4;
        // We retransmitted already at slot 4, shouldn't retransmit until
        // >= 4 + NUM_CONSECUTIVE_LEADER_SLOTS, or if we reset to < 4
        assert!(!ReplayStage::should_retransmit(
            poh_slot,
            &mut last_retransmit_slot
        ));
        assert_eq!(last_retransmit_slot, 4);

        for poh_slot in 4..4 + NUM_CONSECUTIVE_LEADER_SLOTS {
            assert!(!ReplayStage::should_retransmit(
                poh_slot,
                &mut last_retransmit_slot
            ));
            assert_eq!(last_retransmit_slot, 4);
        }

        let poh_slot = 4 + NUM_CONSECUTIVE_LEADER_SLOTS;
        last_retransmit_slot = 4;
        assert!(ReplayStage::should_retransmit(
            poh_slot,
            &mut last_retransmit_slot
        ));
        assert_eq!(last_retransmit_slot, poh_slot);

        let poh_slot = 3;
        last_retransmit_slot = 4;
        assert!(ReplayStage::should_retransmit(
            poh_slot,
            &mut last_retransmit_slot
        ));
        assert_eq!(last_retransmit_slot, poh_slot);
    }

    #[test]
    fn test_update_slot_propagated_threshold_from_votes() {
        let keypairs: HashMap<_, _> = iter::repeat_with(|| {
            let vote_keypairs = ValidatorVoteKeypairs::new_rand();
            (vote_keypairs.node_keypair.pubkey(), vote_keypairs)
        })
        .take(10)
        .collect();

        let new_vote_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.vote_keypair.pubkey())
            .collect();
        let new_node_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.node_keypair.pubkey())
            .collect();

        // Once 4/10 validators have voted, we have hit threshold
        run_test_update_slot_propagated_threshold_from_votes(&keypairs, &new_vote_pubkeys, &[], 4);
        // Adding the same node pubkey's instead of the corresponding
        // vote pubkeys should be equivalent
        run_test_update_slot_propagated_threshold_from_votes(&keypairs, &[], &new_node_pubkeys, 4);
        // Adding the same node pubkey's in the same order as their
        // corresponding vote accounts is redundant, so we don't
        // reach the threshold any sooner.
        run_test_update_slot_propagated_threshold_from_votes(
            &keypairs,
            &new_vote_pubkeys,
            &new_node_pubkeys,
            4,
        );
        // However, if we add different node pubkey's than the
        // vote accounts, we should hit threshold much faster
        // because now we are getting 2 new pubkeys on each
        // iteration instead of 1, so by the 2nd iteration
        // we should have 4/10 validators voting
        run_test_update_slot_propagated_threshold_from_votes(
            &keypairs,
            &new_vote_pubkeys[0..5],
            &new_node_pubkeys[5..],
            2,
        );
    }

    fn run_test_update_slot_propagated_threshold_from_votes(
        all_keypairs: &HashMap<Pubkey, ValidatorVoteKeypairs>,
        new_vote_pubkeys: &[Pubkey],
        new_node_pubkeys: &[Pubkey],
        success_index: usize,
    ) {
        let stake = 10_000;
        let (bank_forks, _, _) = vote_simulator::initialize_state(all_keypairs, stake);
        let root_bank = bank_forks.read().unwrap().root_bank();
        let mut propagated_stats = PropagatedStats {
            total_epoch_stake: stake * all_keypairs.len() as u64,
            ..PropagatedStats::default()
        };

        let child_reached_threshold = false;
        for i in 0..std::cmp::max(new_vote_pubkeys.len(), new_node_pubkeys.len()) {
            propagated_stats.is_propagated = false;
            let len = std::cmp::min(i, new_vote_pubkeys.len());
            let mut voted_pubkeys = new_vote_pubkeys[..len].to_vec();
            let len = std::cmp::min(i, new_node_pubkeys.len());
            let mut node_pubkeys = new_node_pubkeys[..len].to_vec();
            let did_newly_reach_threshold =
                ReplayStage::update_slot_propagated_threshold_from_votes(
                    &mut voted_pubkeys,
                    &mut node_pubkeys,
                    &root_bank,
                    &mut propagated_stats,
                    child_reached_threshold,
                );

            // Only the i'th voted pubkey should be new (everything else was
            // inserted in previous iteration of the loop), so those redundant
            // pubkeys should have been filtered out
            let remaining_vote_pubkeys = {
                if i == 0 || i >= new_vote_pubkeys.len() {
                    vec![]
                } else {
                    vec![new_vote_pubkeys[i - 1]]
                }
            };
            let remaining_node_pubkeys = {
                if i == 0 || i >= new_node_pubkeys.len() {
                    vec![]
                } else {
                    vec![new_node_pubkeys[i - 1]]
                }
            };
            assert_eq!(voted_pubkeys, remaining_vote_pubkeys);
            assert_eq!(node_pubkeys, remaining_node_pubkeys);

            // If we crossed the superminority threshold, then
            // `did_newly_reach_threshold == true`, otherwise the
            // threshold has not been reached
            if i >= success_index {
                assert!(propagated_stats.is_propagated);
                assert!(did_newly_reach_threshold);
            } else {
                assert!(!propagated_stats.is_propagated);
                assert!(!did_newly_reach_threshold);
            }
        }
    }

    #[test]
    fn test_update_slot_propagated_threshold_from_votes2() {
        let mut empty: Vec<Pubkey> = vec![];
        let genesis_config = create_genesis_config(100_000_000).genesis_config;
        let root_bank = Bank::new_for_tests(&genesis_config);
        let stake = 10_000;
        // Simulate a child slot seeing threshold (`child_reached_threshold` = true),
        // then the parent should also be marked as having reached threshold,
        // even if there are no new pubkeys to add (`newly_voted_pubkeys.is_empty()`)
        let mut propagated_stats = PropagatedStats {
            total_epoch_stake: stake * 10,
            ..PropagatedStats::default()
        };
        propagated_stats.total_epoch_stake = stake * 10;
        let child_reached_threshold = true;
        let mut newly_voted_pubkeys: Vec<Pubkey> = vec![];

        assert!(ReplayStage::update_slot_propagated_threshold_from_votes(
            &mut newly_voted_pubkeys,
            &mut empty,
            &root_bank,
            &mut propagated_stats,
            child_reached_threshold,
        ));

        // If propagation already happened (propagated_stats.is_propagated = true),
        // always returns false
        propagated_stats = PropagatedStats {
            total_epoch_stake: stake * 10,
            ..PropagatedStats::default()
        };
        propagated_stats.is_propagated = true;
        newly_voted_pubkeys = vec![];
        assert!(!ReplayStage::update_slot_propagated_threshold_from_votes(
            &mut newly_voted_pubkeys,
            &mut empty,
            &root_bank,
            &mut propagated_stats,
            child_reached_threshold,
        ));

        let child_reached_threshold = false;
        assert!(!ReplayStage::update_slot_propagated_threshold_from_votes(
            &mut newly_voted_pubkeys,
            &mut empty,
            &root_bank,
            &mut propagated_stats,
            child_reached_threshold,
        ));
    }

    #[test]
    fn test_update_propagation_status() {
        // Create genesis stakers
        let vote_keypairs = ValidatorVoteKeypairs::new_rand();
        let node_pubkey = vote_keypairs.node_keypair.pubkey();
        let vote_pubkey = vote_keypairs.vote_keypair.pubkey();
        let keypairs: HashMap<_, _> = vec![(node_pubkey, vote_keypairs)].into_iter().collect();
        let stake = 10_000;
        let (bank_forks_arc, mut progress_map, _) =
            vote_simulator::initialize_state(&keypairs, stake);
        let mut bank_forks = bank_forks_arc.write().unwrap();

        let bank0 = bank_forks.get(0).unwrap();
        bank_forks.insert(Bank::new_from_parent(bank0.clone(), &Pubkey::default(), 9));
        let bank9 = bank_forks.get(9).unwrap();
        bank_forks.insert(Bank::new_from_parent(bank9, &Pubkey::default(), 10));
        bank_forks
            .set_root(9, &AbsRequestSender::default(), None)
            .unwrap();
        let total_epoch_stake = bank0.total_epoch_stake();

        // Insert new ForkProgress for slot 10 and its
        // previous leader slot 9
        progress_map.insert(
            10,
            ForkProgress::new(
                Hash::default(),
                Some(9),
                Some(ValidatorStakeInfo {
                    total_epoch_stake,
                    ..ValidatorStakeInfo::default()
                }),
                0,
                0,
            ),
        );
        progress_map.insert(
            9,
            ForkProgress::new(
                Hash::default(),
                Some(8),
                Some(ValidatorStakeInfo {
                    total_epoch_stake,
                    ..ValidatorStakeInfo::default()
                }),
                0,
                0,
            ),
        );

        // Make sure is_propagated == false so that the propagation logic
        // runs in `update_propagation_status`
        assert!(!progress_map.get_leader_propagation_slot_must_exist(10).0);

        drop(bank_forks);

        let vote_tracker = VoteTracker::default();
        vote_tracker.insert_vote(10, vote_pubkey);
        ReplayStage::update_propagation_status(
            &mut progress_map,
            10,
            &bank_forks_arc,
            &vote_tracker,
            &ClusterSlots::default(),
        );

        let propagated_stats = &progress_map.get(&10).unwrap().propagated_stats;

        // There should now be a cached reference to the VoteTracker for
        // slot 10
        assert!(propagated_stats.slot_vote_tracker.is_some());

        // Updates should have been consumed
        assert!(propagated_stats
            .slot_vote_tracker
            .as_ref()
            .unwrap()
            .write()
            .unwrap()
            .get_voted_slot_updates()
            .is_none());

        // The voter should be recorded
        assert!(propagated_stats
            .propagated_validators
            .contains(&vote_pubkey));

        assert_eq!(propagated_stats.propagated_validators_stake, stake);
    }

    #[test]
    fn test_chain_update_propagation_status() {
        let keypairs: HashMap<_, _> = iter::repeat_with(|| {
            let vote_keypairs = ValidatorVoteKeypairs::new_rand();
            (vote_keypairs.node_keypair.pubkey(), vote_keypairs)
        })
        .take(10)
        .collect();

        let vote_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.vote_keypair.pubkey())
            .collect();

        let stake_per_validator = 10_000;
        let (bank_forks_arc, mut progress_map, _) =
            vote_simulator::initialize_state(&keypairs, stake_per_validator);
        let mut bank_forks = bank_forks_arc.write().unwrap();
        progress_map
            .get_propagated_stats_mut(0)
            .unwrap()
            .is_leader_slot = true;
        bank_forks
            .set_root(0, &AbsRequestSender::default(), None)
            .unwrap();
        let total_epoch_stake = bank_forks.root_bank().total_epoch_stake();

        // Insert new ForkProgress representing a slot for all slots 1..=num_banks. Only
        // make even numbered ones leader slots
        for i in 1..=10 {
            let parent_bank = bank_forks.get(i - 1).unwrap().clone();
            let prev_leader_slot = ((i - 1) / 2) * 2;
            bank_forks.insert(Bank::new_from_parent(parent_bank, &Pubkey::default(), i));
            progress_map.insert(
                i,
                ForkProgress::new(
                    Hash::default(),
                    Some(prev_leader_slot),
                    {
                        if i % 2 == 0 {
                            Some(ValidatorStakeInfo {
                                total_epoch_stake,
                                ..ValidatorStakeInfo::default()
                            })
                        } else {
                            None
                        }
                    },
                    0,
                    0,
                ),
            );
        }

        let vote_tracker = VoteTracker::default();
        for vote_pubkey in &vote_pubkeys {
            // Insert a vote for the last bank for each voter
            vote_tracker.insert_vote(10, *vote_pubkey);
        }

        drop(bank_forks);

        // The last bank should reach propagation threshold, and propagate it all
        // the way back through earlier leader banks
        ReplayStage::update_propagation_status(
            &mut progress_map,
            10,
            &bank_forks_arc,
            &vote_tracker,
            &ClusterSlots::default(),
        );

        for i in 1..=10 {
            let propagated_stats = &progress_map.get(&i).unwrap().propagated_stats;
            // Only the even numbered ones were leader banks, so only
            // those should have been updated
            if i % 2 == 0 {
                assert!(propagated_stats.is_propagated);
            } else {
                assert!(!propagated_stats.is_propagated);
            }
        }
    }

    #[test]
    fn test_chain_update_propagation_status2() {
        let num_validators = 6;
        let keypairs: HashMap<_, _> = iter::repeat_with(|| {
            let vote_keypairs = ValidatorVoteKeypairs::new_rand();
            (vote_keypairs.node_keypair.pubkey(), vote_keypairs)
        })
        .take(num_validators)
        .collect();

        let vote_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.vote_keypair.pubkey())
            .collect();

        let stake_per_validator = 10_000;
        let (bank_forks_arc, mut progress_map, _) =
            vote_simulator::initialize_state(&keypairs, stake_per_validator);
        let mut bank_forks = bank_forks_arc.write().unwrap();
        progress_map
            .get_propagated_stats_mut(0)
            .unwrap()
            .is_leader_slot = true;
        bank_forks
            .set_root(0, &AbsRequestSender::default(), None)
            .unwrap();

        let total_epoch_stake = num_validators as u64 * stake_per_validator;

        // Insert new ForkProgress representing a slot for all slots 1..=num_banks. Only
        // make even numbered ones leader slots
        for i in 1..=10 {
            let parent_bank = bank_forks.get(i - 1).unwrap().clone();
            let prev_leader_slot = i - 1;
            bank_forks.insert(Bank::new_from_parent(parent_bank, &Pubkey::default(), i));
            let mut fork_progress = ForkProgress::new(
                Hash::default(),
                Some(prev_leader_slot),
                Some(ValidatorStakeInfo {
                    total_epoch_stake,
                    ..ValidatorStakeInfo::default()
                }),
                0,
                0,
            );

            let end_range = {
                // The earlier slots are one pubkey away from reaching confirmation
                if i < 5 {
                    2
                } else {
                    // The later slots are two pubkeys away from reaching confirmation
                    1
                }
            };
            fork_progress.propagated_stats.propagated_validators =
                vote_pubkeys[0..end_range].iter().copied().collect();
            fork_progress.propagated_stats.propagated_validators_stake =
                end_range as u64 * stake_per_validator;
            progress_map.insert(i, fork_progress);
        }

        let vote_tracker = VoteTracker::default();
        // Insert a new vote
        vote_tracker.insert_vote(10, vote_pubkeys[2]);

        drop(bank_forks);
        // The last bank should reach propagation threshold, and propagate it all
        // the way back through earlier leader banks
        ReplayStage::update_propagation_status(
            &mut progress_map,
            10,
            &bank_forks_arc,
            &vote_tracker,
            &ClusterSlots::default(),
        );

        // Only the first 5 banks should have reached the threshold
        for i in 1..=10 {
            let propagated_stats = &progress_map.get(&i).unwrap().propagated_stats;
            if i < 5 {
                assert!(propagated_stats.is_propagated);
            } else {
                assert!(!propagated_stats.is_propagated);
            }
        }
    }

    #[test]
    fn test_check_propagation_for_start_leader() {
        let mut progress_map = ProgressMap::default();
        let poh_slot = 5;
        let parent_slot = poh_slot - NUM_CONSECUTIVE_LEADER_SLOTS;

        // If there is no previous leader slot (previous leader slot is None),
        // should succeed
        progress_map.insert(
            parent_slot,
            ForkProgress::new(Hash::default(), None, None, 0, 0),
        );
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // Now if we make the parent was itself the leader, then requires propagation
        // confirmation check because the parent is at least NUM_CONSECUTIVE_LEADER_SLOTS
        // slots from the `poh_slot`
        progress_map.insert(
            parent_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );
        assert!(!ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
        progress_map
            .get_mut(&parent_slot)
            .unwrap()
            .propagated_stats
            .is_propagated = true;
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
        // Now, set up the progress map to show that the `previous_leader_slot` of 5 is
        // `parent_slot - 1` (not equal to the actual parent!), so `parent_slot - 1` needs
        // to see propagation confirmation before we can start a leader for block 5
        let previous_leader_slot = parent_slot - 1;
        progress_map.insert(
            parent_slot,
            ForkProgress::new(Hash::default(), Some(previous_leader_slot), None, 0, 0),
        );
        progress_map.insert(
            previous_leader_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );

        // `previous_leader_slot` has not seen propagation threshold, so should fail
        assert!(!ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // If we set the is_propagated = true for the `previous_leader_slot`, should
        // allow the block to be generated
        progress_map
            .get_mut(&previous_leader_slot)
            .unwrap()
            .propagated_stats
            .is_propagated = true;
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // If the root is now set to `parent_slot`, this filters out `previous_leader_slot` from the progress map,
        // which implies confirmation
        let bank0 = Bank::new_for_tests(&genesis_config::create_genesis_config(10000).0);
        let parent_slot_bank =
            Bank::new_from_parent(Arc::new(bank0), &Pubkey::default(), parent_slot);
        let bank_forks = BankForks::new_rw_arc(parent_slot_bank);
        let mut bank_forks = bank_forks.write().unwrap();
        let bank5 =
            Bank::new_from_parent(bank_forks.get(parent_slot).unwrap(), &Pubkey::default(), 5);
        bank_forks.insert(bank5);

        // Should purge only `previous_leader_slot` from the progress map
        progress_map.handle_new_root(&bank_forks);

        // Should succeed
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
    }

    #[test]
    fn test_check_propagation_skip_propagation_check() {
        let mut progress_map = ProgressMap::default();
        let poh_slot = 4;
        let mut parent_slot = poh_slot - 1;

        // Set up the progress map to show that the last leader slot of 4 is 3,
        // which means 3 and 4 are consecutive leader slots
        progress_map.insert(
            3,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );

        // If the previous leader slot has not seen propagation threshold, but
        // was the direct parent (implying consecutive leader slots), create
        // the block regardless
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // If propagation threshold was achieved on parent, block should
        // also be created
        progress_map
            .get_mut(&3)
            .unwrap()
            .propagated_stats
            .is_propagated = true;
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // Now insert another parent slot 2 for which this validator is also the leader
        parent_slot = poh_slot - NUM_CONSECUTIVE_LEADER_SLOTS + 1;
        progress_map.insert(
            parent_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );

        // Even though `parent_slot` and `poh_slot` are separated by another block,
        // because they're within `NUM_CONSECUTIVE` blocks of each other, the propagation
        // check is still skipped
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // Once the distance becomes >= NUM_CONSECUTIVE_LEADER_SLOTS, then we need to
        // enforce the propagation check
        parent_slot = poh_slot - NUM_CONSECUTIVE_LEADER_SLOTS;
        progress_map.insert(
            parent_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );
        assert!(!ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
    }

    #[test]
    fn test_purge_unconfirmed_duplicate_slot() {
        let (vote_simulator, blockstore) = setup_default_forks(2, None::<GenerateVotes>);
        let VoteSimulator {
            bank_forks,
            node_pubkeys,
            mut progress,
            validator_keypairs,
            ..
        } = vote_simulator;

        // Create bank 7
        let root_bank = bank_forks.read().unwrap().root_bank();
        let bank7 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(6).unwrap(),
            &Pubkey::default(),
            7,
        );
        bank_forks.write().unwrap().insert(bank7);
        blockstore.add_tree(tr(6) / tr(7), false, false, 3, Hash::default());
        let bank7 = bank_forks.read().unwrap().get(7).unwrap();
        let mut descendants = bank_forks.read().unwrap().descendants();
        let mut ancestors = bank_forks.read().unwrap().ancestors();

        // Process a transfer on bank 7
        let sender = node_pubkeys[0];
        let receiver = node_pubkeys[1];
        let old_balance = bank7.get_balance(&sender);
        let transfer_amount = old_balance / 2;
        let transfer_sig = bank7
            .transfer(
                transfer_amount,
                &validator_keypairs.get(&sender).unwrap().node_keypair,
                &receiver,
            )
            .unwrap();

        // Process a vote for slot 0 in bank 5
        let validator0_keypairs = &validator_keypairs.get(&sender).unwrap();
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();
        let tower_sync = TowerSync::new_from_slots(vec![0], bank0.hash(), None);
        let vote_tx = vote_transaction::new_tower_sync_transaction(
            tower_sync,
            bank0.last_blockhash(),
            &validator0_keypairs.node_keypair,
            &validator0_keypairs.vote_keypair,
            &validator0_keypairs.vote_keypair,
            None,
        );
        bank7.process_transaction(&vote_tx).unwrap();
        assert!(bank7.get_signature_status(&vote_tx.signatures[0]).is_some());

        // Both signatures should exist in status cache
        assert!(bank7.get_signature_status(&vote_tx.signatures[0]).is_some());
        assert!(bank7.get_signature_status(&transfer_sig).is_some());

        // Give all slots a bank hash but mark slot 7 dead
        for i in 0..=6 {
            blockstore.insert_bank_hash(i, Hash::new_unique(), false);
        }
        blockstore
            .set_dead_slot(7)
            .expect("Failed to mark slot as dead in blockstore");

        // Purging slot 5 should purge only slots 5 and its descendant 6. Since 7 is already dead,
        // it gets reset but not removed
        ReplayStage::purge_unconfirmed_duplicate_slot(
            5,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            &root_bank,
            &bank_forks,
            &blockstore,
        );
        for i in 5..=7 {
            assert!(bank_forks.read().unwrap().get(i).is_none());
            assert!(progress.get(&i).is_none());
        }
        for i in 0..=4 {
            assert!(bank_forks.read().unwrap().get(i).is_some());
            assert!(progress.get(&i).is_some());
        }

        // Blockstore should have been cleared
        for slot in &[5, 6] {
            assert!(!blockstore.is_full(*slot));
            assert!(!blockstore.is_dead(*slot));
            assert!(blockstore.get_slot_entries(*slot, 0).unwrap().is_empty());
        }

        // Slot 7 was marked dead before, should no longer be marked
        assert!(!blockstore.is_dead(7));
        assert!(!blockstore.get_slot_entries(7, 0).unwrap().is_empty());

        // Should not be able to find signature in slot 5 for previously
        // processed transactions
        assert!(bank7.get_signature_status(&vote_tx.signatures[0]).is_none());
        assert!(bank7.get_signature_status(&transfer_sig).is_none());

        // Getting balance should return the old balance (accounts were cleared)
        assert_eq!(bank7.get_balance(&sender), old_balance);

        // Purging slot 4 should purge only slot 4
        let mut descendants = bank_forks.read().unwrap().descendants();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        ReplayStage::purge_unconfirmed_duplicate_slot(
            4,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            &root_bank,
            &bank_forks,
            &blockstore,
        );
        for i in 4..=6 {
            assert!(bank_forks.read().unwrap().get(i).is_none());
            assert!(progress.get(&i).is_none());
            assert!(blockstore.get_slot_entries(i, 0).unwrap().is_empty());
        }
        for i in 0..=3 {
            assert!(bank_forks.read().unwrap().get(i).is_some());
            assert!(progress.get(&i).is_some());
            assert!(!blockstore.get_slot_entries(i, 0).unwrap().is_empty());
        }

        // Purging slot 1 should purge both forks 2 and 3 but leave 7 untouched as it is dead
        let mut descendants = bank_forks.read().unwrap().descendants();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        ReplayStage::purge_unconfirmed_duplicate_slot(
            1,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            &root_bank,
            &bank_forks,
            &blockstore,
        );
        for i in 1..=6 {
            assert!(bank_forks.read().unwrap().get(i).is_none());
            assert!(progress.get(&i).is_none());
            assert!(blockstore.get_slot_entries(i, 0).unwrap().is_empty());
        }
        assert!(bank_forks.read().unwrap().get(0).is_some());
        assert!(progress.get(&0).is_some());

        // Slot 7 untouched
        assert!(!blockstore.is_dead(7));
        assert!(!blockstore.get_slot_entries(7, 0).unwrap().is_empty());
    }

    #[test]
    fn test_purge_unconfirmed_duplicate_slots_and_reattach() {
        let ReplayBlockstoreComponents {
            blockstore,
            validator_node_to_vote_keys,
            vote_simulator,
            leader_schedule_cache,
            rpc_subscriptions,
            ..
        } = replay_blockstore_components(
            Some(tr(0) / (tr(1) / (tr(2) / (tr(4))) / (tr(3) / (tr(5) / (tr(6)))))),
            1,
            None::<GenerateVotes>,
        );

        let VoteSimulator {
            bank_forks,
            mut progress,
            ..
        } = vote_simulator;

        let mut replay_timing = ReplayLoopTiming::default();

        // Create bank 7 and insert to blockstore and bank forks
        let root_bank = bank_forks.read().unwrap().root_bank();
        let bank7 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(6).unwrap(),
            &Pubkey::default(),
            7,
        );
        bank_forks.write().unwrap().insert(bank7);
        blockstore.add_tree(tr(6) / tr(7), false, false, 3, Hash::default());
        let mut descendants = bank_forks.read().unwrap().descendants();
        let mut ancestors = bank_forks.read().unwrap().ancestors();

        // Mark earlier slots as frozen, but we have the wrong version of slots 3 and 5, so slot 6 is dead and
        // slot 7 is unreplayed
        for i in 0..=5 {
            blockstore.insert_bank_hash(i, Hash::new_unique(), false);
        }
        blockstore
            .set_dead_slot(6)
            .expect("Failed to mark slot 6 as dead in blockstore");

        // Purge slot 3 as it is duplicate, this should also purge slot 5 but not touch 6 and 7
        ReplayStage::purge_unconfirmed_duplicate_slot(
            3,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            &root_bank,
            &bank_forks,
            &blockstore,
        );
        for slot in &[3, 5, 6, 7] {
            assert!(bank_forks.read().unwrap().get(*slot).is_none());
            assert!(progress.get(slot).is_none());
        }
        for slot in &[3, 5] {
            assert!(!blockstore.is_full(*slot));
            assert!(!blockstore.is_dead(*slot));
            assert!(blockstore.get_slot_entries(*slot, 0).unwrap().is_empty());
        }
        for slot in 6..=7 {
            assert!(!blockstore.is_dead(slot));
            assert!(!blockstore.get_slot_entries(slot, 0).unwrap().is_empty())
        }

        // Simulate repair fixing slot 3 and 5
        let (shreds, _) = make_slot_entries(
            3,    // slot
            1,    // parent_slot
            8,    // num_entries
            true, // merkle_variant
        );
        blockstore.insert_shreds(shreds, None, false).unwrap();
        let (shreds, _) = make_slot_entries(
            5,    // slot
            3,    // parent_slot
            8,    // num_entries
            true, // merkle_variant
        );
        blockstore.insert_shreds(shreds, None, false).unwrap();

        // 3 should now be an active bank
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &mut replay_timing,
        );
        assert_eq!(bank_forks.read().unwrap().active_bank_slots(), vec![3]);

        // Freeze 3
        {
            let bank3 = bank_forks.read().unwrap().get(3).unwrap();
            progress.insert(
                3,
                ForkProgress::new_from_bank(
                    &bank3,
                    bank3.collector_id(),
                    validator_node_to_vote_keys
                        .get(bank3.collector_id())
                        .unwrap(),
                    Some(1),
                    0,
                    0,
                ),
            );
            bank3.freeze();
        }
        // 5 Should now be an active bank
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &mut replay_timing,
        );
        assert_eq!(bank_forks.read().unwrap().active_bank_slots(), vec![5]);

        // Freeze 5
        {
            let bank5 = bank_forks.read().unwrap().get(5).unwrap();
            progress.insert(
                5,
                ForkProgress::new_from_bank(
                    &bank5,
                    bank5.collector_id(),
                    validator_node_to_vote_keys
                        .get(bank5.collector_id())
                        .unwrap(),
                    Some(3),
                    0,
                    0,
                ),
            );
            bank5.freeze();
        }
        // 6 should now be an active bank even though we haven't repaired it because it
        // wasn't dumped
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &mut replay_timing,
        );
        assert_eq!(bank_forks.read().unwrap().active_bank_slots(), vec![6]);

        // Freeze 6 now that we have the correct version of 5.
        {
            let bank6 = bank_forks.read().unwrap().get(6).unwrap();
            progress.insert(
                6,
                ForkProgress::new_from_bank(
                    &bank6,
                    bank6.collector_id(),
                    validator_node_to_vote_keys
                        .get(bank6.collector_id())
                        .unwrap(),
                    Some(5),
                    0,
                    0,
                ),
            );
            bank6.freeze();
        }
        // 7 should be found as an active bank
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &mut replay_timing,
        );
        assert_eq!(bank_forks.read().unwrap().active_bank_slots(), vec![7]);
    }

    #[test]
    fn test_purge_ancestors_descendants() {
        let (VoteSimulator { bank_forks, .. }, _) = setup_default_forks(1, None::<GenerateVotes>);

        // Purge branch rooted at slot 2
        let mut descendants = bank_forks.read().unwrap().descendants();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        let slot_2_descendants = descendants.get(&2).unwrap().clone();
        ReplayStage::purge_ancestors_descendants(
            2,
            &slot_2_descendants,
            &mut ancestors,
            &mut descendants,
        );

        // Result should be equivalent to removing slot from BankForks
        // and regenerating the `ancestor` `descendant` maps
        for d in slot_2_descendants {
            bank_forks.write().unwrap().remove(d);
        }
        bank_forks.write().unwrap().remove(2);
        assert!(check_map_eq(
            &ancestors,
            &bank_forks.read().unwrap().ancestors()
        ));
        assert!(check_map_eq(
            &descendants,
            &bank_forks.read().unwrap().descendants()
        ));

        // Try to purge the root
        bank_forks
            .write()
            .unwrap()
            .set_root(3, &AbsRequestSender::default(), None)
            .unwrap();
        let mut descendants = bank_forks.read().unwrap().descendants();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        let slot_3_descendants = descendants.get(&3).unwrap().clone();
        ReplayStage::purge_ancestors_descendants(
            3,
            &slot_3_descendants,
            &mut ancestors,
            &mut descendants,
        );

        assert!(ancestors.is_empty());
        // Only remaining keys should be ones < root
        for k in descendants.keys() {
            assert!(*k < 3);
        }
    }

    #[test]
    fn test_leader_snapshot_restart_propagation() {
        let ReplayBlockstoreComponents {
            validator_node_to_vote_keys,
            leader_schedule_cache,
            vote_simulator,
            ..
        } = replay_blockstore_components(None, 1, None::<GenerateVotes>);

        let VoteSimulator {
            mut progress,
            bank_forks,
            ..
        } = vote_simulator;

        let root_bank = bank_forks.read().unwrap().root_bank();
        let my_pubkey = leader_schedule_cache
            .slot_leader_at(root_bank.slot(), Some(&root_bank))
            .unwrap();

        // Check that we are the leader of the root bank
        assert!(
            progress
                .get_propagated_stats(root_bank.slot())
                .unwrap()
                .is_leader_slot
        );
        let ancestors = bank_forks.read().unwrap().ancestors();

        // Freeze bank so it shows up in frozen banks
        root_bank.freeze();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        // Compute bank stats, make sure vote is propagated back to starting root bank
        let vote_tracker = VoteTracker::default();

        // Add votes
        for vote_key in validator_node_to_vote_keys.values() {
            vote_tracker.insert_vote(root_bank.slot(), *vote_key);
        }

        assert!(
            !progress
                .get_leader_propagation_slot_must_exist(root_bank.slot())
                .0
        );

        // Update propagation status
        let mut tower = Tower::new_for_tests(0, 0.67);
        ReplayStage::compute_bank_stats(
            &validator_node_to_vote_keys[&my_pubkey],
            &ancestors,
            &mut frozen_banks,
            &mut tower,
            &mut progress,
            &vote_tracker,
            &ClusterSlots::default(),
            &bank_forks,
            &mut HeaviestSubtreeForkChoice::new_from_bank_forks(bank_forks.clone()),
            &mut LatestValidatorVotesForFrozenBanks::default(),
        );

        // Check status is true
        assert!(
            progress
                .get_leader_propagation_slot_must_exist(root_bank.slot())
                .0
        );
    }

    #[test]
    fn test_unconfirmed_duplicate_slots_and_lockouts_for_non_heaviest_fork() {
        /*
            Build fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |      |
            slot 3    |
               |      |
            slot 4    |
                    slot 5
        */
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3) / (tr(4)))) / tr(5));

        let mut vote_simulator = VoteSimulator::new(1);
        vote_simulator.fill_bank_forks(forks, &HashMap::<Pubkey, Vec<u64>>::new(), true);
        let (bank_forks, mut progress) = (vote_simulator.bank_forks, vote_simulator.progress);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let mut tower = Tower::new_for_tests(8, 2.0 / 3.0);

        // All forks have same weight so heaviest bank to vote/reset on should be the tip of
        // the fork with the lower slot
        let (vote_fork, reset_fork, _) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert_eq!(vote_fork.unwrap(), 4);
        assert_eq!(reset_fork.unwrap(), 4);

        // Record the vote for 5 which is not on the heaviest fork.
        tower.record_bank_vote(&bank_forks.read().unwrap().get(5).unwrap());

        // 4 should be the heaviest slot, but should not be votable
        // because of lockout. 5 is the heaviest slot on the same fork as the last vote.
        let (vote_fork, reset_fork, heaviest_fork_failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork, Some(5));
        assert_eq!(
            heaviest_fork_failures,
            vec![
                HeaviestForkFailures::FailedSwitchThreshold(4, 0, 10000),
                HeaviestForkFailures::LockedOut(4)
            ]
        );

        // Mark 5 as duplicate
        blockstore.store_duplicate_slot(5, vec![], vec![]).unwrap();
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut purge_repair_slot_counter = PurgeRepairSlotCounter::default();
        let mut duplicate_confirmed_slots = DuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let bank5_hash = bank_forks.read().unwrap().bank_hash(5).unwrap();
        assert_ne!(bank5_hash, Hash::default());
        let duplicate_state = DuplicateState::new_from_state(
            5,
            &duplicate_confirmed_slots,
            &vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            || progress.is_dead(5).unwrap_or(false),
            || Some(bank5_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            5,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut purge_repair_slot_counter,
            SlotStateUpdate::Duplicate(duplicate_state),
        );

        // 4 should be the heaviest slot, but should not be votable
        // because of lockout. 5 is no longer valid due to it being a duplicate, however we still
        // reset onto 5.
        let (vote_fork, reset_fork, heaviest_fork_failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork, Some(5));
        assert_eq!(
            heaviest_fork_failures,
            vec![
                HeaviestForkFailures::FailedSwitchThreshold(4, 0, 10000),
                HeaviestForkFailures::LockedOut(4)
            ]
        );

        // Continue building on 5
        let forks = tr(5) / (tr(6) / (tr(7) / (tr(8) / (tr(9)))) / tr(10));
        vote_simulator.bank_forks = bank_forks;
        vote_simulator.progress = progress;
        vote_simulator.fill_bank_forks(forks, &HashMap::<Pubkey, Vec<u64>>::new(), true);
        let (bank_forks, mut progress) = (vote_simulator.bank_forks, vote_simulator.progress);
        // 4 is still the heaviest slot, but not votable because of lockout.
        // 9 is the deepest slot from our last voted fork (5), so it is what we should
        // reset to.
        let (vote_fork, reset_fork, heaviest_fork_failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork, Some(9));
        assert_eq!(
            heaviest_fork_failures,
            vec![
                HeaviestForkFailures::FailedSwitchThreshold(4, 0, 10000),
                HeaviestForkFailures::LockedOut(4)
            ]
        );

        // If slot 5 is marked as confirmed, it becomes the heaviest bank on same slot again
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        duplicate_confirmed_slots.insert(5, bank5_hash);
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            bank5_hash,
            || progress.is_dead(5).unwrap_or(false),
            || Some(bank5_hash),
        );
        check_slot_agrees_with_cluster(
            5,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            &mut purge_repair_slot_counter,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );

        // The confirmed hash is detected in `progress`, which means
        // it's confirmation on the replayed block. This means we have
        // the right version of the block, so `duplicate_slots_to_repair`
        // should be empty
        assert!(duplicate_slots_to_repair.is_empty());

        // We should still reset to slot 9 as it's the heaviest on the now valid
        // fork.
        let (vote_fork, reset_fork, heaviest_fork_failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork.unwrap(), 9);
        assert_eq!(
            heaviest_fork_failures,
            vec![
                HeaviestForkFailures::FailedSwitchThreshold(4, 0, 10000),
                HeaviestForkFailures::LockedOut(4)
            ]
        );

        // Resetting our forks back to how it was should allow us to reset to our
        // last vote which was previously marked as invalid and now duplicate confirmed
        let bank6_hash = bank_forks.read().unwrap().bank_hash(6).unwrap();
        let _ = vote_simulator
            .tbft_structs
            .heaviest_subtree_fork_choice
            .split_off(&(6, bank6_hash));
        // Should now pick 5 as the heaviest fork from last vote again.
        let (vote_fork, reset_fork, heaviest_fork_failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork.unwrap(), 5);
        assert_eq!(
            heaviest_fork_failures,
            vec![
                HeaviestForkFailures::FailedSwitchThreshold(4, 0, 10000),
                HeaviestForkFailures::LockedOut(4)
            ]
        );
    }

    #[test]
    fn test_unconfirmed_duplicate_slots_and_lockouts() {
        /*
            Build fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |      |
            slot 3    |
               |      |
            slot 4    |
                    slot 5
                      |
                    slot 6
        */
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3) / (tr(4)))) / (tr(5) / (tr(6))));

        // Make enough validators for vote switch threshold later
        let mut vote_simulator = VoteSimulator::new(2);
        let validator_votes: HashMap<Pubkey, Vec<u64>> = vec![
            (vote_simulator.node_pubkeys[0], vec![5]),
            (vote_simulator.node_pubkeys[1], vec![2]),
        ]
        .into_iter()
        .collect();
        vote_simulator.fill_bank_forks(forks, &validator_votes, true);

        let (bank_forks, mut progress) = (vote_simulator.bank_forks, vote_simulator.progress);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let mut tower = Tower::new_for_tests(8, 0.67);

        // All forks have same weight so heaviest bank to vote/reset on should be the tip of
        // the fork with the lower slot
        let (vote_fork, reset_fork, _) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert_eq!(vote_fork.unwrap(), 4);
        assert_eq!(reset_fork.unwrap(), 4);

        // Record the vote for 4
        tower.record_bank_vote(&bank_forks.read().unwrap().get(4).unwrap());

        // Mark 4 as duplicate, 3 should be the heaviest slot, but should not be votable
        // because of lockout
        blockstore.store_duplicate_slot(4, vec![], vec![]).unwrap();
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut duplicate_confirmed_slots = DuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let bank4_hash = bank_forks.read().unwrap().bank_hash(4).unwrap();
        assert_ne!(bank4_hash, Hash::default());
        let duplicate_state = DuplicateState::new_from_state(
            4,
            &duplicate_confirmed_slots,
            &vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            || progress.is_dead(4).unwrap_or(false),
            || Some(bank4_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            4,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            SlotStateUpdate::Duplicate(duplicate_state),
        );

        let (vote_fork, reset_fork, _) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork, Some(3));

        // Now mark 2, an ancestor of 4, as duplicate
        blockstore.store_duplicate_slot(2, vec![], vec![]).unwrap();
        let bank2_hash = bank_forks.read().unwrap().bank_hash(2).unwrap();
        assert_ne!(bank2_hash, Hash::default());
        let duplicate_state = DuplicateState::new_from_state(
            2,
            &duplicate_confirmed_slots,
            &vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            || progress.is_dead(2).unwrap_or(false),
            || Some(bank2_hash),
        );
        check_slot_agrees_with_cluster(
            2,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            SlotStateUpdate::Duplicate(duplicate_state),
        );

        let (vote_fork, reset_fork, _) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );

        // Should now pick the next heaviest fork that is not a descendant of 2, which is 6.
        // However the lockout from vote 4 should still apply, so 6 should not be votable
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork.unwrap(), 6);

        // If slot 4 is marked as confirmed, then this confirms slot 2 and 4, and
        // then slot 4 is now the heaviest bank again
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        duplicate_confirmed_slots.insert(4, bank4_hash);
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            bank4_hash,
            || progress.is_dead(4).unwrap_or(false),
            || Some(bank4_hash),
        );
        check_slot_agrees_with_cluster(
            4,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        // The confirmed hash is detected in `progress`, which means
        // it's confirmation on the replayed block. This means we have
        // the right version of the block, so `duplicate_slots_to_repair`
        // should be empty
        assert!(duplicate_slots_to_repair.is_empty());
        let (vote_fork, reset_fork, _) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            None,
        );
        // Should now pick the heaviest fork 4 again, but lockouts apply so fork 4
        // is not votable, which avoids voting for 4 again.
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork.unwrap(), 4);
    }

    #[test]
    fn test_dump_then_repair_correct_slots() {
        // Create the tree of banks in a BankForks object
        let forks = tr(0) / (tr(1)) / (tr(2));

        let ReplayBlockstoreComponents {
            ref mut vote_simulator,
            ref blockstore,
            ref leader_schedule_cache,
            ..
        } = replay_blockstore_components(Some(forks), 1, None);

        let VoteSimulator {
            ref mut progress,
            ref bank_forks,
            ..
        } = vote_simulator;

        let (mut ancestors, mut descendants) = {
            let r_bank_forks = bank_forks.read().unwrap();
            (r_bank_forks.ancestors(), r_bank_forks.descendants())
        };

        // Insert different versions of both 1 and 2. Both slots 1 and 2 should
        // then be purged
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        duplicate_slots_to_repair.insert(1, Hash::new_unique());
        duplicate_slots_to_repair.insert(2, Hash::new_unique());
        let mut purge_repair_slot_counter = PurgeRepairSlotCounter::default();
        let (dumped_slots_sender, dumped_slots_receiver) = unbounded();
        let should_be_dumped = duplicate_slots_to_repair
            .iter()
            .map(|(&s, &h)| (s, h))
            .collect_vec();

        ReplayStage::dump_then_repair_correct_slots(
            &mut duplicate_slots_to_repair,
            &mut ancestors,
            &mut descendants,
            progress,
            bank_forks,
            blockstore,
            None,
            &mut purge_repair_slot_counter,
            &dumped_slots_sender,
            &Pubkey::new_unique(),
            leader_schedule_cache,
        );
        assert_eq!(should_be_dumped, dumped_slots_receiver.recv().ok().unwrap());

        let r_bank_forks = bank_forks.read().unwrap();
        for slot in 0..=2 {
            let bank = r_bank_forks.get(slot);
            let ancestor_result = ancestors.get(&slot);
            let descendants_result = descendants.get(&slot);
            if slot == 0 {
                assert!(bank.is_some());
                assert!(ancestor_result.is_some());
                assert!(descendants_result.is_some());
            } else {
                assert!(bank.is_none());
                assert!(ancestor_result.is_none());
                assert!(descendants_result.is_none());
            }
        }
        assert_eq!(2, purge_repair_slot_counter.len());
        assert_eq!(1, *purge_repair_slot_counter.get(&1).unwrap());
        assert_eq!(1, *purge_repair_slot_counter.get(&2).unwrap());
    }

    fn setup_vote_then_rollback(
        first_vote: Slot,
        num_validators: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> ReplayBlockstoreComponents {
        /*
            Build fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |      |
            slot 3    |
               |      |
            slot 4    |
               |      |
            slot 5    |
                    slot 6
                      |
                    slot 7
        */
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3) / (tr(4) / (tr(5))))) / (tr(6) / (tr(7))));

        let mut replay_components =
            replay_blockstore_components(Some(forks), num_validators, generate_votes);

        let ReplayBlockstoreComponents {
            ref mut tower,
            ref blockstore,
            ref mut vote_simulator,
            ref leader_schedule_cache,
            ..
        } = replay_components;

        let VoteSimulator {
            ref mut progress,
            ref bank_forks,
            ref mut tbft_structs,
            ..
        } = vote_simulator;

        tower.record_bank_vote(&bank_forks.read().unwrap().get(first_vote).unwrap());

        // Simulate another version of slot 2 was duplicate confirmed
        let our_bank2_hash = bank_forks.read().unwrap().bank_hash(2).unwrap();
        let duplicate_confirmed_bank2_hash = Hash::new_unique();
        let mut duplicate_confirmed_slots = DuplicateConfirmedSlots::default();
        duplicate_confirmed_slots.insert(2, duplicate_confirmed_bank2_hash);
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();

        // Mark fork choice branch as invalid so select forks below doesn't panic
        // on a nonexistent `heaviest_bank_on_same_fork` after we dump the duplicate fork.
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            duplicate_confirmed_bank2_hash,
            || progress.is_dead(2).unwrap_or(false),
            || Some(our_bank2_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            2,
            bank_forks.read().unwrap().root(),
            blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        assert_eq!(
            *duplicate_slots_to_repair.get(&2).unwrap(),
            duplicate_confirmed_bank2_hash
        );
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        let mut descendants = bank_forks.read().unwrap().descendants();
        let old_descendants_of_2 = descendants.get(&2).unwrap().clone();
        let (dumped_slots_sender, _dumped_slots_receiver) = unbounded();

        ReplayStage::dump_then_repair_correct_slots(
            &mut duplicate_slots_to_repair,
            &mut ancestors,
            &mut descendants,
            progress,
            bank_forks,
            blockstore,
            None,
            &mut PurgeRepairSlotCounter::default(),
            &dumped_slots_sender,
            &Pubkey::new_unique(),
            leader_schedule_cache,
        );

        // Check everything was purged properly
        for purged_slot in std::iter::once(&2).chain(old_descendants_of_2.iter()) {
            assert!(!ancestors.contains_key(purged_slot));
            assert!(!descendants.contains_key(purged_slot));
        }

        replay_components
    }

    fn run_test_duplicate_rollback_then_vote(first_vote: Slot) -> SelectVoteAndResetForkResult {
        let replay_components = setup_vote_then_rollback(
            first_vote,
            2,
            Some(Box::new(|node_keys| {
                // Simulate everyone else voting on 6, so we have enough to
                // make a switch to the other fork
                node_keys.into_iter().map(|k| (k, vec![6])).collect()
            })),
        );

        let ReplayBlockstoreComponents {
            mut tower,
            vote_simulator,
            ..
        } = replay_components;

        let VoteSimulator {
            mut progress,
            bank_forks,
            mut tbft_structs,
            mut latest_validator_votes_for_frozen_banks,
            ..
        } = vote_simulator;

        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        let ancestors = bank_forks.read().unwrap().ancestors();
        let descendants = bank_forks.read().unwrap().descendants();

        ReplayStage::compute_bank_stats(
            &Pubkey::new_unique(),
            &ancestors,
            &mut frozen_banks,
            &mut tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Try to switch to vote to the heaviest slot 6, then return the vote results
        let (heaviest_bank, heaviest_bank_on_same_fork) = tbft_structs
            .heaviest_subtree_fork_choice
            .select_forks(&frozen_banks, &tower, &progress, &ancestors, &bank_forks);
        assert_eq!(heaviest_bank.slot(), 7);
        assert!(heaviest_bank_on_same_fork.is_none());
        select_vote_and_reset_forks(
            &heaviest_bank,
            heaviest_bank_on_same_fork.as_ref(),
            &ancestors,
            &descendants,
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &tbft_structs.heaviest_subtree_fork_choice,
        )
    }

    #[test]
    fn test_duplicate_rollback_then_vote_locked_out() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote(5);

        // If we vote on 5 first then try to vote on 7, we should be locked out,
        // despite the rollback
        assert!(vote_bank.is_none());
        assert_eq!(reset_bank.unwrap().slot(), 7);
        assert_eq!(
            heaviest_fork_failures,
            vec![HeaviestForkFailures::LockedOut(7)]
        );
    }

    #[test]
    fn test_duplicate_rollback_then_vote_success() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote(4);

        // If we vote on 4 first then try to vote on 7, we should succeed
        assert_matches!(
            vote_bank
                .map(|(bank, switch_decision)| (bank.slot(), switch_decision))
                .unwrap(),
            (7, SwitchForkDecision::SwitchProof(_))
        );
        assert_eq!(reset_bank.unwrap().slot(), 7);
        assert!(heaviest_fork_failures.is_empty());
    }

    fn run_test_duplicate_rollback_then_vote_on_other_duplicate(
        first_vote: Slot,
    ) -> SelectVoteAndResetForkResult {
        let replay_components = setup_vote_then_rollback(first_vote, 10, None::<GenerateVotes>);

        let ReplayBlockstoreComponents {
            mut tower,
            mut vote_simulator,
            ..
        } = replay_components;

        // Simulate repairing an alternate version of slot 2, 3 and 4 that we just dumped. Because
        // we're including votes this time for slot 1, it should generate a different
        // version of 2.
        let cluster_votes: HashMap<Pubkey, Vec<Slot>> = vote_simulator
            .node_pubkeys
            .iter()
            .map(|k| (*k, vec![1, 2]))
            .collect();

        // Create new versions of slots 2, 3, 4, 5, with parent slot 1
        vote_simulator.create_and_vote_new_branch(
            1,
            5,
            &cluster_votes,
            &HashSet::new(),
            &Pubkey::new_unique(),
            &mut tower,
        );

        let VoteSimulator {
            mut progress,
            bank_forks,
            mut tbft_structs,
            mut latest_validator_votes_for_frozen_banks,
            ..
        } = vote_simulator;

        // Check that the new branch with slot 2 is different than the original version.
        let bank_1_hash = bank_forks.read().unwrap().bank_hash(1).unwrap();
        let children_of_1 = (&tbft_structs.heaviest_subtree_fork_choice)
            .children(&(1, bank_1_hash))
            .unwrap();
        let duplicate_versions_of_2 = children_of_1.filter(|(slot, _hash)| *slot == 2).count();
        assert_eq!(duplicate_versions_of_2, 2);

        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        let ancestors = bank_forks.read().unwrap().ancestors();
        let descendants = bank_forks.read().unwrap().descendants();

        ReplayStage::compute_bank_stats(
            &Pubkey::new_unique(),
            &ancestors,
            &mut frozen_banks,
            &mut tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );
        // Try to switch to vote to the heaviest slot 5, then return the vote results
        let (heaviest_bank, heaviest_bank_on_same_fork) = tbft_structs
            .heaviest_subtree_fork_choice
            .select_forks(&frozen_banks, &tower, &progress, &ancestors, &bank_forks);
        assert_eq!(heaviest_bank.slot(), 5);
        assert!(heaviest_bank_on_same_fork.is_none());
        select_vote_and_reset_forks(
            &heaviest_bank,
            heaviest_bank_on_same_fork.as_ref(),
            &ancestors,
            &descendants,
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &tbft_structs.heaviest_subtree_fork_choice,
        )
    }

    #[test]
    fn test_duplicate_rollback_then_vote_on_other_duplicate_success() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote_on_other_duplicate(3);

        // If we vote on 2 first then try to vote on 5, we should succeed
        assert_matches!(
            vote_bank
                .map(|(bank, switch_decision)| (bank.slot(), switch_decision))
                .unwrap(),
            (5, SwitchForkDecision::SwitchProof(_))
        );
        assert_eq!(reset_bank.unwrap().slot(), 5);
        assert!(heaviest_fork_failures.is_empty());
    }

    #[test]
    fn test_duplicate_rollback_then_vote_on_other_duplicate_same_slot_locked_out() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote_on_other_duplicate(5);

        // If we vote on 5 first then try to vote on another version of 5,
        // lockout should fail
        assert!(vote_bank.is_none());
        assert_eq!(reset_bank.unwrap().slot(), 5);
        assert_eq!(
            heaviest_fork_failures,
            vec![HeaviestForkFailures::LockedOut(5)]
        );
    }

    #[test]
    #[ignore]
    fn test_duplicate_rollback_then_vote_on_other_duplicate_different_slot_locked_out() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote_on_other_duplicate(4);

        // If we vote on 4 first then try to vote on 5 descended from another version
        // of 4, lockout should fail
        assert!(vote_bank.is_none());
        assert_eq!(reset_bank.unwrap().slot(), 5);
        assert_eq!(
            heaviest_fork_failures,
            vec![HeaviestForkFailures::LockedOut(5)]
        );
    }

    #[test]
    fn test_gossip_vote_doesnt_affect_fork_choice() {
        let (
            VoteSimulator {
                bank_forks,
                mut tbft_structs,
                mut latest_validator_votes_for_frozen_banks,
                vote_pubkeys,
                ..
            },
            _,
        ) = setup_default_forks(1, None::<GenerateVotes>);

        let vote_pubkey = vote_pubkeys[0];
        let mut unfrozen_gossip_verified_vote_hashes = UnfrozenGossipVerifiedVoteHashes::default();
        let (gossip_verified_vote_hash_sender, gossip_verified_vote_hash_receiver) = unbounded();

        // Best slot is 4
        assert_eq!(
            tbft_structs
                .heaviest_subtree_fork_choice
                .best_overall_slot()
                .0,
            4
        );

        // Cast a vote for slot 3 on one fork
        let vote_slot = 3;
        let vote_bank = bank_forks.read().unwrap().get(vote_slot).unwrap();
        gossip_verified_vote_hash_sender
            .send((vote_pubkey, vote_slot, vote_bank.hash()))
            .expect("Send should succeed");
        ReplayStage::process_gossip_verified_vote_hashes(
            &gossip_verified_vote_hash_receiver,
            &mut unfrozen_gossip_verified_vote_hashes,
            &tbft_structs.heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Pick the best fork. Gossip votes shouldn't affect fork choice
        tbft_structs
            .heaviest_subtree_fork_choice
            .compute_bank_stats(
                &vote_bank,
                &Tower::default(),
                &mut latest_validator_votes_for_frozen_banks,
            );

        // Best slot is still 4
        assert_eq!(
            tbft_structs
                .heaviest_subtree_fork_choice
                .best_overall_slot()
                .0,
            4
        );
    }

    #[test]
    fn test_replay_stage_refresh_last_vote() {
        let ReplayBlockstoreComponents {
            cluster_info,
            poh_recorder,
            mut tower,
            my_pubkey,
            vote_simulator,
            ..
        } = replay_blockstore_components(None, 10, None::<GenerateVotes>);
        let tower_storage = NullTowerStorage::default();

        let VoteSimulator {
            mut validator_keypairs,
            bank_forks,
            mut progress,
            ..
        } = vote_simulator;

        let mut last_vote_refresh_time = LastVoteRefreshTime {
            last_refresh_time: Instant::now(),
            last_print_time: Instant::now(),
        };
        let has_new_vote_been_rooted = false;
        let mut voted_signatures = vec![];

        let identity_keypair = cluster_info.keypair().clone();
        let my_vote_keypair = vec![Arc::new(
            validator_keypairs.remove(&my_pubkey).unwrap().vote_keypair,
        )];
        let my_vote_pubkey = my_vote_keypair[0].pubkey();
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();
        progress.insert(
            bank0.slot(),
            ForkProgress::new_from_bank(
                &bank0,
                bank0.collector_id(),
                &Pubkey::default(),
                None,
                0,
                0,
            ),
        );

        bank0.set_initial_accounts_hash_verification_completed();

        let (voting_sender, voting_receiver) = unbounded();

        // Simulate landing a vote for slot 0 landing in slot 1
        let bank1 = new_bank_from_parent_with_bank_forks(
            bank_forks.as_ref(),
            bank0.clone(),
            &Pubkey::default(),
            1,
        );
        bank1.fill_bank_with_ticks_for_tests();
        progress.insert(
            bank1.slot(),
            ForkProgress::new_from_bank(
                &bank1,
                bank1.collector_id(),
                &Pubkey::default(),
                None,
                0,
                0,
            ),
        );
        tower.record_bank_vote(&bank0);
        ReplayStage::push_vote(
            &bank0,
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut tower,
            &SwitchForkDecision::SameFork,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut ReplayLoopTiming::default(),
            &voting_sender,
            None,
        );
        let vote_info = voting_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

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

        crate::voting_service::VotingService::handle_vote(
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            vote_info,
            Arc::new(connection_cache),
        );

        let mut cursor = Cursor::default();
        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        let vote_tx = &votes[0];
        assert_eq!(vote_tx.message.recent_blockhash, bank0.last_blockhash());
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            BlockhashStatus::Blockhash(bank0.last_blockhash())
        );
        assert_eq!(tower.last_voted_slot().unwrap(), 0);
        bank1.process_transaction(vote_tx).unwrap();
        bank1.freeze();

        // Trying to refresh the vote for bank 0 in bank 1 or bank 2 won't succeed because
        // the last vote has landed already
        let bank2 = new_bank_from_parent_with_bank_forks(
            bank_forks.as_ref(),
            bank1.clone(),
            &Pubkey::default(),
            2,
        );
        bank2.fill_bank_with_ticks_for_tests();
        bank2.freeze();
        progress.insert(
            bank2.slot(),
            ForkProgress::new_from_bank(
                &bank2,
                bank2.collector_id(),
                &Pubkey::default(),
                None,
                0,
                0,
            ),
        );
        for refresh_bank in &[bank1.clone(), bank2.clone()] {
            progress
                .get_fork_stats_mut(refresh_bank.slot())
                .unwrap()
                .my_latest_landed_vote =
                Tower::last_voted_slot_in_bank(refresh_bank, &my_vote_pubkey);
            assert!(!ReplayStage::maybe_refresh_last_vote(
                &mut tower,
                &progress,
                Some(refresh_bank.clone()),
                &my_vote_pubkey,
                &identity_keypair,
                &my_vote_keypair,
                &mut voted_signatures,
                has_new_vote_been_rooted,
                &mut last_vote_refresh_time,
                &voting_sender,
                None,
            ));

            // No new votes have been submitted to gossip
            let votes = cluster_info.get_votes(&mut cursor);
            assert!(votes.is_empty());
            // Tower's latest vote tx blockhash hasn't changed either
            assert_eq!(
                tower.last_vote_tx_blockhash(),
                BlockhashStatus::Blockhash(bank0.last_blockhash())
            );
            assert_eq!(tower.last_voted_slot().unwrap(), 0);
        }

        // Simulate submitting a new vote for bank 1 to the network, but the vote
        // not landing
        tower.record_bank_vote(&bank1);
        ReplayStage::push_vote(
            &bank1,
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut tower,
            &SwitchForkDecision::SameFork,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut ReplayLoopTiming::default(),
            &voting_sender,
            None,
        );
        let vote_info = voting_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

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

        crate::voting_service::VotingService::handle_vote(
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            vote_info,
            Arc::new(connection_cache),
        );

        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        let vote_tx = &votes[0];
        assert_eq!(vote_tx.message.recent_blockhash, bank1.last_blockhash());
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            BlockhashStatus::Blockhash(bank1.last_blockhash())
        );
        assert_eq!(tower.last_voted_slot().unwrap(), 1);

        // Trying to refresh the vote for bank 1 in bank 2 won't succeed because
        // the blockheight is not increased enough
        progress
            .get_fork_stats_mut(bank1.slot())
            .unwrap()
            .block_height = bank1.block_height();
        progress
            .get_fork_stats_mut(bank2.slot())
            .unwrap()
            .block_height = bank2.block_height();
        progress
            .get_fork_stats_mut(bank2.slot())
            .unwrap()
            .my_latest_landed_vote = Tower::last_voted_slot_in_bank(&bank2, &my_vote_pubkey);
        assert!(!ReplayStage::maybe_refresh_last_vote(
            &mut tower,
            &progress,
            Some(bank2.clone()),
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut last_vote_refresh_time,
            &voting_sender,
            None,
        ));

        // No new votes have been submitted to gossip
        let votes = cluster_info.get_votes(&mut cursor);
        assert!(votes.is_empty());
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            BlockhashStatus::Blockhash(bank1.last_blockhash())
        );
        assert_eq!(tower.last_voted_slot().unwrap(), 1);

        // Create a bank where the last vote transaction will have expired
        let expired_bank = {
            let mut parent_bank = bank2.clone();
            for _ in 0..REFRESH_VOTE_BLOCKHEIGHT {
                let slot = parent_bank.slot() + 1;
                parent_bank = new_bank_from_parent_with_bank_forks(
                    bank_forks.as_ref(),
                    parent_bank,
                    &Pubkey::default(),
                    slot,
                );
                parent_bank.fill_bank_with_ticks_for_tests();
                parent_bank.freeze();
            }
            parent_bank
        };
        progress.insert(
            expired_bank.slot(),
            ForkProgress::new_from_bank(
                &expired_bank,
                expired_bank.collector_id(),
                &Pubkey::default(),
                None,
                0,
                0,
            ),
        );

        // Now trying to refresh the vote for slot 1 will succeed because the blockheight has increased
        // enough
        progress
            .get_fork_stats_mut(expired_bank.slot())
            .unwrap()
            .my_latest_landed_vote = Tower::last_voted_slot_in_bank(&expired_bank, &my_vote_pubkey);
        progress
            .get_fork_stats_mut(expired_bank.slot())
            .unwrap()
            .block_height = expired_bank.block_height();
        last_vote_refresh_time.last_refresh_time = last_vote_refresh_time
            .last_refresh_time
            .checked_sub(Duration::from_millis(
                MAX_VOTE_REFRESH_INTERVAL_MILLIS as u64 + 1,
            ))
            .unwrap();
        let clone_refresh_time = last_vote_refresh_time.last_refresh_time;
        assert!(ReplayStage::maybe_refresh_last_vote(
            &mut tower,
            &progress,
            Some(expired_bank.clone()),
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut last_vote_refresh_time,
            &voting_sender,
            None,
        ));
        let vote_info = voting_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
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

        crate::voting_service::VotingService::handle_vote(
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            vote_info,
            Arc::new(connection_cache),
        );

        assert!(last_vote_refresh_time.last_refresh_time > clone_refresh_time);
        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        let vote_tx = &votes[0];
        assert_eq!(
            vote_tx.message.recent_blockhash,
            expired_bank.last_blockhash()
        );
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            BlockhashStatus::Blockhash(expired_bank.last_blockhash())
        );
        assert_eq!(tower.last_voted_slot().unwrap(), 1);

        // Processing the vote transaction should be valid
        let expired_bank_child_slot = expired_bank.slot() + 1;
        let expired_bank_child = new_bank_from_parent_with_bank_forks(
            bank_forks.as_ref(),
            expired_bank.clone(),
            &Pubkey::default(),
            expired_bank_child_slot,
        );
        expired_bank_child.process_transaction(vote_tx).unwrap();
        let vote_account = expired_bank_child
            .get_vote_account(&my_vote_pubkey)
            .unwrap();
        assert_eq!(
            vote_account
                .vote_state_view()
                .unwrap()
                .votes_iter()
                .map(|lockout| lockout.slot())
                .collect_vec(),
            vec![0, 1]
        );
        expired_bank_child.fill_bank_with_ticks_for_tests();
        expired_bank_child.freeze();

        // Trying to refresh the vote on a sibling bank where:
        // 1) The vote for slot 1 hasn't landed
        // 2) The blockheight is still eligble for a refresh
        // This will still not refresh because `MAX_VOTE_REFRESH_INTERVAL_MILLIS` has not expired yet
        let expired_bank_sibling = {
            let mut parent_bank = bank2.clone();
            for i in 0..expired_bank_child_slot {
                let slot = expired_bank_child.slot() + i + 1;
                parent_bank = new_bank_from_parent_with_bank_forks(
                    bank_forks.as_ref(),
                    parent_bank,
                    &Pubkey::default(),
                    slot,
                );
                parent_bank.fill_bank_with_ticks_for_tests();
                parent_bank.freeze();
            }
            parent_bank
        };
        // Set the last refresh to now, shouldn't refresh because the last refresh just happened.
        last_vote_refresh_time.last_refresh_time = Instant::now();
        ReplayStage::maybe_refresh_last_vote(
            &mut tower,
            &progress,
            Some(expired_bank_sibling),
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut last_vote_refresh_time,
            &voting_sender,
            None,
        );

        let votes = cluster_info.get_votes(&mut cursor);
        assert!(votes.is_empty());
        assert_eq!(
            vote_tx.message.recent_blockhash,
            expired_bank.last_blockhash()
        );
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            BlockhashStatus::Blockhash(expired_bank.last_blockhash())
        );
        assert_eq!(tower.last_voted_slot().unwrap(), 1);
    }

    #[allow(clippy::too_many_arguments)]
    fn send_vote_in_new_bank(
        parent_bank: Arc<Bank>,
        my_slot: Slot,
        my_vote_keypair: &[Arc<Keypair>],
        tower: &mut Tower,
        identity_keypair: &Keypair,
        voted_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
        voting_sender: &Sender<VoteOp>,
        voting_receiver: &Receiver<VoteOp>,
        cluster_info: &ClusterInfo,
        poh_recorder: &RwLock<PohRecorder>,
        tower_storage: &dyn TowerStorage,
        make_it_landing: bool,
        cursor: &mut Cursor,
        bank_forks: Arc<RwLock<BankForks>>,
        progress: &mut ProgressMap,
    ) -> Arc<Bank> {
        let my_vote_pubkey = &my_vote_keypair[0].pubkey();
        tower.record_bank_vote(&parent_bank);
        ReplayStage::push_vote(
            &parent_bank,
            my_vote_pubkey,
            identity_keypair,
            my_vote_keypair,
            tower,
            &SwitchForkDecision::SameFork,
            voted_signatures,
            has_new_vote_been_rooted,
            &mut ReplayLoopTiming::default(),
            voting_sender,
            None,
        );
        let vote_info = voting_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
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

        crate::voting_service::VotingService::handle_vote(
            cluster_info,
            poh_recorder,
            tower_storage,
            vote_info,
            Arc::new(connection_cache),
        );

        let votes = cluster_info.get_votes(cursor);
        assert_eq!(votes.len(), 1);
        let vote_tx = &votes[0];
        assert_eq!(
            vote_tx.message.recent_blockhash,
            parent_bank.last_blockhash()
        );
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            BlockhashStatus::Blockhash(parent_bank.last_blockhash())
        );
        assert_eq!(tower.last_voted_slot().unwrap(), parent_bank.slot());
        let bank = new_bank_from_parent_with_bank_forks(
            &bank_forks,
            parent_bank,
            &Pubkey::default(),
            my_slot,
        );
        bank.fill_bank_with_ticks_for_tests();
        if make_it_landing {
            bank.process_transaction(vote_tx).unwrap();
        }
        bank.freeze();
        progress.entry(my_slot).or_insert_with(|| {
            ForkProgress::new_from_bank(
                &bank,
                &identity_keypair.pubkey(),
                my_vote_pubkey,
                None,
                0,
                0,
            )
        });
        bank_forks.read().unwrap().get(my_slot).unwrap()
    }

    #[test]
    fn test_replay_stage_last_vote_outside_slot_hashes() {
        solana_logger::setup();
        let ReplayBlockstoreComponents {
            cluster_info,
            poh_recorder,
            mut tower,
            my_pubkey,
            vote_simulator,
            ..
        } = replay_blockstore_components(None, 10, None::<GenerateVotes>);
        let tower_storage = NullTowerStorage::default();

        let VoteSimulator {
            mut validator_keypairs,
            bank_forks,
            mut tbft_structs,
            mut latest_validator_votes_for_frozen_banks,
            mut progress,
            ..
        } = vote_simulator;

        let has_new_vote_been_rooted = false;
        let mut voted_signatures = vec![];

        let identity_keypair = cluster_info.keypair().clone();
        let my_vote_keypair = vec![Arc::new(
            validator_keypairs.remove(&my_pubkey).unwrap().vote_keypair,
        )];
        let my_vote_pubkey = my_vote_keypair[0].pubkey();
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();

        bank0.set_initial_accounts_hash_verification_completed();

        // Add a new fork starting from 0 with bigger slot number, we assume it has a bigger
        // weight, but we cannot switch because of lockout.
        let other_fork_slot = 1;
        let other_fork_bank = new_bank_from_parent_with_bank_forks(
            bank_forks.as_ref(),
            bank0.clone(),
            &Pubkey::default(),
            other_fork_slot,
        );
        other_fork_bank.fill_bank_with_ticks_for_tests();
        other_fork_bank.freeze();
        progress.entry(other_fork_slot).or_insert_with(|| {
            ForkProgress::new_from_bank(
                &other_fork_bank,
                &identity_keypair.pubkey(),
                &my_vote_keypair[0].pubkey(),
                None,
                0,
                0,
            )
        });

        let (voting_sender, voting_receiver) = unbounded();
        let mut cursor = Cursor::default();

        let mut new_bank = send_vote_in_new_bank(
            bank0,
            2,
            &my_vote_keypair,
            &mut tower,
            &identity_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &voting_sender,
            &voting_receiver,
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            true,
            &mut cursor,
            bank_forks.clone(),
            &mut progress,
        );
        new_bank = send_vote_in_new_bank(
            new_bank.clone(),
            new_bank.slot() + 1,
            &my_vote_keypair,
            &mut tower,
            &identity_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &voting_sender,
            &voting_receiver,
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            false,
            &mut cursor,
            bank_forks.clone(),
            &mut progress,
        );
        // Create enough banks on the fork so last vote is outside SlotHash, make sure
        // we now vote at the tip of the fork.
        let last_voted_slot = tower.last_voted_slot().unwrap();
        while new_bank.is_in_slot_hashes_history(&last_voted_slot) {
            let new_slot = new_bank.slot() + 1;
            let bank = new_bank_from_parent_with_bank_forks(
                bank_forks.as_ref(),
                new_bank,
                &Pubkey::default(),
                new_slot,
            );
            bank.fill_bank_with_ticks_for_tests();
            bank.freeze();
            progress.entry(new_slot).or_insert_with(|| {
                ForkProgress::new_from_bank(
                    &bank,
                    &identity_keypair.pubkey(),
                    &my_vote_keypair[0].pubkey(),
                    None,
                    0,
                    0,
                )
            });
            new_bank = bank_forks.read().unwrap().get(new_slot).unwrap();
        }
        let tip_of_voted_fork = new_bank.slot();

        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &bank_forks.read().unwrap().ancestors(),
            &mut frozen_banks,
            &mut tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );
        assert_eq!(tower.last_voted_slot(), Some(last_voted_slot));
        assert_eq!(progress.my_latest_landed_vote(tip_of_voted_fork), Some(0));
        let other_fork_bank = &bank_forks.read().unwrap().get(other_fork_slot).unwrap();
        let SelectVoteAndResetForkResult { vote_bank, .. } = select_vote_and_reset_forks(
            other_fork_bank,
            Some(&new_bank),
            &bank_forks.read().unwrap().ancestors(),
            &bank_forks.read().unwrap().descendants(),
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &tbft_structs.heaviest_subtree_fork_choice,
        );
        assert!(vote_bank.is_some());
        assert_eq!(vote_bank.unwrap().0.slot(), tip_of_voted_fork);

        // If last vote is already equal to heaviest_bank_on_same_voted_fork,
        // we should not vote.
        let last_voted_bank = &bank_forks.read().unwrap().get(last_voted_slot).unwrap();
        let SelectVoteAndResetForkResult { vote_bank, .. } = select_vote_and_reset_forks(
            other_fork_bank,
            Some(last_voted_bank),
            &bank_forks.read().unwrap().ancestors(),
            &bank_forks.read().unwrap().descendants(),
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &tbft_structs.heaviest_subtree_fork_choice,
        );
        assert!(vote_bank.is_none());

        // If last vote is still inside slot hashes history of heaviest_bank_on_same_voted_fork,
        // we should not vote.
        let last_voted_bank_plus_1 = &bank_forks.read().unwrap().get(last_voted_slot + 1).unwrap();
        let SelectVoteAndResetForkResult { vote_bank, .. } = select_vote_and_reset_forks(
            other_fork_bank,
            Some(last_voted_bank_plus_1),
            &bank_forks.read().unwrap().ancestors(),
            &bank_forks.read().unwrap().descendants(),
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &tbft_structs.heaviest_subtree_fork_choice,
        );
        assert!(vote_bank.is_none());

        // create a new bank and make last_voted_slot land, we should not vote.
        progress
            .entry(new_bank.slot())
            .and_modify(|s| s.fork_stats.my_latest_landed_vote = Some(last_voted_slot));
        assert!(!new_bank.is_in_slot_hashes_history(&last_voted_slot));
        let SelectVoteAndResetForkResult { vote_bank, .. } = select_vote_and_reset_forks(
            other_fork_bank,
            Some(&new_bank),
            &bank_forks.read().unwrap().ancestors(),
            &bank_forks.read().unwrap().descendants(),
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &tbft_structs.heaviest_subtree_fork_choice,
        );
        assert!(vote_bank.is_none());
    }

    #[test]
    fn test_retransmit_latest_unpropagated_leader_slot() {
        let ReplayBlockstoreComponents {
            validator_node_to_vote_keys,
            leader_schedule_cache,
            poh_recorder,
            vote_simulator,
            ..
        } = replay_blockstore_components(None, 10, None::<GenerateVotes>);

        let VoteSimulator {
            mut progress,
            ref bank_forks,
            ..
        } = vote_simulator;

        let poh_recorder = Arc::new(poh_recorder);
        let (retransmit_slots_sender, retransmit_slots_receiver) = unbounded();

        let bank1 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &leader_schedule_cache.slot_leader_at(1, None).unwrap(),
            1,
        );
        progress.insert(
            1,
            ForkProgress::new_from_bank(
                &bank1,
                bank1.collector_id(),
                validator_node_to_vote_keys
                    .get(bank1.collector_id())
                    .unwrap(),
                Some(0),
                0,
                0,
            ),
        );
        assert!(progress.get_propagated_stats(1).unwrap().is_leader_slot);
        bank1.freeze();
        bank_forks.write().unwrap().insert(bank1);

        progress.get_retransmit_info_mut(0).unwrap().retry_time = Instant::now();
        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert_matches!(res, Err(_));
        assert_eq!(
            progress.get_retransmit_info(0).unwrap().retry_iteration,
            0,
            "retransmit should not advance retry_iteration before time has been set"
        );

        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert!(
            res.is_err(),
            "retry_iteration=0, elapsed < 2^0 * RETRANSMIT_BASE_DELAY_MS"
        );

        progress.get_retransmit_info_mut(0).unwrap().retry_time = Instant::now()
            .checked_sub(Duration::from_millis(RETRANSMIT_BASE_DELAY_MS + 1))
            .unwrap();
        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert!(
            res.is_ok(),
            "retry_iteration=0, elapsed > RETRANSMIT_BASE_DELAY_MS"
        );
        assert_eq!(
            progress.get_retransmit_info(0).unwrap().retry_iteration,
            1,
            "retransmit should advance retry_iteration"
        );

        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert!(
            res.is_err(),
            "retry_iteration=1, elapsed < 2^1 * RETRANSMIT_BASE_DELAY_MS"
        );

        progress.get_retransmit_info_mut(0).unwrap().retry_time = Instant::now()
            .checked_sub(Duration::from_millis(RETRANSMIT_BASE_DELAY_MS + 1))
            .unwrap();
        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert!(
            res.is_err(),
            "retry_iteration=1, elapsed < 2^1 * RETRANSMIT_BASE_DELAY_MS"
        );

        progress.get_retransmit_info_mut(0).unwrap().retry_time = Instant::now()
            .checked_sub(Duration::from_millis(2 * RETRANSMIT_BASE_DELAY_MS + 1))
            .unwrap();
        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert!(
            res.is_ok(),
            "retry_iteration=1, elapsed > 2^1 * RETRANSMIT_BASE_DELAY_MS"
        );
        assert_eq!(
            progress.get_retransmit_info(0).unwrap().retry_iteration,
            2,
            "retransmit should advance retry_iteration"
        );

        // increment to retry iteration 3
        progress
            .get_retransmit_info_mut(0)
            .unwrap()
            .increment_retry_iteration();

        progress.get_retransmit_info_mut(0).unwrap().retry_time = Instant::now()
            .checked_sub(Duration::from_millis(2 * RETRANSMIT_BASE_DELAY_MS + 1))
            .unwrap();
        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert!(
            res.is_err(),
            "retry_iteration=3, elapsed < 2^3 * RETRANSMIT_BASE_DELAY_MS"
        );

        progress.get_retransmit_info_mut(0).unwrap().retry_time = Instant::now()
            .checked_sub(Duration::from_millis(8 * RETRANSMIT_BASE_DELAY_MS + 1))
            .unwrap();
        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );
        let res = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10));
        assert!(
            res.is_ok(),
            "retry_iteration=3, elapsed > 2^3 * RETRANSMIT_BASE_DELAY"
        );
        assert_eq!(
            progress.get_retransmit_info(0).unwrap().retry_iteration,
            4,
            "retransmit should advance retry_iteration"
        );
    }

    fn receive_slots(retransmit_slots_receiver: &Receiver<Slot>) -> Vec<Slot> {
        let mut slots = Vec::default();
        while let Ok(slot) = retransmit_slots_receiver.recv_timeout(Duration::from_millis(10)) {
            slots.push(slot);
        }
        slots
    }

    #[test]
    fn test_maybe_retransmit_unpropagated_slots() {
        let ReplayBlockstoreComponents {
            validator_node_to_vote_keys,
            leader_schedule_cache,
            vote_simulator,
            ..
        } = replay_blockstore_components(None, 10, None::<GenerateVotes>);

        let VoteSimulator {
            mut progress,
            ref bank_forks,
            ..
        } = vote_simulator;

        let (retransmit_slots_sender, retransmit_slots_receiver) = unbounded();
        let retry_time = Instant::now()
            .checked_sub(Duration::from_millis(RETRANSMIT_BASE_DELAY_MS + 1))
            .unwrap();
        progress.get_retransmit_info_mut(0).unwrap().retry_time = retry_time;

        let mut prev_index = 0;
        for i in (1..10).chain(11..15) {
            let bank = Bank::new_from_parent(
                bank_forks.read().unwrap().get(prev_index).unwrap(),
                &leader_schedule_cache.slot_leader_at(i, None).unwrap(),
                i,
            );
            progress.insert(
                i,
                ForkProgress::new_from_bank(
                    &bank,
                    bank.collector_id(),
                    validator_node_to_vote_keys
                        .get(bank.collector_id())
                        .unwrap(),
                    Some(0),
                    0,
                    0,
                ),
            );
            assert!(progress.get_propagated_stats(i).unwrap().is_leader_slot);
            bank.freeze();
            bank_forks.write().unwrap().insert(bank);
            prev_index = i;
            progress.get_retransmit_info_mut(i).unwrap().retry_time = retry_time;
        }

        // expect single slot when latest_leader_slot is the start of a consecutive range
        let latest_leader_slot = 0;
        ReplayStage::maybe_retransmit_unpropagated_slots(
            "test",
            &retransmit_slots_sender,
            &mut progress,
            latest_leader_slot,
        );
        let received_slots = receive_slots(&retransmit_slots_receiver);
        assert_eq!(received_slots, vec![0]);

        // expect range of slots from start of consecutive slots
        let latest_leader_slot = 6;
        ReplayStage::maybe_retransmit_unpropagated_slots(
            "test",
            &retransmit_slots_sender,
            &mut progress,
            latest_leader_slot,
        );
        let received_slots = receive_slots(&retransmit_slots_receiver);
        assert_eq!(received_slots, vec![4, 5, 6]);

        // expect range of slots skipping a discontinuity in the range
        let latest_leader_slot = 11;
        ReplayStage::maybe_retransmit_unpropagated_slots(
            "test",
            &retransmit_slots_sender,
            &mut progress,
            latest_leader_slot,
        );
        let received_slots = receive_slots(&retransmit_slots_receiver);
        assert_eq!(received_slots, vec![8, 9, 11]);
    }

    #[test]
    fn test_dumped_slot_not_causing_panic() {
        solana_logger::setup();
        let ReplayBlockstoreComponents {
            validator_node_to_vote_keys,
            leader_schedule_cache,
            poh_recorder,
            vote_simulator,
            rpc_subscriptions,
            ref my_pubkey,
            ref blockstore,
            ..
        } = replay_blockstore_components(None, 10, None::<GenerateVotes>);

        let VoteSimulator {
            mut progress,
            ref bank_forks,
            ..
        } = vote_simulator;

        let poh_recorder = Arc::new(poh_recorder);
        let (retransmit_slots_sender, _) = unbounded();

        // Use a bank slot when I was not leader to avoid panic for dumping my own slot
        let slot_to_dump = (1..100)
            .find(|i| leader_schedule_cache.slot_leader_at(*i, None) != Some(*my_pubkey))
            .unwrap();
        let bank_to_dump = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &leader_schedule_cache
                .slot_leader_at(slot_to_dump, None)
                .unwrap(),
            slot_to_dump,
        );
        progress.insert(
            slot_to_dump,
            ForkProgress::new_from_bank(
                &bank_to_dump,
                bank_to_dump.collector_id(),
                validator_node_to_vote_keys
                    .get(bank_to_dump.collector_id())
                    .unwrap(),
                Some(0),
                0,
                0,
            ),
        );
        assert!(progress.get_propagated_stats(slot_to_dump).is_some());
        bank_to_dump.freeze();
        bank_forks.write().unwrap().insert(bank_to_dump);
        let bank_to_dump = bank_forks
            .read()
            .unwrap()
            .get(slot_to_dump)
            .expect("Just inserted");

        progress.get_retransmit_info_mut(0).unwrap().retry_time = Instant::now();
        poh_recorder
            .write()
            .unwrap()
            .reset(bank_to_dump, Some((slot_to_dump + 1, slot_to_dump + 1)));
        assert_eq!(poh_recorder.read().unwrap().start_slot(), slot_to_dump);

        // Now dump and repair slot_to_dump
        let (mut ancestors, mut descendants) = {
            let r_bank_forks = bank_forks.read().unwrap();
            (r_bank_forks.ancestors(), r_bank_forks.descendants())
        };
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        let bank_to_dump_bad_hash = Hash::new_unique();
        duplicate_slots_to_repair.insert(slot_to_dump, bank_to_dump_bad_hash);
        let mut purge_repair_slot_counter = PurgeRepairSlotCounter::default();
        let (dumped_slots_sender, dumped_slots_receiver) = unbounded();

        ReplayStage::dump_then_repair_correct_slots(
            &mut duplicate_slots_to_repair,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            bank_forks,
            blockstore,
            None,
            &mut purge_repair_slot_counter,
            &dumped_slots_sender,
            my_pubkey,
            &leader_schedule_cache,
        );
        assert_eq!(
            dumped_slots_receiver.recv_timeout(Duration::from_secs(1)),
            Ok(vec![(slot_to_dump, bank_to_dump_bad_hash)])
        );

        // Now check it doesn't cause panic in the following functions.
        ReplayStage::retransmit_latest_unpropagated_leader_slot(
            &poh_recorder,
            &retransmit_slots_sender,
            &mut progress,
        );

        let (banking_tracer, _) = BankingTracer::new(None).unwrap();
        // A vote has not technically been rooted, but it doesn't matter for
        // this test to use true to avoid skipping the leader slot
        let has_new_vote_been_rooted = true;
        let track_transaction_indexes = false;

        assert!(!ReplayStage::maybe_start_leader(
            my_pubkey,
            bank_forks,
            &poh_recorder,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &retransmit_slots_sender,
            &mut SkippedSlotsInfo::default(),
            &banking_tracer,
            has_new_vote_been_rooted,
            track_transaction_indexes,
            &None,
            &mut false,
        ));
    }

    #[test]
    #[should_panic(expected = "We are attempting to dump a block that we produced")]
    fn test_dump_own_slots_fails() {
        // Create the tree of banks in a BankForks object
        let forks = tr(0) / (tr(1)) / (tr(2));

        let ReplayBlockstoreComponents {
            ref mut vote_simulator,
            ref blockstore,
            ref my_pubkey,
            ref leader_schedule_cache,
            ..
        } = replay_blockstore_components(Some(forks), 1, None);

        let VoteSimulator {
            ref mut progress,
            ref bank_forks,
            ..
        } = vote_simulator;

        let (mut ancestors, mut descendants) = {
            let r_bank_forks = bank_forks.read().unwrap();
            (r_bank_forks.ancestors(), r_bank_forks.descendants())
        };

        // Insert different versions of both 1 and 2. Although normally these slots would be dumped,
        // because we were the leader for these slots we should panic
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        duplicate_slots_to_repair.insert(1, Hash::new_unique());
        duplicate_slots_to_repair.insert(2, Hash::new_unique());
        let mut purge_repair_slot_counter = PurgeRepairSlotCounter::default();
        let (dumped_slots_sender, _) = unbounded();

        ReplayStage::dump_then_repair_correct_slots(
            &mut duplicate_slots_to_repair,
            &mut ancestors,
            &mut descendants,
            progress,
            bank_forks,
            blockstore,
            None,
            &mut purge_repair_slot_counter,
            &dumped_slots_sender,
            my_pubkey,
            leader_schedule_cache,
        );
    }

    fn run_compute_and_select_forks(
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        tower: &mut Tower,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
        my_vote_pubkey: Option<Pubkey>,
    ) -> (Option<Slot>, Option<Slot>, Vec<HeaviestForkFailures>) {
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let ancestors = &bank_forks.read().unwrap().ancestors();
        let descendants = &bank_forks.read().unwrap().descendants();
        ReplayStage::compute_bank_stats(
            &my_vote_pubkey.unwrap_or_default(),
            &bank_forks.read().unwrap().ancestors(),
            &mut frozen_banks,
            tower,
            progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            bank_forks,
            heaviest_subtree_fork_choice,
            latest_validator_votes_for_frozen_banks,
        );
        let (heaviest_bank, heaviest_bank_on_same_fork) = heaviest_subtree_fork_choice
            .select_forks(&frozen_banks, tower, progress, ancestors, bank_forks);
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = select_vote_and_reset_forks(
            &heaviest_bank,
            heaviest_bank_on_same_fork.as_ref(),
            ancestors,
            descendants,
            progress,
            tower,
            latest_validator_votes_for_frozen_banks,
            heaviest_subtree_fork_choice,
        );
        (
            vote_bank.map(|(b, _)| b.slot()),
            reset_bank.map(|b| b.slot()),
            heaviest_fork_failures,
        )
    }

    type GenerateVotes = Box<dyn Fn(Vec<Pubkey>) -> HashMap<Pubkey, Vec<Slot>>>;

    pub fn setup_forks_from_tree(
        tree: Tree<Slot>,
        num_keys: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> (VoteSimulator, Blockstore) {
        let mut vote_simulator = VoteSimulator::new(num_keys);
        let pubkeys: Vec<Pubkey> = vote_simulator
            .validator_keypairs
            .values()
            .map(|k| k.node_keypair.pubkey())
            .collect();
        let cluster_votes = generate_votes
            .map(|generate_votes| generate_votes(pubkeys))
            .unwrap_or_default();
        vote_simulator.fill_bank_forks(tree.clone(), &cluster_votes, true);
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&ledger_path).unwrap();
        blockstore.add_tree(tree, false, true, 2, Hash::default());
        (vote_simulator, blockstore)
    }

    fn setup_default_forks(
        num_keys: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> (VoteSimulator, Blockstore) {
        /*
            Build fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |    slot 3
            slot 4    |
                    slot 5
                      |
                    slot 6
        */

        let tree = tr(0) / (tr(1) / (tr(2) / (tr(4))) / (tr(3) / (tr(5) / (tr(6)))));
        setup_forks_from_tree(tree, num_keys, generate_votes)
    }

    fn check_map_eq<K: Eq + std::hash::Hash + std::fmt::Debug, T: PartialEq + std::fmt::Debug>(
        map1: &HashMap<K, T>,
        map2: &HashMap<K, T>,
    ) -> bool {
        map1.len() == map2.len() && map1.iter().all(|(k, v)| map2.get(k).unwrap() == v)
    }

    #[test]
    fn test_check_for_vote_only_mode() {
        let in_vote_only_mode = AtomicBool::new(false);
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank0 = Bank::new_for_tests(&genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);
        ReplayStage::check_for_vote_only_mode(1000, 0, &in_vote_only_mode, &bank_forks);
        assert!(in_vote_only_mode.load(Ordering::Relaxed));
        ReplayStage::check_for_vote_only_mode(10, 0, &in_vote_only_mode, &bank_forks);
        assert!(!in_vote_only_mode.load(Ordering::Relaxed));
    }

    #[test]
    fn test_tower_sync_from_bank_failed_switch() {
        solana_logger::setup_with_default(
            "error,solana_core::replay_stage=info,solana_core::consensus=info",
        );
        /*
            Fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |    slot 3
            slot 4    |
                    slot 5
                      |
                    slot 6

            We had some point voted 0 - 6, while the rest of the network voted 0 - 4.
            We are sitting with an oudated tower that has voted until 1. We see that 4 is the heaviest slot,
            however in the past we have voted up to 6. We must acknowledge the vote state present at 6,
            adopt it as our own and *not* vote on 2 or 4, to respect slashing rules as there is
            not enough stake to switch
        */

        let generate_votes = |pubkeys: Vec<Pubkey>| {
            pubkeys
                .into_iter()
                .zip(iter::once(vec![0, 1, 3, 5, 6]).chain(iter::repeat(vec![0, 1, 2, 4]).take(2)))
                .collect()
        };
        let (mut vote_simulator, _blockstore) =
            setup_default_forks(3, Some(Box::new(generate_votes)));
        let (bank_forks, mut progress) = (vote_simulator.bank_forks, vote_simulator.progress);
        let bank_hash = |slot| bank_forks.read().unwrap().bank_hash(slot).unwrap();
        let my_vote_pubkey = vote_simulator.vote_pubkeys[0];
        let mut tower = Tower::default();
        tower.node_pubkey = vote_simulator.node_pubkeys[0];
        tower.record_vote(0, bank_hash(0));
        tower.record_vote(1, bank_hash(1));

        let (vote_fork, reset_fork, failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            Some(my_vote_pubkey),
        );

        assert_eq!(vote_fork, None);
        assert_eq!(reset_fork, Some(6));
        assert_eq!(
            failures,
            vec![
                HeaviestForkFailures::FailedSwitchThreshold(4, 0, 30000),
                HeaviestForkFailures::LockedOut(4)
            ]
        );

        let (vote_fork, reset_fork, failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            Some(my_vote_pubkey),
        );

        assert_eq!(vote_fork, None);
        assert_eq!(reset_fork, Some(6));
        assert_eq!(
            failures,
            vec![
                HeaviestForkFailures::FailedSwitchThreshold(4, 0, 30000),
                HeaviestForkFailures::LockedOut(4)
            ]
        );
    }

    #[test]
    fn test_tower_sync_from_bank_failed_lockout() {
        solana_logger::setup_with_default(
            "error,solana_core::replay_stage=info,solana_core::consensus=info",
        );
        /*
            Fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 3    |
               |    slot 2
            slot 4    |
                    slot 5
                      |
                    slot 6

            We had some point voted 0 - 6, while the rest of the network voted 0 - 4.
            We are sitting with an oudated tower that has voted until 1. We see that 4 is the heaviest slot,
            however in the past we have voted up to 6. We must acknowledge the vote state present at 6,
            adopt it as our own and *not* vote on 3 or 4, to respect slashing rules as we are locked
            out on 4, even though there is enough stake to switch. However we should still reset onto
            4.
        */

        let generate_votes = |pubkeys: Vec<Pubkey>| {
            pubkeys
                .into_iter()
                .zip(iter::once(vec![0, 1, 2, 5, 6]).chain(iter::repeat(vec![0, 1, 3, 4]).take(2)))
                .collect()
        };
        let tree = tr(0) / (tr(1) / (tr(3) / (tr(4))) / (tr(2) / (tr(5) / (tr(6)))));
        let (mut vote_simulator, _blockstore) =
            setup_forks_from_tree(tree, 3, Some(Box::new(generate_votes)));
        let (bank_forks, mut progress) = (vote_simulator.bank_forks, vote_simulator.progress);
        let bank_hash = |slot| bank_forks.read().unwrap().bank_hash(slot).unwrap();
        let my_vote_pubkey = vote_simulator.vote_pubkeys[0];
        let mut tower = Tower::default();
        tower.node_pubkey = vote_simulator.node_pubkeys[0];
        tower.record_vote(0, bank_hash(0));
        tower.record_vote(1, bank_hash(1));

        let (vote_fork, reset_fork, failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            Some(my_vote_pubkey),
        );

        assert_eq!(vote_fork, None);
        assert_eq!(reset_fork, Some(4));
        assert_eq!(failures, vec![HeaviestForkFailures::LockedOut(4),]);

        let (vote_fork, reset_fork, failures) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.tbft_structs.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
            Some(my_vote_pubkey),
        );

        assert_eq!(vote_fork, None);
        assert_eq!(reset_fork, Some(4));
        assert_eq!(failures, vec![HeaviestForkFailures::LockedOut(4),]);
    }

    #[test]
    fn test_tower_adopt_from_bank_cache_only_computed() {
        solana_logger::setup_with_default(
            "error,solana_core::replay_stage=info,solana_core::consensus=info",
        );
        /*
            Fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 3    |
               |    slot 2
            slot 4    |
                    slot 5
                      |
                    slot 6

            We had some point voted 0 - 6, we are sitting with an oudated tower that has voted until 1.
        */

        let generate_votes = |pubkeys: Vec<Pubkey>| {
            pubkeys
                .into_iter()
                .zip(iter::once(vec![0, 1, 2, 5, 6]).chain(iter::repeat(vec![0, 1, 3, 4]).take(2)))
                .collect()
        };
        let tree = tr(0) / (tr(1) / (tr(3) / (tr(4))) / (tr(2) / (tr(5) / (tr(6)))));
        let (vote_simulator, _blockstore) =
            setup_forks_from_tree(tree, 3, Some(Box::new(generate_votes)));
        let (bank_forks, mut progress) = (vote_simulator.bank_forks, vote_simulator.progress);
        let bank_hash = |slot| bank_forks.read().unwrap().bank_hash(slot).unwrap();
        let my_vote_pubkey = vote_simulator.vote_pubkeys[0];
        let mut tower = Tower::default();
        tower.node_pubkey = vote_simulator.node_pubkeys[0];
        tower.record_vote(0, bank_hash(0));
        tower.record_vote(1, bank_hash(1));

        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let ancestors = &bank_forks.read().unwrap().ancestors();

        // slot 3 was computed in a previous iteration and failed threshold check, but was not locked out
        let fork_stats_3 = progress.get_fork_stats_mut(3).unwrap();
        fork_stats_3.vote_threshold = vec![ThresholdDecision::FailedThreshold(4, 4000)];
        fork_stats_3.is_locked_out = false;
        fork_stats_3.computed = true;
        // slot 4 is yet to be computed.
        let fork_stats_4 = progress.get_fork_stats_mut(4).unwrap();
        fork_stats_4.computed = false;

        frozen_banks.sort_by_key(|bank| bank.slot());
        let bank_6 = frozen_banks.get(6).unwrap();
        assert_eq!(bank_6.slot(), 6);

        ReplayStage::adopt_on_chain_tower_if_behind(
            &my_vote_pubkey,
            ancestors,
            &frozen_banks,
            &mut tower,
            &mut progress,
            bank_6,
            &bank_forks,
        );

        // slot 3 should now pass the threshold check but be locked out.
        let fork_stats_3 = progress.get_fork_stats(3).unwrap();
        assert!(fork_stats_3.vote_threshold.is_empty());
        assert!(fork_stats_3.is_locked_out);
        assert!(fork_stats_3.computed);
        // slot 4 should be untouched since it is yet to be computed.
        let fork_stats_4 = progress.get_fork_stats(4).unwrap();
        assert!(!fork_stats_4.is_locked_out);
        assert!(!fork_stats_4.computed);
    }

    #[test]
    fn test_tower_load_missing() {
        let tower_file = tempdir().unwrap().into_path();
        let tower_storage = FileTowerStorage::new(tower_file);
        let node_pubkey = Pubkey::new_unique();
        let vote_account = Pubkey::new_unique();
        let tree = tr(0) / (tr(1) / (tr(3) / (tr(4))) / (tr(2) / (tr(5) / (tr(6)))));
        let generate_votes = |pubkeys: Vec<Pubkey>| {
            pubkeys
                .into_iter()
                .zip(iter::once(vec![0, 1, 2, 5, 6]).chain(iter::repeat(vec![0, 1, 3, 4]).take(2)))
                .collect()
        };
        let (vote_simulator, _blockstore) =
            setup_forks_from_tree(tree, 3, Some(Box::new(generate_votes)));
        let bank_forks = vote_simulator.bank_forks;

        let tower =
            ReplayStage::load_tower(&tower_storage, &node_pubkey, &vote_account, &bank_forks)
                .unwrap();
        let expected_tower = Tower::new_for_tests(VOTE_THRESHOLD_DEPTH, VOTE_THRESHOLD_SIZE);
        assert_eq!(tower.vote_state, expected_tower.vote_state);
        assert_eq!(tower.node_pubkey, node_pubkey);
    }

    #[test]
    fn test_tower_load() {
        let tower_file = tempdir().unwrap().into_path();
        let tower_storage = FileTowerStorage::new(tower_file);
        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let vote_account = Pubkey::new_unique();
        let tree = tr(0) / (tr(1) / (tr(3) / (tr(4))) / (tr(2) / (tr(5) / (tr(6)))));
        let generate_votes = |pubkeys: Vec<Pubkey>| {
            pubkeys
                .into_iter()
                .zip(iter::once(vec![0, 1, 2, 5, 6]).chain(iter::repeat(vec![0, 1, 3, 4]).take(2)))
                .collect()
        };
        let (vote_simulator, _blockstore) =
            setup_forks_from_tree(tree, 3, Some(Box::new(generate_votes)));
        let bank_forks = vote_simulator.bank_forks;
        let expected_tower = Tower::new_random(node_pubkey);
        expected_tower.save(&tower_storage, &node_keypair).unwrap();

        let tower =
            ReplayStage::load_tower(&tower_storage, &node_pubkey, &vote_account, &bank_forks)
                .unwrap();
        assert_eq!(tower.vote_state, expected_tower.vote_state);
        assert_eq!(tower.node_pubkey, expected_tower.node_pubkey);
    }

    #[test]
    fn test_initialize_progress_and_fork_choice_with_duplicates() {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(123);

        let ticks_per_slot = 1;
        genesis_config.ticks_per_slot = ticks_per_slot;
        let (ledger_path, blockhash) =
            solana_ledger::create_new_tmp_ledger_auto_delete!(&genesis_config);
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();

        /*
          Bank forks with:
               slot 0
                 |
               slot 1 -> Duplicate before restart, the restart slot
                 |
               slot 2
                 |
               slot 3 -> Duplicate before restart, artificially rooted
                 |
               slot 4 -> Duplicate before restart, artificially rooted
                 |
               slot 5 -> Duplicate before restart
                 |
               slot 6
        */

        let mut last_hash = blockhash;
        for i in 0..6 {
            last_hash =
                fill_blockstore_slot_with_ticks(&blockstore, ticks_per_slot, i + 1, i, last_hash);
        }
        // Artifically root 3 and 4
        blockstore.set_roots([3, 4].iter()).unwrap();

        // Set up bank0
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let bank0 = bank_forks.read().unwrap().get_with_scheduler(0).unwrap();
        let recyclers = VerifyRecyclers::default();
        let replay_tx_thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .thread_name(|i| format!("solReplayTx{i:02}"))
            .build()
            .expect("new rayon threadpool");

        process_bank_0(
            &bank0,
            &blockstore,
            &replay_tx_thread_pool,
            &ProcessOptions::default(),
            &recyclers,
            None,
            None,
        )
        .unwrap();

        // Mark block 1, 3, 4, 5 as duplicate
        blockstore.store_duplicate_slot(1, vec![], vec![]).unwrap();
        blockstore.store_duplicate_slot(3, vec![], vec![]).unwrap();
        blockstore.store_duplicate_slot(4, vec![], vec![]).unwrap();
        blockstore.store_duplicate_slot(5, vec![], vec![]).unwrap();

        let bank1 = bank_forks.write().unwrap().insert(Bank::new_from_parent(
            bank0.clone_without_scheduler(),
            &Pubkey::default(),
            1,
        ));
        confirm_full_slot(
            &blockstore,
            &bank1,
            &replay_tx_thread_pool,
            &ProcessOptions::default(),
            &recyclers,
            &mut ConfirmationProgress::new(bank0.last_blockhash()),
            None,
            None,
            None,
            &mut ExecuteTimings::default(),
        )
        .unwrap();

        bank_forks
            .write()
            .unwrap()
            .set_root(
                1,
                &solana_runtime::accounts_background_service::AbsRequestSender::default(),
                None,
            )
            .unwrap();

        let leader_schedule_cache = LeaderScheduleCache::new_from_bank(&bank1);

        // process_blockstore_from_root() from slot 1 onwards
        blockstore_processor::process_blockstore_from_root(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &ProcessOptions::default(),
            None,
            None,
            None,
            &AbsRequestSender::default(),
        )
        .unwrap();

        assert_eq!(bank_forks.read().unwrap().root(), 4);

        // Verify that fork choice can be initialized and that the root is not marked duplicate
        let (_progress, fork_choice) =
            ReplayStage::initialize_progress_and_fork_choice_with_locked_bank_forks(
                &bank_forks,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &blockstore,
            );

        let bank_forks = bank_forks.read().unwrap();
        // 4 (the artificial root) is the tree root and no longer duplicate
        assert_eq!(fork_choice.tree_root().0, 4);
        assert!(fork_choice
            .is_candidate(&(4, bank_forks.bank_hash(4).unwrap()))
            .unwrap());

        // 5 is still considered duplicate, so it is not a valid fork choice candidate
        assert!(!fork_choice
            .is_candidate(&(5, bank_forks.bank_hash(5).unwrap()))
            .unwrap());
    }

    #[test]
    fn test_skip_leader_slot_for_existing_slot() {
        solana_logger::setup();

        let ReplayBlockstoreComponents {
            blockstore,
            my_pubkey,
            leader_schedule_cache,
            poh_recorder,
            vote_simulator,
            rpc_subscriptions,
            ..
        } = replay_blockstore_components(None, 1, None);

        let VoteSimulator {
            bank_forks,
            mut progress,
            ..
        } = vote_simulator;

        let working_bank = bank_forks.read().unwrap().working_bank();
        assert!(working_bank.is_complete());
        assert!(working_bank.is_frozen());
        // Mark startup verification as complete to avoid skipping leader slots
        working_bank.set_startup_verification_complete();

        // Insert a block two slots greater than current bank. This slot does
        // not have a corresponding Bank in BankForks; this emulates a scenario
        // where the block had previously been created and added to BankForks,
        // but then got removed. This could be the case if the Bank was not on
        // the major fork.
        let dummy_slot = working_bank.slot() + 2;
        let initial_slot = working_bank.slot();
        let num_entries = 10;
        let merkle_variant = true;
        let (shreds, _) = make_slot_entries(dummy_slot, initial_slot, num_entries, merkle_variant);
        blockstore.insert_shreds(shreds, None, false).unwrap();

        // Reset PoH recorder to the completed bank to ensure consistent state
        ReplayStage::reset_poh_recorder(
            &my_pubkey,
            &blockstore,
            working_bank.clone(),
            &poh_recorder,
            &leader_schedule_cache,
        );

        // Register just over one slot worth of ticks directly with PoH recorder
        let num_poh_ticks =
            (working_bank.ticks_per_slot() * working_bank.hashes_per_tick().unwrap()) + 1;
        poh_recorder
            .write()
            .map(|mut poh_recorder| {
                for _ in 0..num_poh_ticks + 1 {
                    poh_recorder.tick();
                }
            })
            .unwrap();

        let poh_recorder = Arc::new(poh_recorder);
        let (retransmit_slots_sender, _) = unbounded();
        let (banking_tracer, _) = BankingTracer::new(None).unwrap();
        // A vote has not technically been rooted, but it doesn't matter for
        // this test to use true to avoid skipping the leader slot
        let has_new_vote_been_rooted = true;
        let track_transaction_indexes = false;

        // We should not attempt to start leader for the dummy_slot
        assert_matches!(
            poh_recorder.read().unwrap().reached_leader_slot(&my_pubkey),
            PohLeaderStatus::NotReached
        );

        assert!(!ReplayStage::maybe_start_leader(
            &my_pubkey,
            &bank_forks,
            &poh_recorder,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &retransmit_slots_sender,
            &mut SkippedSlotsInfo::default(),
            &banking_tracer,
            has_new_vote_been_rooted,
            track_transaction_indexes,
            &None,
            &mut false,
        ));

        // Register another slots worth of ticks  with PoH recorder
        poh_recorder
            .write()
            .map(|mut poh_recorder| {
                for _ in 0..num_poh_ticks + 1 {
                    poh_recorder.tick();
                }
            })
            .unwrap();

        // We should now start leader for dummy_slot + 1
        let good_slot = dummy_slot + 1;
        assert!(ReplayStage::maybe_start_leader(
            &my_pubkey,
            &bank_forks,
            &poh_recorder,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &None,
            &mut progress,
            &retransmit_slots_sender,
            &mut SkippedSlotsInfo::default(),
            &banking_tracer,
            has_new_vote_been_rooted,
            track_transaction_indexes,
            &None,
            &mut false,
        ));
        // Get the new working bank, which is also the new leader bank/slot
        let working_bank = bank_forks.read().unwrap().working_bank();
        // The new bank's slot must NOT be dummy_slot as the blockstore already
        // had a shred inserted for dummy_slot prior to maybe_start_leader().
        // maybe_start_leader() must not pick dummy_slot to avoid creating a
        // duplicate block.
        assert_eq!(working_bank.slot(), good_slot);
        assert_eq!(working_bank.parent_slot(), initial_slot);
    }

    #[test]
    #[should_panic(expected = "Additional duplicate confirmed notification for slot 6")]
    fn test_mark_slots_duplicate_confirmed() {
        let generate_votes = |pubkeys: Vec<Pubkey>| {
            pubkeys
                .into_iter()
                .zip(iter::once(vec![0, 1, 2, 5, 6]).chain(iter::repeat(vec![0, 1, 3, 4]).take(2)))
                .collect()
        };
        let tree = tr(0) / (tr(1) / (tr(3) / (tr(4))) / (tr(2) / (tr(5) / (tr(6)))));
        let (vote_simulator, blockstore) =
            setup_forks_from_tree(tree, 3, Some(Box::new(generate_votes)));
        let VoteSimulator {
            bank_forks,
            mut tbft_structs,
            mut progress,
            ..
        } = vote_simulator;

        let (ancestor_hashes_replay_update_sender, _) = unbounded();
        let mut duplicate_confirmed_slots = DuplicateConfirmedSlots::default();
        let bank_hash_0 = bank_forks.read().unwrap().bank_hash(0).unwrap();
        bank_forks
            .write()
            .unwrap()
            .set_root(1, &AbsRequestSender::default(), None)
            .unwrap();

        // Mark 0 as duplicate confirmed, should fail as it is 0 < root
        let confirmed_slots = [(0, bank_hash_0)];
        ReplayStage::mark_slots_duplicate_confirmed(
            &confirmed_slots,
            &blockstore,
            &bank_forks,
            &mut progress,
            &mut DuplicateSlotsTracker::default(),
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut EpochSlotsFrozenSlots::default(),
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            &mut duplicate_confirmed_slots,
        );

        assert!(!duplicate_confirmed_slots.contains_key(&0));

        // Mark 5 as duplicate confirmed, should suceed
        let bank_hash_5 = bank_forks.read().unwrap().bank_hash(5).unwrap();
        let confirmed_slots = [(5, bank_hash_5)];

        ReplayStage::mark_slots_duplicate_confirmed(
            &confirmed_slots,
            &blockstore,
            &bank_forks,
            &mut progress,
            &mut DuplicateSlotsTracker::default(),
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut EpochSlotsFrozenSlots::default(),
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            &mut duplicate_confirmed_slots,
        );

        assert_eq!(*duplicate_confirmed_slots.get(&5).unwrap(), bank_hash_5);
        assert!(tbft_structs
            .heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(5, bank_hash_5))
            .unwrap_or(false));

        // Mark 5 and 6 as duplicate confirmed, should succeed
        let bank_hash_6 = bank_forks.read().unwrap().bank_hash(6).unwrap();
        let confirmed_slots = [(5, bank_hash_5), (6, bank_hash_6)];

        ReplayStage::mark_slots_duplicate_confirmed(
            &confirmed_slots,
            &blockstore,
            &bank_forks,
            &mut progress,
            &mut DuplicateSlotsTracker::default(),
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut EpochSlotsFrozenSlots::default(),
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            &mut duplicate_confirmed_slots,
        );

        assert_eq!(*duplicate_confirmed_slots.get(&5).unwrap(), bank_hash_5);
        assert!(tbft_structs
            .heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(5, bank_hash_5))
            .unwrap_or(false));
        assert_eq!(*duplicate_confirmed_slots.get(&6).unwrap(), bank_hash_6);
        assert!(tbft_structs
            .heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(6, bank_hash_6))
            .unwrap_or(false));

        // Mark 6 as duplicate confirmed again with a different hash, should panic
        let confirmed_slots = [(6, Hash::new_unique())];
        ReplayStage::mark_slots_duplicate_confirmed(
            &confirmed_slots,
            &blockstore,
            &bank_forks,
            &mut progress,
            &mut DuplicateSlotsTracker::default(),
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut EpochSlotsFrozenSlots::default(),
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
            &mut duplicate_confirmed_slots,
        );
    }

    #[test_case(true ; "same_batch")]
    #[test_case(false ; "seperate_batches")]
    #[should_panic(expected = "Additional duplicate confirmed notification for slot 6")]
    fn test_process_duplicate_confirmed_slots(same_batch: bool) {
        let generate_votes = |pubkeys: Vec<Pubkey>| {
            pubkeys
                .into_iter()
                .zip(iter::once(vec![0, 1, 2, 5, 6]).chain(iter::repeat(vec![0, 1, 3, 4]).take(2)))
                .collect()
        };
        let tree = tr(0) / (tr(1) / (tr(3) / (tr(4))) / (tr(2) / (tr(5) / (tr(6)))));
        let (vote_simulator, blockstore) =
            setup_forks_from_tree(tree, 3, Some(Box::new(generate_votes)));
        let VoteSimulator {
            bank_forks,
            mut tbft_structs,
            progress,
            ..
        } = vote_simulator;

        let (ancestor_hashes_replay_update_sender, _) = unbounded();
        let (sender, receiver) = unbounded();
        let mut duplicate_confirmed_slots = DuplicateConfirmedSlots::default();
        let bank_hash_0 = bank_forks.read().unwrap().bank_hash(0).unwrap();
        bank_forks
            .write()
            .unwrap()
            .set_root(1, &AbsRequestSender::default(), None)
            .unwrap();

        // Mark 0 as duplicate confirmed, should fail as it is 0 < root
        sender.send(vec![(0, bank_hash_0)]).unwrap();

        ReplayStage::process_duplicate_confirmed_slots(
            &receiver,
            &blockstore,
            &mut DuplicateSlotsTracker::default(),
            &mut duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &bank_forks,
            &progress,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
        );

        assert!(!duplicate_confirmed_slots.contains_key(&0));

        // Mark 5 as duplicate confirmed, should succed
        let bank_hash_5 = bank_forks.read().unwrap().bank_hash(5).unwrap();
        sender.send(vec![(5, bank_hash_5)]).unwrap();

        ReplayStage::process_duplicate_confirmed_slots(
            &receiver,
            &blockstore,
            &mut DuplicateSlotsTracker::default(),
            &mut duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &bank_forks,
            &progress,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
        );

        assert_eq!(*duplicate_confirmed_slots.get(&5).unwrap(), bank_hash_5);
        assert!(tbft_structs
            .heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(5, bank_hash_5))
            .unwrap_or(false));

        // Mark 5 and 6 as duplicate confirmed, should suceed
        let bank_hash_6 = bank_forks.read().unwrap().bank_hash(6).unwrap();
        if same_batch {
            sender
                .send(vec![(5, bank_hash_5), (6, bank_hash_6)])
                .unwrap();
        } else {
            sender.send(vec![(5, bank_hash_5)]).unwrap();
            sender.send(vec![(6, bank_hash_6)]).unwrap();
        }

        ReplayStage::process_duplicate_confirmed_slots(
            &receiver,
            &blockstore,
            &mut DuplicateSlotsTracker::default(),
            &mut duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &bank_forks,
            &progress,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
        );

        assert_eq!(*duplicate_confirmed_slots.get(&5).unwrap(), bank_hash_5);
        assert!(tbft_structs
            .heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(5, bank_hash_5))
            .unwrap_or(false));
        assert_eq!(*duplicate_confirmed_slots.get(&6).unwrap(), bank_hash_6);
        assert!(tbft_structs
            .heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(6, bank_hash_6))
            .unwrap_or(false));

        // Mark 6 as duplicate confirmed again with a different hash, should panic
        sender.send(vec![(6, Hash::new_unique())]).unwrap();

        ReplayStage::process_duplicate_confirmed_slots(
            &receiver,
            &blockstore,
            &mut DuplicateSlotsTracker::default(),
            &mut duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &bank_forks,
            &progress,
            &mut tbft_structs.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            &mut PurgeRepairSlotCounter::default(),
        );
    }
}
