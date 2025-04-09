//! The Alpenglow voting loop, handles all three types of votes as well as
//! rooting, leader logic, and dumping and repairing the notarized versions.
use {
    crate::{
        alpenglow_consensus::{
            certificate_pool::CertificatePool,
            vote_history::VoteHistory,
            vote_history_storage::{SavedVoteHistory, SavedVoteHistoryVersions},
        },
        banking_trace::BankingTracer,
        commitment_service::{
            AlpenglowCommitmentAggregationData, AlpenglowCommitmentType, CommitmentAggregationData,
        },
        consensus::progress_map::ProgressMap,
        replay_stage::{Finalizer, ReplayStage, MAX_VOTE_SIGNATURES},
        voting_service::VoteOp,
    },
    alpenglow_vote::vote::Vote,
    crossbeam_channel::Sender,
    solana_feature_set::FeatureSet,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::{last_of_consecutive_leader_slots, leader_slot_index},
    },
    solana_measure::measure::Measure,
    solana_poh::{
        poh_recorder::{PohRecorder, GRACE_TICKS_FACTOR, MAX_GRACE_SLOTS},
        poh_service,
    },
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions, slot_status_notifier::SlotStatusNotifier,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender,
        bank::{Bank, NewBankOptions},
        bank_forks::BankForks,
        installed_scheduler_pool::BankWithScheduler,
        root_bank_cache::RootBankCache,
        vote_sender_types::AlpenglowVoteReceiver as VoteReceiver,
    },
    solana_sdk::{
        clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        timing::timestamp,
        transaction::{Transaction, VersionedTransaction},
    },
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Instant,
    },
};

/// Inputs to the voting loop
pub struct VotingLoopConfig {
    pub exit: Arc<AtomicBool>,
    // Validator config
    pub vote_account: Pubkey,
    pub wait_for_vote_to_start_leader: bool,
    pub wait_to_vote_slot: Option<Slot>,
    pub track_transaction_indexes: bool,

    // Shared state
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub blockstore: Arc<Blockstore>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub cluster_info: Arc<ClusterInfo>,
    pub poh_recorder: Arc<RwLock<PohRecorder>>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub rpc_subscriptions: Arc<RpcSubscriptions>,
    pub banking_tracer: Arc<BankingTracer>,

    // Senders
    pub accounts_background_request_sender: AbsRequestSender,
    pub voting_sender: Sender<VoteOp>,
    pub lockouts_sender: Sender<CommitmentAggregationData>,
    pub drop_bank_sender: Sender<Vec<BankWithScheduler>>,
    pub bank_notification_sender: Option<BankNotificationSenderConfig>,
    pub slot_status_notifier: Option<SlotStatusNotifier>,

    // Receivers
    pub vote_receiver: VoteReceiver,
    pub replay_highest_frozen: Arc<ReplayHighestFrozen>,
}

/// Context required to construct vote transactions
struct VotingContext {
    vote_history: VoteHistory,
    vote_account_pubkey: Pubkey,
    identity_keypair: Arc<Keypair>,
    authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    has_new_vote_been_rooted: bool,
    voting_sender: Sender<VoteOp>,
    wait_to_vote_slot: Option<Slot>,
    voted_signatures: Vec<Signature>,
}

/// Context shared with replay, gossip, banking stage etc
struct SharedContext {
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    bank_forks: Arc<RwLock<BankForks>>,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    rpc_subscriptions: Arc<RpcSubscriptions>,
    banking_tracer: Arc<BankingTracer>,
    vote_receiver: VoteReceiver,
    replay_highest_frozen: Arc<ReplayHighestFrozen>,
    // TODO(ashwin): share this with replay (currently empty)
    progress: ProgressMap,
    // TODO(ashwin): integrate with gossip set-identity
    my_pubkey: Pubkey,
}

#[derive(Default)]
pub struct ReplayHighestFrozen {
    pub highest_frozen_slot: Mutex<Slot>,
    pub freeze_notification: Condvar,
}

pub(crate) enum GenerateVoteTxResult {
    // non voting validator, not eligible for refresh
    // until authorized keypair is overriden
    NonVoting,
    // hot spare validator, not eligble for refresh
    // until set identity is invoked
    HotSpare,
    // failed generation, eligible for refresh
    Failed,
    Tx(Transaction),
}

impl GenerateVoteTxResult {
    pub(crate) fn is_non_voting(&self) -> bool {
        matches!(self, Self::NonVoting)
    }

    pub(crate) fn is_hot_spare(&self) -> bool {
        matches!(self, Self::HotSpare)
    }
}

enum StartLeaderStatus {
    /// Successfully started leader block production
    StartedLeader,

    /// Missed our slot, either:
    /// - Something higher has been notarized/finalized
    /// - Our leader window has been skip certified
    MissedSlot,

    /// Replay has not yet frozen the parent slot
    ReplayIsBehind(/* parent slot */ Slot),

    /// Startup verification is not yet complete
    StartupVerificationIncomplete,

    /// Bank forks already contains bank
    AlreadyHaveBank,

    /// Haven't landed a vote
    VoteNotRooted,
}

pub struct VotingLoop {
    t_voting_loop: JoinHandle<()>,
}

impl VotingLoop {
    pub fn new(config: VotingLoopConfig) -> Self {
        let voting_loop = move || {
            Self::voting_loop(config);
        };

        let t_voting_loop = Builder::new()
            .name("solVotingLoop".to_string())
            .spawn(voting_loop)
            .unwrap();

        Self { t_voting_loop }
    }

    fn voting_loop(config: VotingLoopConfig) {
        let VotingLoopConfig {
            exit,
            vote_account,
            wait_for_vote_to_start_leader,
            wait_to_vote_slot,
            track_transaction_indexes,
            authorized_voter_keypairs,
            blockstore,
            bank_forks,
            cluster_info,
            poh_recorder,
            leader_schedule_cache,
            rpc_subscriptions,
            banking_tracer,
            accounts_background_request_sender,
            voting_sender,
            lockouts_sender,
            drop_bank_sender,
            bank_notification_sender,
            slot_status_notifier,
            vote_receiver,
            replay_highest_frozen,
        } = config;

        let _exit = Finalizer::new(exit.clone());
        let mut root_bank_cache = RootBankCache::new(bank_forks.clone());
        let mut cert_pool = CertificatePool::new_from_root_bank(&root_bank_cache.root_bank());

        let mut current_slot = root_bank_cache.root_bank().slot() + 1;
        let mut current_leader = None;

        let identity_keypair = cluster_info.keypair().clone();
        let my_pubkey = identity_keypair.pubkey();
        let has_new_vote_been_rooted = !wait_for_vote_to_start_leader;
        // TODO: Properly initialize VoteHistory from storage
        let vote_history = VoteHistory::new(my_pubkey, root_bank_cache.root_bank().slot());
        // TODO(ashwin): handle set identity here and in loop
        // reminder prev 3 need to be mutable
        if my_pubkey != vote_history.node_pubkey {
            todo!();
        }

        let mut voting_context = VotingContext {
            vote_history,
            vote_account_pubkey: vote_account,
            identity_keypair,
            authorized_voter_keypairs,
            has_new_vote_been_rooted,
            voting_sender,
            wait_to_vote_slot,
            voted_signatures: vec![],
        };
        let mut shared_context = SharedContext {
            blockstore: blockstore.clone(),
            leader_schedule_cache: leader_schedule_cache.clone(),
            bank_forks: bank_forks.clone(),
            poh_recorder: poh_recorder.clone(),
            rpc_subscriptions: rpc_subscriptions.clone(),
            banking_tracer,
            vote_receiver,
            replay_highest_frozen,
            progress: ProgressMap::default(),
            my_pubkey,
        };

        // Reset poh recorder to root bank for things that still depend on poh accuracy
        ReplayStage::reset_poh_recorder(
            &my_pubkey,
            blockstore.as_ref(),
            root_bank_cache.root_bank(),
            poh_recorder.as_ref(),
            &leader_schedule_cache,
        );

        // TODO(ashwin): Start loop once migration is complete current_slot from vote history
        loop {
            let leader_end_slot = last_of_consecutive_leader_slots(current_slot);
            let mut skipped = false;

            let Some(leader_pubkey) = leader_schedule_cache
                .slot_leader_at(current_slot, Some(root_bank_cache.root_bank().as_ref()))
            else {
                error!("Unable to compute the leader at slot {current_slot}. Something is wrong, exiting");
                return;
            };
            let is_leader = leader_pubkey == my_pubkey;

            // Create a timer for the leader window
            let skip_timer = Instant::now();
            let timeouts: Vec<_> = (0..(NUM_CONSECUTIVE_LEADER_SLOTS as usize))
                .map(poh_service::skip_timeout)
                .collect();

            // If we are the leader for this window, produce a block. We choose to block here as
            // building a block is the highest priority
            ReplayStage::log_leader_change(
                &my_pubkey,
                current_slot,
                &mut current_leader,
                &leader_pubkey,
            );
            if is_leader {
                info!("{my_pubkey}: Reached our leader window {current_slot} to {leader_end_slot}. Inserting the first bank");
                if !Self::start_first_leader_block(
                    current_slot,
                    &mut cert_pool,
                    &slot_status_notifier,
                    track_transaction_indexes,
                    skip_timer,
                    &shared_context,
                ) {
                    // Even if the leader was not successfully started for this window, it is fine to continue to the voting
                    // portion of the loop as we will skip our own leader slots
                    error!("Unable to start our leader window for {current_slot} to {leader_end_slot}. Skipping this window");
                }
            }

            while current_slot <= leader_end_slot {
                let leader_slot_index = leader_slot_index(current_slot);
                let timeout = timeouts[leader_slot_index];
                let cert_log_timer = Instant::now();
                let mut skip_refresh_timer = Instant::now();
                let mut notarized = false;

                info!(
                    "{my_pubkey}: Entering voting loop for slot: {current_slot}, is_leader: {is_leader}. Skip timer {} vs timeout {}",
                    skip_timer.elapsed().as_millis(), timeout.as_millis()
                );

                while !Self::branch_notarized(&my_pubkey, current_slot, &cert_pool)
                    && !Self::skip_certified(&my_pubkey, current_slot, &cert_pool)
                {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }

                    // We use a blocking receive if skipped is true,
                    // as we can only progress on a certificate
                    Self::ingest_votes_into_certificate_pool(
                        &my_pubkey,
                        &shared_context.vote_receiver,
                        /* block */ skipped,
                        &mut cert_pool,
                    );

                    if !skipped && skip_timer.elapsed().as_millis() > timeout.as_millis() {
                        skipped = true;
                        Self::vote_skip(
                            &my_pubkey,
                            current_slot,
                            leader_end_slot,
                            &mut root_bank_cache,
                            &mut cert_pool,
                            &mut voting_context,
                        );
                        skip_refresh_timer = Instant::now();
                    }

                    // TODO: figure out proper refresh timing
                    if skipped && skip_refresh_timer.elapsed().as_millis() > 1000 {
                        // Ensure that we refresh in case something went wrong
                        Self::refresh_skip(
                            &my_pubkey,
                            &mut root_bank_cache,
                            &mut cert_pool,
                            &mut voting_context,
                        );
                        skip_refresh_timer = Instant::now();
                    }

                    if skipped || notarized {
                        continue;
                    }

                    // Check if replay has successfully completed and the bank passes
                    // the validation conditions
                    let Some(bank) = bank_forks.read().unwrap().get(current_slot) else {
                        continue;
                    };
                    if !bank.is_frozen() {
                        continue;
                    };
                    if !Self::can_vote_notarize(current_slot, bank.parent_slot(), &cert_pool) {
                        continue;
                    }

                    // Vote notarize
                    if Self::vote_notarize(
                        &my_pubkey,
                        bank.as_ref(),
                        is_leader,
                        &blockstore,
                        &mut cert_pool,
                        &mut voting_context,
                    ) {
                        notarized = true;
                        Self::alpenglow_update_commitment_cache(
                            AlpenglowCommitmentType::Notarize,
                            current_slot,
                            &lockouts_sender,
                        );
                    }
                }

                info!(
                    "{my_pubkey}: Slot {current_slot} certificate observed in {} ms. Skip timer {} vs timeout {}",
                    cert_log_timer.elapsed().as_millis(),
                    skip_timer.elapsed().as_millis(),
                    timeout.as_millis(),
                );

                if !skipped && Self::branch_notarized(&my_pubkey, current_slot, &cert_pool) {
                    if let Some(bank) = bank_forks.read().unwrap().get(current_slot) {
                        if bank.is_frozen() {
                            Self::vote_finalize(
                                &my_pubkey,
                                bank.slot(),
                                &mut root_bank_cache,
                                &mut cert_pool,
                                &mut voting_context,
                            );
                        } else {
                            // TODO: could consider voting later
                            warn!("Replay is catching up skipping finalize vote on {current_slot}");
                        }
                    } else {
                        warn!("Block is still being ingested skipping finalize vote on {current_slot}");
                    }
                }

                current_slot += 1;
            }

            // Set new root
            if let Some(new_root) = Self::maybe_set_root(
                leader_end_slot,
                &mut cert_pool,
                &accounts_background_request_sender,
                &bank_notification_sender,
                &drop_bank_sender,
                &mut shared_context,
                &mut voting_context,
            ) {
                // TODO(ashwin): this can happen at anytime, doesn't need to wait to here to let rpc know
                Self::alpenglow_update_commitment_cache(
                    AlpenglowCommitmentType::Root,
                    new_root,
                    &lockouts_sender,
                );
            }
        }
    }

    fn branch_notarized(my_pubkey: &Pubkey, slot: Slot, cert_pool: &CertificatePool) -> bool {
        if let Some(size) = cert_pool.get_notarization_cert_size(slot) {
            info!(
                "{my_pubkey}: Branch Notarized: Slot {} from {} validators",
                slot, size
            );
            return true;
        };

        false
    }

    fn skip_certified(my_pubkey: &Pubkey, slot: Slot, cert_pool: &CertificatePool) -> bool {
        // TODO(ashwin): can include cert size for debugging
        if cert_pool.skip_certified(slot) {
            info!("{my_pubkey}: Skip Certified: Slot {}", slot,);
            return true;
        }

        false
    }

    /// Starts production of the leader block for `slot`.
    /// Assumes that we are the leader for `slot`.
    /// This function will block on replay or certificate ingestion until we
    /// can produce the block. It only fails and returns false if:
    /// - We missed our slot
    /// - Startup verification is incomplete
    /// - We already have a bank for `slot` in `bank_forks`
    /// - We have not landed a vote
    fn start_first_leader_block(
        slot: Slot,
        cert_pool: &mut CertificatePool,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        track_transaction_indexes: bool,
        skip_timer: Instant,
        ctx: &SharedContext,
    ) -> bool {
        loop {
            match Self::maybe_start_leader(
                slot,
                cert_pool,
                slot_status_notifier,
                track_transaction_indexes,
                skip_timer,
                ctx,
            ) {
                StartLeaderStatus::StartedLeader => return true,
                StartLeaderStatus::MissedSlot => return false,
                StartLeaderStatus::VoteNotRooted => return false,
                StartLeaderStatus::StartupVerificationIncomplete => return false,
                StartLeaderStatus::AlreadyHaveBank => {
                    // TODO: Verify that the blockstore shred check is still happening somewhere
                    warn!(
                        "Bank forks already has a bank for our leader slot {slot}.
                        This indicates a restart, skipping production of {slot}"
                    );
                    return false;
                }
                StartLeaderStatus::ReplayIsBehind(parent_slot) => {
                    // In this case replay has not ended for the previous leader, however
                    // we have already observed a certificate for the previous leader.
                    // We will block and attempt to produce our block. If we are so far behind
                    // such that replay does not finish before the skip certificate for our leader
                    // block arrives, we will hit the `MissedSlot` case above and abort our entire leader
                    // window.
                    // TODO: For optimistic block production even if we `MissedSlot` our first leader
                    // block we should still try to produce our second leader block if replay finishes
                    // in time.
                    info!(
                        "{}: We want to produce {slot} off of notarized slot {parent_slot},
                        however {parent_slot} has not finished replay,
                        we are running behind so delaying our leader block",
                        ctx.my_pubkey
                    );
                    let mut highest_frozen_slot = ctx
                        .replay_highest_frozen
                        .highest_frozen_slot
                        .lock()
                        .unwrap();
                    while *highest_frozen_slot < parent_slot {
                        highest_frozen_slot = ctx
                            .replay_highest_frozen
                            .freeze_notification
                            .wait(highest_frozen_slot)
                            .unwrap();
                    }
                }
            }

            Self::ingest_votes_into_certificate_pool(
                &ctx.my_pubkey,
                &ctx.vote_receiver,
                /* block */ false,
                cert_pool,
            );
        }
    }

    /// Checks if we are set to produce a leader block for `slot`:
    /// - Is the highest notarization/finalized slot from `cert_pool` frozen
    /// - Startup verification is complete
    /// - Bank forks does not already contain a bank for `slot`
    /// - If `wait_for_vote_to_start_leader` is set, we have landed a vote
    ///
    /// If checks pass we return `StartLeaderStatus::StartedLeader` and:
    /// - Reset poh to the `parent_slot` (highest notarized/finalized slot)
    /// - Create a new bank for `slot` with parent `parent_slot`
    /// - Insert into bank_forks and poh recorder
    /// - Add the transactions for the notarization certificate and skip certificate
    fn maybe_start_leader(
        slot: Slot,
        cert_pool: &mut CertificatePool,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        track_transaction_indexes: bool,
        skip_timer: Instant,
        ctx: &SharedContext,
    ) -> StartLeaderStatus {
        if cert_pool.skip_certified(slot) {
            // We have missed our leader slot
            warn!(
                "Slot {slot} has already been skip certified,
                skipping production of {slot}"
            );
            return StartLeaderStatus::MissedSlot;
        }

        // TODO(ashwin): We max with root here, as the snapshot slot might not have a certificate.
        // Think about this more and fix if necessary.
        let parent_slot = cert_pool
            .highest_not_skip_certificate_slot()
            .max(ctx.bank_forks.read().unwrap().root());

        if parent_slot >= slot {
            // We have missed our leader slot
            warn!(
                "Slot {parent_slot} has already been notarized / finalized,
                skipping production of {slot}"
            );
            return StartLeaderStatus::MissedSlot;
        }

        let Some(parent_bank) = ctx.bank_forks.read().unwrap().get(parent_slot) else {
            return StartLeaderStatus::ReplayIsBehind(parent_slot);
        };

        if !parent_bank.is_frozen() {
            return StartLeaderStatus::ReplayIsBehind(parent_slot);
        }

        if !parent_bank.is_startup_verification_complete() {
            info!(
                "{}: Startup verification incomplete, skipping my leader slot {slot}",
                ctx.my_pubkey
            );
            return StartLeaderStatus::StartupVerificationIncomplete;
        }

        if ctx.bank_forks.read().unwrap().get(slot).is_some() {
            return StartLeaderStatus::AlreadyHaveBank;
        }

        // TODO(ashwin): plug this in from replay
        let has_new_vote_been_rooted = true;
        if !has_new_vote_been_rooted {
            info!(
                "{}: Have not landed a vote, skipping my leader slot {slot}",
                ctx.my_pubkey
            );
            return StartLeaderStatus::VoteNotRooted;
        }

        info!(
            "{}: Checking leader decision slot {slot} parent {parent_slot}",
            ctx.my_pubkey
        );
        if !cert_pool.make_start_leader_decision(
            slot,
            parent_slot,
            // We only need skip certificates for slots > the slot at which alpenglow is enabled
            // TODO(ashwin): plumb in to support migration
            /*first_alpenglow_slot*/
            0,
        ) {
            panic!(
                "Unable to get notarization or skip certificate to build a leader block \
                for slot {slot} descending from parent slot {parent_slot}. Something has \
                gone wrong with the timer loop"
            );
        };

        // Insert the bank
        Self::insert_first_leader_bank(
            slot,
            parent_bank,
            slot_status_notifier,
            track_transaction_indexes,
            skip_timer,
            ctx,
        )
    }

    /// Inserts the first leader bank `slot` of this window
    /// This function will hold the `poh_recorder` write lock
    /// to prevent the block creation loop from progressing until
    /// the new bank is set in both `bank_forks` and the `poh_recorder`.
    fn insert_first_leader_bank(
        slot: Slot,
        parent_bank: Arc<Bank>,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        track_transaction_indexes: bool,
        skip_timer: Instant,
        ctx: &SharedContext,
    ) -> StartLeaderStatus {
        let parent_slot = parent_bank.slot();
        let root_slot = ctx.bank_forks.read().unwrap().root();
        trace!("maybe_start_leader poh_recorder write lock");
        let mut w_poh_recorder = ctx.poh_recorder.write().unwrap();
        if let Some(bank) = w_poh_recorder.bank() {
            // This indicates we were producing a block in the previous window, however our
            // block got skipped before we finished production. At this point we should abandon
            // our previous window and ensure that we produce a block for this window
            warn!(
                "{}: Attempting to produce a block for {slot}, however we still are in production of \
                {}. This indicates that we were too slow and {slot} has already been skipped. Clearing our bank.",
                ctx.my_pubkey,
                bank.slot(),
            );

            // Let the block creation loop know to abandon building the previous leader window
            w_poh_recorder.clear_bank_due_to_preemption(slot);
        }

        if w_poh_recorder.start_slot() != parent_slot {
            // Important to keep Poh somewhat accurate for
            // parts of the system relying on PohRecorder::would_be_leader()
            //
            // TODO: On migration need to keep the ticks around for parent slots in previous epoch
            // because reset below will delete those ticks
            trace!("Resetting poh to {parent_slot}");
            let next_leader_slot = ctx.leader_schedule_cache.next_leader_slot(
                &ctx.my_pubkey,
                parent_slot,
                &parent_bank,
                Some(ctx.blockstore.as_ref()),
                GRACE_TICKS_FACTOR * MAX_GRACE_SLOTS,
            );

            info!("resetting poh_recorder to {}", parent_slot);
            w_poh_recorder.reset(parent_bank.clone(), next_leader_slot);
        }

        let tpu_bank = ReplayStage::new_bank_from_parent_with_notify(
            parent_bank.clone(),
            slot,
            root_slot,
            &ctx.my_pubkey,
            &ctx.rpc_subscriptions,
            slot_status_notifier,
            NewBankOptions::default(),
        );
        // make sure parent is frozen for finalized hashes via the above
        // new()-ing of its child bank
        ctx.banking_tracer.hash_event(
            parent_slot,
            &parent_bank.last_blockhash(),
            &parent_bank.hash(),
        );

        // Insert the bank
        let tpu_bank = ctx.bank_forks.write().unwrap().insert(tpu_bank);
        let poh_bank_start =
            w_poh_recorder.set_bank(tpu_bank, track_transaction_indexes, Some(skip_timer));
        // TODO: cleanup, this is no longer needed
        poh_bank_start
            .contains_valid_certificate
            .store(true, Ordering::Relaxed);

        info!(
            "{}: new fork:{} parent:{} (leader) root:{}",
            ctx.my_pubkey, slot, parent_slot, root_slot
        );

        StartLeaderStatus::StartedLeader
    }

    /// Checks if any slots between `vote_history`'s current root
    /// and `slot` have received a finalization certificate and are frozen
    ///
    /// If so, set the root as the highest slot that fits these conditions
    /// and return the root
    fn maybe_set_root(
        slot: Slot,
        cert_pool: &mut CertificatePool,
        accounts_background_request_sender: &AbsRequestSender,
        bank_notification_sender: &Option<BankNotificationSenderConfig>,
        drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
        ctx: &mut SharedContext,
        vctx: &mut VotingContext,
    ) -> Option<Slot> {
        let old_root = vctx.vote_history.root;
        info!(
            "{}: Checking for finalization certificates between {old_root} and {slot}",
            ctx.my_pubkey
        );
        let new_root = (old_root + 1..=slot).rev().find(|slot| {
            cert_pool.get_finalization_cert_size(*slot).is_some()
                && ctx.bank_forks.read().unwrap().is_frozen(*slot)
        })?;
        trace!("{}: Attempting to set new root {new_root}", ctx.my_pubkey);
        vctx.vote_history.set_root(new_root);
        cert_pool.handle_new_root(ctx.bank_forks.read().unwrap().get(new_root).unwrap());
        if let Err(e) = ReplayStage::check_and_handle_new_root(
            &ctx.my_pubkey,
            slot,
            new_root,
            ctx.bank_forks.as_ref(),
            &mut ctx.progress,
            ctx.blockstore.as_ref(),
            &ctx.leader_schedule_cache,
            accounts_background_request_sender,
            &ctx.rpc_subscriptions,
            Some(new_root),
            bank_notification_sender,
            &mut vctx.has_new_vote_been_rooted,
            &mut vctx.voted_signatures,
            drop_bank_sender,
            None,
        ) {
            error!("Unable to set root: {e:?}");
            return None;
        }

        // Distinguish between duplicate versions of same slot
        let hash = ctx.bank_forks.read().unwrap().bank_hash(new_root).unwrap();
        if let Err(e) =
            ctx.blockstore
                .insert_optimistic_slot(new_root, &hash, timestamp().try_into().unwrap())
        {
            error!(
                "failed to record optimistic slot in blockstore: slot={}: {:?}",
                new_root, &e
            );
        }

        Some(new_root)
    }

    /// Create and send a skip vote for `[start, end]`
    fn vote_skip(
        my_pubkey: &Pubkey,
        start: Slot,
        end: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) {
        let bank = root_bank_cache.root_bank();
        info!(
            "{my_pubkey}: Voting skip for slot range [{start},{end}] with blockhash from {}",
            bank.slot()
        );
        for slot in start..=end {
            let vote = Vote::new_skip_vote(slot);
            Self::send_vote(vote, false, bank.as_ref(), cert_pool, voting_context);
        }
    }

    /// Create and send a finalization vote for `slot`
    fn vote_finalize(
        my_pubkey: &Pubkey,
        slot: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) {
        let bank = root_bank_cache.root_bank();
        info!(
            "{my_pubkey}: Voting finalize for slot {slot} with blockhash from {}",
            bank.slot()
        );
        let vote = Vote::new_finalization_vote(slot);
        Self::send_vote(vote, false, bank.as_ref(), cert_pool, voting_context);
    }

    /// Refresh the last skip vote
    fn refresh_skip(
        my_pubkey: &Pubkey,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) {
        // TODO(ashwin): Fix when doing voting loop for v0
        let Some(skip_vote) = voting_context.vote_history.skip_votes.last().copied() else {
            return;
        };
        let bank = root_bank_cache.root_bank();
        info!(
            "{my_pubkey}: Refreshing skip for slot {} with blockhash from {}",
            skip_vote.slot(),
            bank.slot()
        );
        Self::send_vote(skip_vote, true, bank.as_ref(), cert_pool, voting_context);
    }

    /// Determines if we can vote notarize at this time.
    /// If this is the first leader block of the window, we check for certificates,
    /// otherwise we ensure that the parent slot is consecutive.
    fn can_vote_notarize(slot: Slot, parent_slot: Slot, cert_pool: &CertificatePool) -> bool {
        let leader_slot_index = leader_slot_index(slot);
        if leader_slot_index == 0 {
            // Check if we have the certificates to vote on this block
            // TODO(ashwin): track by hash,
            // TODO: fix WFSM hack for 1
            if cert_pool.get_notarization_cert_size(parent_slot).is_none() && parent_slot > 1 {
                // Need to ingest more votes
                return false;
            }

            if !(parent_slot + 1..slot).all(|s| cert_pool.skip_certified(s)) {
                // Need to ingest more votes
                return false;
            }
            // TODO: fix WFSM hack for 1
        } else if parent_slot > 1 {
            // Only vote notarize if it is a consecutive slot
            if parent_slot + 1 != slot {
                // TODO(ashwin): can probably mark notarize here as we'll never be able to fix this
                return false;
            }
        }
        true
    }

    /// Create and send a Notarization or Finalization vote
    /// Attempts to retrieve the block id from blockstore.
    ///
    /// Returns false if we should attempt to retry this vote later
    /// because the block_id/bank hash is not yet populated.
    fn vote_notarize(
        my_pubkey: &Pubkey,
        bank: &Bank,
        is_leader: bool,
        blockstore: &Blockstore,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) -> bool {
        debug_assert!(bank.is_frozen());
        let slot = bank.slot();
        let hash = bank.hash();
        let Some(block_id) = blockstore
            .check_last_fec_set_and_get_block_id(
                slot,
                hash,
                is_leader,
                // TODO:(ashwin) ignore duplicate block checks (?)
                &FeatureSet::all_enabled(),
            ).unwrap_or_else(|e| {
                if !is_leader {
                    warn!("Unable to retrieve block_id, failed last fec set checks for slot {slot} hash {hash}: {e:?}")
                }
                None
            }
        ) else {
            if is_leader {
                // For leader slots, shredding is asynchronous so block_id might not yet
                // be available. In this case we want to retry our vote later
                return false;
            }
            // At this point we could mark the bank as dead similar to TowerBFT, however
            // for alpenglow this is not necessary
            warn!(
                "Unable to retrieve block id or duplicate block checks have failed
                for non leader slot {slot} {hash}, not voting notarize"
            );
            return true;
        };

        info!("{my_pubkey}: Voting notarize for slot {slot} hash {hash} block_id {block_id}");
        let vote = Vote::new_notarization_vote(slot, block_id, hash);
        Self::send_vote(vote, false, bank, cert_pool, voting_context);

        true
    }

    /// Send an alpenglow vote
    /// `bank` will be used for:
    /// - startup verifiation
    /// - vote account checks
    /// - authorized voter checks
    /// - selecting the blockhash to sign with
    ///
    /// For notarization & finalization votes this will be the voted bank
    /// for skip votes we need to ensure that the bank selected will be on
    /// the leader's choosen fork.
    #[allow(clippy::too_many_arguments)]
    fn send_vote(
        vote: Vote,
        is_refresh: bool,
        bank: &Bank,
        cert_pool: &mut CertificatePool,
        context: &mut VotingContext,
    ) {
        let mut generate_time = Measure::start("generate_alpenglow_vote");
        let vote_tx_result = Self::generate_vote_tx(&vote, bank, context);
        generate_time.stop();
        // TODO(ashwin): add metrics struct here and throughout the whole file
        // replay_timing.generate_vote_us += generate_time.as_us();
        let GenerateVoteTxResult::Tx(vote_tx) = vote_tx_result else {
            return;
        };
        if let Err(e) =
            cert_pool.add_vote(&vote, vote_tx.clone().into(), &context.vote_account_pubkey)
        {
            if !is_refresh {
                warn!("Unable to push our own vote into the pool {}", e);
                return;
            }
        };

        // Update and save the vote history
        match vote {
            Vote::Notarize(_) => context.vote_history.latest_notarize_vote = vote,
            Vote::Skip(..) => context.vote_history.push_skip_vote(vote),
            Vote::Finalize(_) => context.vote_history.latest_finalize_vote = vote,
            _ => todo!(),
        }
        let saved_vote_history =
            SavedVoteHistory::new(&context.vote_history, &context.identity_keypair).unwrap_or_else(
                |err| {
                    error!("Unable to create saved vote history: {:?}", err);
                    std::process::exit(1);
                },
            );

        // Send the vote over the wire
        context
            .voting_sender
            .send(VoteOp::PushAlpenglowVote {
                tx: vote_tx,
                slot: bank.slot(),
                saved_vote_history: SavedVoteHistoryVersions::from(saved_vote_history),
            })
            .unwrap_or_else(|err| warn!("Error: {:?}", err));
    }

    fn generate_vote_tx(
        vote: &Vote,
        bank: &Bank,
        context: &mut VotingContext,
    ) -> GenerateVoteTxResult {
        let vote_account_pubkey = context.vote_account_pubkey;
        let authorized_voter_keypairs = context.authorized_voter_keypairs.read().unwrap();
        if !bank.is_startup_verification_complete() {
            info!("startup verification incomplete, so unable to vote");
            return GenerateVoteTxResult::Failed;
        }

        if authorized_voter_keypairs.is_empty() {
            return GenerateVoteTxResult::NonVoting;
        }
        if let Some(slot) = context.wait_to_vote_slot {
            if vote.slot() < slot {
                return GenerateVoteTxResult::Failed;
            }
        }
        let vote_account = match bank.get_vote_account(&context.vote_account_pubkey) {
            None => {
                warn!("Vote account {vote_account_pubkey} does not exist.  Unable to vote");
                return GenerateVoteTxResult::Failed;
            }
            Some(vote_account) => vote_account,
        };
        let vote_state = match vote_account.alpenglow_vote_state() {
            None => {
                warn!(
                    "Vote account {vote_account_pubkey} does not have an Alpenglow vote state.  Unable to vote",
                );
                return GenerateVoteTxResult::Failed;
            }
            Some(vote_state) => vote_state,
        };
        if *vote_state.node_pubkey() != context.identity_keypair.pubkey() {
            info!(
                "Vote account node_pubkey mismatch: {} (expected: {}).  Unable to vote",
                vote_state.node_pubkey(),
                context.identity_keypair.pubkey()
            );
            return GenerateVoteTxResult::HotSpare;
        }

        let Some(authorized_voter_pubkey) = vote_state.get_authorized_voter(bank.epoch()) else {
            warn!(
                "Vote account {vote_account_pubkey} has no authorized voter for epoch {}.  Unable to vote",
                bank.epoch()
            );
            return GenerateVoteTxResult::Failed;
        };

        let authorized_voter_keypair = match authorized_voter_keypairs
            .iter()
            .find(|keypair| keypair.pubkey() == authorized_voter_pubkey)
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

        let vote_ix =
            vote.to_vote_instruction(vote_account_pubkey, authorized_voter_keypair.pubkey());
        let vote_tx = Transaction::new_signed_with_payer(
            &[vote_ix],
            Some(&context.identity_keypair.pubkey()),
            &[&context.identity_keypair, authorized_voter_keypair],
            bank.last_blockhash(),
        );

        if !context.has_new_vote_been_rooted {
            context.voted_signatures.push(vote_tx.signatures[0]);
            if context.voted_signatures.len() > MAX_VOTE_SIGNATURES {
                context.voted_signatures.remove(0);
            }
        } else {
            context.voted_signatures.clear();
        }

        GenerateVoteTxResult::Tx(vote_tx)
    }

    /// Ingest votes from all to all, tpu, and gossip into the certificate pool
    ///
    /// Returns the highest slot of the newly created notarization/skip certificates
    fn ingest_votes_into_certificate_pool(
        _my_pubkey: &Pubkey,
        vote_receiver: &VoteReceiver,
        block: bool,
        cert_pool: &mut CertificatePool,
    ) {
        let add_to_cert_pool =
            |(vote, vote_account_pubkey, tx): (Vote, Pubkey, VersionedTransaction)| {
                let _ = cert_pool.add_vote(&vote, tx, &vote_account_pubkey);
            };

        if block {
            let first = vote_receiver.recv().unwrap();
            std::iter::once(first)
                .chain(vote_receiver.try_iter())
                .for_each(add_to_cert_pool)
        } else {
            vote_receiver.try_iter().for_each(add_to_cert_pool)
        }
    }

    fn alpenglow_update_commitment_cache(
        commitment_type: AlpenglowCommitmentType,
        slot: Slot,
        lockouts_sender: &Sender<CommitmentAggregationData>,
    ) {
        if let Err(e) = lockouts_sender.send(
            CommitmentAggregationData::AlpenglowCommitmentAggregationData(
                AlpenglowCommitmentAggregationData {
                    commitment_type,
                    slot,
                },
            ),
        ) {
            trace!("lockouts_sender failed: {:?}", e);
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_voting_loop.join().map(|_| ())
    }
}
