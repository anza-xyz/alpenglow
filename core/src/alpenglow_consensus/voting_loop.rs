//! The Alpenglow voting loop, handles all three types of votes as well as
//! rooting, leader logic, and dumping and repairing the notarized versions.
use {
    super::{BLOCKTIME, DELTA, DELTA_TIMEOUT},
    crate::{
        alpenglow_consensus::{
            certificate_pool::{CertificatePool, StartLeaderCertificates},
            vote_history::VoteHistory,
            vote_history_storage::{SavedVoteHistory, SavedVoteHistoryVersions},
        },
        banking_stage::alpenglow_update_bank_forks_and_poh_recorder_for_new_tpu_bank,
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
    solana_poh::poh_recorder::{PohRecorder, TransactionRecorder},
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
        transaction::Transaction,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Display,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
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
    pub cert_tracker: Arc<RwLock<ReplayCertificateTracker>>,
    pub cluster_info: Arc<ClusterInfo>,
    pub poh_recorder: Arc<RwLock<PohRecorder>>,
    pub transaction_recorder: TransactionRecorder,
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
    transaction_recorder: TransactionRecorder,
    cert_tracker: Arc<RwLock<ReplayCertificateTracker>>,
    rpc_subscriptions: Arc<RpcSubscriptions>,
    banking_tracer: Arc<BankingTracer>,
    // TODO(ashwin): share this with replay (currently empty)
    progress: ProgressMap,
    // TODO(ashwin): integrate with gossip set-identity
    my_pubkey: Pubkey,
}

#[derive(Debug, Clone, Default)]
// TODO(ashwin): Evaluate alternatives to this
pub struct ReplayCertificateTracker {
    notarization_certificates: BTreeMap<Slot, usize>,
    skip_certificates: BTreeSet<Slot>,
}

impl ReplayCertificateTracker {
    pub(crate) fn new_rw_arc() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::default()))
    }

    pub(crate) fn add_notarization_certificate(&mut self, slot: Slot, size: usize) {
        self.notarization_certificates.insert(slot, size);
    }

    pub(crate) fn add_skip_certificates(&mut self, start: Slot, end: Slot) {
        for slot in start..=end {
            self.skip_certificates.insert(slot);
        }
    }

    fn highest_replay_notarized(&self) -> Option<Slot> {
        self.notarization_certificates.keys().max().copied()
    }

    fn replay_branch_notarized(&self, slot: Slot) -> Option<usize> {
        self.notarization_certificates.get(&slot).copied()
    }

    fn replay_skip_certified(&self, slot: Slot) -> bool {
        self.skip_certificates.contains(&slot)
    }

    fn set_root(&mut self, root: Slot) {
        self.notarization_certificates = self.notarization_certificates.split_off(&root);
        self.skip_certificates = self.skip_certificates.split_off(&root);
    }
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

/// Vote type which needs block id
enum BlockIdVote {
    Notarize,
    Finalize,
}

impl Display for BlockIdVote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Notarize => f.write_str("notarize"),
            Self::Finalize => f.write_str("finalize"),
        }
    }
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
            cert_tracker,
            cluster_info,
            poh_recorder,
            transaction_recorder,
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
        } = config;

        let _exit = Finalizer::new(exit.clone());
        let mut root_bank_cache = RootBankCache::new(bank_forks.clone());
        let mut cert_pool = CertificatePool::new();

        let mut current_slot = root_bank_cache.root_bank().slot() + 1;
        let mut last_leader_block = 0;
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
            transaction_recorder,
            cert_tracker: cert_tracker.clone(),
            rpc_subscriptions: rpc_subscriptions.clone(),
            banking_tracer,
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

            // TODO(ashwin): Do maybe_leader for all 4 blocks here, right now
            // instead we build each block synchronously below
            // if leader_schedule_cache.slot_leader_at(current_slot) {

            // }

            // Create a timer for the leader window
            let skip_timer = Instant::now();
            let timeouts: Vec<_> = (1..=(NUM_CONSECUTIVE_LEADER_SLOTS as u128))
                .map(|i| DELTA_TIMEOUT + i * BLOCKTIME + DELTA)
                .collect();

            while current_slot <= leader_end_slot {
                let leader_slot_index = leader_slot_index(current_slot);
                let timeout = timeouts[leader_slot_index];
                let cert_log_timer = Instant::now();
                let mut skip_refresh_timer = Instant::now();

                let leader_pubkey = leader_schedule_cache
                    .slot_leader_at(current_slot, Some(root_bank_cache.root_bank().as_ref()));
                let is_leader = leader_pubkey.map(|pk| pk == my_pubkey).unwrap_or(false);
                let mut notarized = false;

                info!(
                    "{my_pubkey}: Starting timer for slot: {current_slot}, is_leader: {is_leader}. Skip timer {} vs timeout {}",
                    skip_timer.elapsed().as_millis(), timeout
                );

                while !Self::branch_notarized(
                    &my_pubkey,
                    current_slot,
                    &cert_pool,
                    cert_tracker.as_ref(),
                ) && !Self::skip_certified(
                    &my_pubkey,
                    current_slot,
                    &mut root_bank_cache,
                    &mut cert_pool,
                    cert_tracker.as_ref(),
                ) {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    // If we're the leader try to start building a block
                    if last_leader_block < current_slot
                        && !poh_recorder.read().unwrap().has_bank()
                        && Self::maybe_start_leader(
                            &leader_pubkey,
                            current_slot,
                            &mut cert_pool,
                            &slot_status_notifier,
                            track_transaction_indexes,
                            &shared_context,
                        )
                    {
                        last_leader_block = current_slot;
                        ReplayStage::log_leader_change(
                            &my_pubkey,
                            current_slot,
                            &mut current_leader,
                            &my_pubkey,
                        );
                    }

                    if !skipped && skip_timer.elapsed().as_millis() > timeout {
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

                    // TODO(ashwin): we could block here & use return value if we've skipped and are
                    // waiting on a cert to avoid tight spin
                    Self::ingest_votes_into_certificate_pool(
                        &my_pubkey,
                        &vote_receiver,
                        &mut cert_pool,
                        bank_forks.as_ref(),
                    );

                    if skipped || notarized {
                        continue;
                    }

                    // Check if replay has successfully completed
                    let Some(bank) = bank_forks.read().unwrap().get(current_slot) else {
                        continue;
                    };
                    if !bank.is_frozen() {
                        continue;
                    };

                    // Vote notarize
                    if Self::vote(
                        &my_pubkey,
                        BlockIdVote::Notarize,
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
                    timeout,
                );

                if !skipped
                    && Self::branch_notarized(
                        &my_pubkey,
                        current_slot,
                        &cert_pool,
                        cert_tracker.as_ref(),
                    )
                {
                    if let Some(bank) = bank_forks.read().unwrap().get(current_slot) {
                        if bank.is_frozen() {
                            Self::vote(
                                &my_pubkey,
                                BlockIdVote::Finalize,
                                bank.as_ref(),
                                is_leader,
                                &blockstore,
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

            // TODO(ashwin): If we were the leader for `current_slot` and the bank has not completed,
            // we can abandon the bank now
        }
    }

    fn branch_notarized(
        my_pubkey: &Pubkey,
        slot: Slot,
        cert_pool: &CertificatePool,
        cert_tracker: &RwLock<ReplayCertificateTracker>,
    ) -> bool {
        if let Some(size) = cert_pool.is_notarization_certificate_complete(slot) {
            info!(
                "{my_pubkey}: Branch Notarized: Slot {} from {} validators",
                slot, size
            );
            return true;
        };
        if let Some(size) = cert_tracker.write().unwrap().replay_branch_notarized(slot) {
            info!(
                "{my_pubkey}: Branch Notarized (replay): Slot {} from {} validators",
                slot, size
            );
            return true;
        };

        false
    }

    fn skip_certified(
        my_pubkey: &Pubkey,
        slot: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        cert_tracker: &RwLock<ReplayCertificateTracker>,
    ) -> bool {
        let root_bank = root_bank_cache.root_bank();
        let epoch = root_bank.epoch_schedule().get_epoch(slot);
        // TODO(ashwin): once we figure out how to handle skip ranges across
        // epoch boundaries, cache this accordingly
        let total_stake = root_bank
            .epoch_total_stake(epoch)
            .expect("Current epoch must exist in root bank");
        // TODO(ashwin): can include cert size for debugging
        if cert_pool.skip_certified(slot, total_stake) {
            info!("{my_pubkey}: Skip Certified: Slot {}", slot,);
            return true;
        }

        if cert_tracker.write().unwrap().replay_skip_certified(slot) {
            info!("{my_pubkey}: Skip Certified (replay): Slot {}", slot,);
            return true;
        }
        false
    }

    /// Checks if we are set to produce a leader block for `slot`:
    /// - Does `my_pubkey` match the leader pubkey
    /// - Is the highest notarization/finalized slot from `cert_pool` frozen
    /// - Startup verification is complete
    /// - Bank forks does not already contain a bank for `slot`
    /// - If `wait_for_vote_to_start_leader` is set, we have landed a vote
    ///
    /// If checks pass we return true and:
    /// - Reset poh to the `parent_slot` (highest notarized/finalized slot)
    /// - Create a new bank for `slot` with parent `parent_slot`
    /// - Insert into bank_forks and poh recorder
    /// - Add the transactions for the notarization certificate and skip certificate
    fn maybe_start_leader(
        leader: &Option<Pubkey>,
        slot: Slot,
        cert_pool: &mut CertificatePool,
        slot_status_notifier: &Option<SlotStatusNotifier>,
        track_transaction_indexes: bool,
        ctx: &SharedContext,
    ) -> bool {
        let Some(leader) = leader else {
            warn!(
                "{}: No leader information available for slot {slot},
                skipping leader pipeline",
                ctx.my_pubkey
            );
            return false;
        };

        if *leader != ctx.my_pubkey {
            // Not our leader slot
            return false;
        }

        // TODO(ashwin): We max with root here, as the snapshot slot might not have a certificate.
        // Think about this more and fix if necessary.
        let parent_slot = cert_pool
            .highest_not_skip_certificate_slot()
            .max(ctx.bank_forks.read().unwrap().root());
        if ctx
            .cert_tracker
            .read()
            .unwrap()
            .highest_replay_notarized()
            .unwrap_or(0)
            > parent_slot
        {
            // A block has been frozen that has a notarization certificate for a slot
            // higher than `parent_slot`. Certificate pool ingestion is lagging behind,
            // do not attempt to build on `parent_slot` as we want to build off of the
            // highest notarized slot.
            // TODO(ashwin): We can remove this once we solve the catchup problem w/o cert tracker
            warn!(
                "Vote states in replay indicate that a slot higher than {parent_slot} has been
                notarized but we have not yet observed it in our certificate pool due to slow vote
                ingestion. Holding off on block production for now"
            );
            return false;
        }

        if parent_slot >= slot {
            // We have missed our leader slot
            warn!(
                "Slot {parent_slot} has already been notarized / finalized,
                skipping production of {slot}"
            );
            return false;
        }

        let Some(parent_bank) = ctx.bank_forks.read().unwrap().get(parent_slot) else {
            info!(
                "{}: We want to produce {slot} off of notarized slot {parent_slot},
                however {parent_slot} is not yet in bank forks,
                we are running behind so delaying our leader block",
                ctx.my_pubkey
            );
            return false;
        };

        if !parent_bank.is_frozen() {
            info!(
                "{}: We want to produce {slot} off of notarized slot {parent_slot},
                however {parent_slot} has not finished replay,
                we are running behind so delaying our leader block",
                ctx.my_pubkey
            );
            return false;
        }

        if !ReplayStage::common_maybe_start_leader_checks(
            &ctx.my_pubkey,
            ctx.leader_schedule_cache.as_ref(),
            parent_bank.as_ref(),
            ctx.bank_forks.as_ref(),
            slot,
            // TODO(ashwin): plug this in from replay
            /*has_new_vote_been_rooted*/
            true,
        ) {
            return false;
        }

        let root_slot = ctx.bank_forks.read().unwrap().root();
        let total_stake = parent_bank
            .epoch_total_stake(parent_bank.epoch_schedule().get_epoch(slot))
            .expect("my_leader_slot - 1 got a certificate, so we must be in a known epoch range");
        info!(
            "{}: Checking leader decision slot {slot} parent {parent_slot}",
            ctx.my_pubkey
        );
        let Some(StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        }) = cert_pool.make_start_leader_decision(
            slot,
            parent_slot,
            // We only need skip certificates for slots > the slot at which alpenglow is enabled
            // TODO(ashwin): plumb in to support migration
            /*first_alpenglow_slot*/
            0,
            total_stake,
        )
        else {
            if slot > parent_slot + 1 {
                let r_cert_tracker = ctx.cert_tracker.read().unwrap();
                if ((parent_slot + 1)..slot).all(|s| {
                    r_cert_tracker.replay_skip_certified(s)
                        || cert_pool.skip_certified(s, total_stake)
                }) {
                    // A block has been frozen that has the skip certificate for the required range
                    // Certificate pool ingestion is lagging behind, we cannot build our block now
                    // but reevaluate later.
                    // TODO(ashwin): We can remove this once we solve the catchup problem w/o cert tracker
                    warn!(
                        "Vote states in replay indicate that the range {} to {}
                        is skip certified but we have not yet observed it in our certificate pool due
                        to slow vote ingestion. Holding off on block production for now",
                        parent_slot + 1,
                        slot -1
                    );
                    return false;
                }
            }
            panic!(
                "Unable to get notarization or skip certificate to build a leader block
                for slot {slot} descending from parent slot {parent_slot}. Something has gone wrong with the timer loop"
            );
        };

        if ctx.poh_recorder.read().unwrap().start_slot() != parent_slot {
            // Important to keep Poh somewhat accurate for
            // parts of the system relying on PohRecorder::would_be_leader()
            //
            // TODO: On migration need to keep the ticks around for parent slots in previous epoch
            // because reset below will delete those ticks
            ReplayStage::reset_poh_recorder(
                &ctx.my_pubkey,
                ctx.blockstore.as_ref(),
                parent_bank.clone(),
                ctx.poh_recorder.as_ref(),
                ctx.leader_schedule_cache.as_ref(),
            );
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
            parent_bank.slot(),
            &parent_bank.last_blockhash(),
            &parent_bank.hash(),
        );
        if !alpenglow_update_bank_forks_and_poh_recorder_for_new_tpu_bank(
            ctx.bank_forks.as_ref(),
            ctx.poh_recorder.as_ref(),
            &ctx.transaction_recorder,
            tpu_bank,
            track_transaction_indexes,
            notarization_certificate,
            skip_certificate,
        ) {
            error!(
                "{}: Unable to create bank and commit certificates for slot:{slot} parent:{parent_slot}.
                This is likely because your block has already been skipped. Abandoning this block",
                ctx.my_pubkey
            );
            // We return true here to indicate that we should not attempt to rebuild this block.
            return true;
        }

        info!(
            "{}: new fork:{} parent:{} (leader) root:{}",
            ctx.my_pubkey, slot, parent_slot, root_slot
        );
        true
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
            cert_pool.is_finalized_slot(*slot) && ctx.bank_forks.read().unwrap().is_frozen(*slot)
        })?;
        info!("{}: Attempting to set new root {new_root}", ctx.my_pubkey);
        vctx.vote_history.set_root(new_root);
        cert_pool.purge(new_root);
        ctx.cert_tracker.write().unwrap().set_root(new_root);
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
    /// Attempts to extend the range of the previous skip vote if possible
    fn vote_skip(
        my_pubkey: &Pubkey,
        start: Slot,
        end: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) {
        // Extend the previous range if possible
        let start = voting_context
            .vote_history
            .prev_skip_start(start - 1)
            .unwrap_or(start);

        let vote = Vote::new_skip_vote(start, end);
        let bank = root_bank_cache.root_bank();
        info!(
            "{my_pubkey}: Voting skip for slot range [{start},{end}] with blockhash from {}",
            bank.slot()
        );
        Self::send_vote(vote, false, bank.as_ref(), cert_pool, voting_context);
    }

    /// Refresh the last skip vote
    fn refresh_skip(
        my_pubkey: &Pubkey,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) {
        let Some(skip_vote) = voting_context.vote_history.skip_votes.last().copied() else {
            return;
        };
        let (start, end) = skip_vote
            .skip_range()
            .expect("Must be a skip vote")
            .into_inner();
        let bank = root_bank_cache.root_bank();
        info!(
            "{my_pubkey}: Refreshing skip for slot range [{start},{end}] with blockhash from {}",
            bank.slot()
        );
        Self::send_vote(skip_vote, true, bank.as_ref(), cert_pool, voting_context);
    }

    /// Create and send a Notarization or Finalization vote
    /// Attempts to retrieve the block id from blockstore.
    ///
    /// Returns false if we should attempt to retry this vote later
    /// because the block_id/bank hash is not yet populated.
    fn vote(
        my_pubkey: &Pubkey,
        vote_type: BlockIdVote,
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
                for non leader slot {slot} {hash}, not voting {vote_type}"
            );
            return true;
        };

        let vote = match vote_type {
            BlockIdVote::Notarize => {
                Vote::new_notarization_vote(
                    slot, block_id, hash, // TODO: timestamp from vote history
                    None,
                )
            }
            BlockIdVote::Finalize => Vote::new_finalization_vote(slot, block_id, hash),
        };
        info!("{my_pubkey}: Voting {vote_type} for slot {slot} hash {hash} block_id {block_id}");
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
        if let Err(e) = cert_pool.add_vote(
            &vote,
            vote_tx.clone().into(),
            &context.vote_account_pubkey,
            bank.epoch_vote_account_stake(&context.vote_account_pubkey),
            bank.total_epoch_stake(),
        ) {
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
        my_pubkey: &Pubkey,
        vote_receiver: &VoteReceiver,
        cert_pool: &mut CertificatePool,
        bank_forks: &RwLock<BankForks>,
    ) -> Option<Slot> {
        let mut cached_root_bank = None;

        vote_receiver
            .try_iter()
            .filter_map(|(vote, vote_account_pubkey, tx)| {
                let root_bank =
                    cached_root_bank.get_or_insert_with(|| bank_forks.read().unwrap().root_bank());
                let epoch = root_bank.epoch_schedule().get_epoch(vote.slot());
                let epoch_stakes = root_bank.epoch_stakes(epoch)?;
                let validator_stake = epoch_stakes.vote_account_stake(&vote_account_pubkey);
                if validator_stake == 0 {
                    return None;
                }

                let total_stake = root_bank.epoch_total_stake(epoch)?;
                match cert_pool.add_vote(
                    &vote,
                    tx,
                    &vote_account_pubkey,
                    validator_stake,
                    total_stake,
                ) {
                    Ok(Some(cert)) if cert.is_notarization_or_skip() => {
                        info!("{my_pubkey}: Ingested new certificate {cert:?}");
                        Some(cert.slot())
                    }
                    Ok(_) => None,
                    Err(_) => {
                        // TODO(ashwin): increment metrics on non duplicate failures
                        None
                    }
                }
            })
            .max()
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
