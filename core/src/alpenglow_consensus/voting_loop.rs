//! The Alpenglow voting loop, handles all three types of votes as well as
//! rooting, leader logic, and dumping and repairing the notarized versions.
use {
    super::{
        block_creation_loop::{LeaderWindowInfo, LeaderWindowNotifier},
        certificate_pool::{self, AddVoteError},
        parent_ready_tracker::BlockProductionParent,
        vote_history_storage::VoteHistoryStorage,
        Block, CertificateId,
    },
    crate::{
        alpenglow_consensus::{
            certificate_pool::CertificatePool,
            vote_certificate::{LegacyVoteCertificate, VoteCertificate},
            vote_history::VoteHistory,
            vote_history_storage::{SavedVoteHistory, SavedVoteHistoryVersions},
        },
        commitment_service::{
            AlpenglowCommitmentAggregationData, AlpenglowCommitmentType, CommitmentAggregationData,
        },
        replay_stage::{
            CompletedBlock, CompletedBlockReceiver, Finalizer, ReplayStage, MAX_VOTE_SIGNATURES,
        },
        voting_service::VoteOp,
    },
    alpenglow_vote::vote::Vote,
    crossbeam_channel::{RecvTimeoutError, Sender},
    solana_feature_set::FeatureSet,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::{
            first_of_consecutive_leader_slots, last_of_consecutive_leader_slots, leader_slot_index,
        },
    },
    solana_measure::measure::Measure,
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender, bank::Bank, bank_forks::BankForks,
        installed_scheduler_pool::BankWithScheduler, root_bank_cache::RootBankCache,
        vote_sender_types::AlpenglowVoteReceiver as VoteReceiver,
    },
    solana_sdk::{
        clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        timing::timestamp,
        transaction::{Transaction, VersionedTransaction},
    },
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

/// Banks that have completed replay, but are yet to be voted on
type PendingBlocks = BTreeMap<Slot, Arc<Bank>>;

/// Inputs to the voting loop
pub struct VotingLoopConfig {
    pub exit: Arc<AtomicBool>,
    // Validator config
    pub vote_account: Pubkey,
    pub wait_to_vote_slot: Option<Slot>,
    pub wait_for_vote_to_start_leader: bool,
    pub vote_history: VoteHistory,
    pub vote_history_storage: Arc<dyn VoteHistoryStorage>,

    // Shared state
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub blockstore: Arc<Blockstore>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub cluster_info: Arc<ClusterInfo>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub rpc_subscriptions: Arc<RpcSubscriptions>,

    // Senders / Notifiers
    pub accounts_background_request_sender: AbsRequestSender,
    pub voting_sender: Sender<VoteOp>,
    pub commitment_sender: Sender<CommitmentAggregationData>,
    pub drop_bank_sender: Sender<Vec<BankWithScheduler>>,
    pub bank_notification_sender: Option<BankNotificationSenderConfig>,
    pub leader_window_notifier: Arc<LeaderWindowNotifier>,
    pub certificate_sender: Sender<(CertificateId, LegacyVoteCertificate)>,

    // Receivers
    pub(crate) completed_block_receiver: CompletedBlockReceiver,
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
    commitment_sender: Sender<CommitmentAggregationData>,
    wait_to_vote_slot: Option<Slot>,
    voted_signatures: Vec<Signature>,
}

/// Context shared with replay, gossip, banking stage etc
struct SharedContext {
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    bank_forks: Arc<RwLock<BankForks>>,
    rpc_subscriptions: Arc<RpcSubscriptions>,
    vote_receiver: VoteReceiver,
    // TODO(ashwin): integrate with gossip set-identity
    my_pubkey: Pubkey,
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
            wait_to_vote_slot,
            wait_for_vote_to_start_leader,
            vote_history,
            vote_history_storage: _, // TODO: set-identity
            authorized_voter_keypairs,
            blockstore,
            bank_forks,
            cluster_info,
            leader_schedule_cache,
            rpc_subscriptions,
            accounts_background_request_sender,
            voting_sender,
            commitment_sender,
            drop_bank_sender,
            bank_notification_sender,
            leader_window_notifier,
            certificate_sender,
            completed_block_receiver,
            vote_receiver,
        } = config;

        let _exit = Finalizer::new(exit.clone());
        let mut root_bank_cache = RootBankCache::new(bank_forks.clone());

        let identity_keypair = cluster_info.keypair().clone();
        let my_pubkey = identity_keypair.pubkey();
        let has_new_vote_been_rooted = !wait_for_vote_to_start_leader;
        // TODO(ashwin): handle set identity here and in loop
        // reminder prev 3 need to be mutable
        if my_pubkey != vote_history.node_pubkey {
            todo!();
        }

        let mut current_leader = None;
        let mut current_slot = {
            let bank_forks_slot = root_bank_cache.root_bank().slot();
            let vote_history_slot = vote_history.root();
            if bank_forks_slot != vote_history_slot {
                panic!(
                    "{my_pubkey}: Mismatch bank forks root {bank_forks_slot} vs vote history root {vote_history_slot}"
                );
            }
            let slot = bank_forks_slot + 1;
            info!(
                "{my_pubkey}: Starting voting loop from {slot}: root {}",
                root_bank_cache.root_bank().slot()
            );
            slot
        };

        let mut cert_pool = certificate_pool::load_from_blockstore(
            &my_pubkey,
            &root_bank_cache.root_bank(),
            blockstore.as_ref(),
            Some(certificate_sender),
        );
        let mut pending_blocks = PendingBlocks::default();

        let mut voting_context = VotingContext {
            vote_history,
            vote_account_pubkey: vote_account,
            identity_keypair,
            authorized_voter_keypairs,
            has_new_vote_been_rooted,
            voting_sender,
            commitment_sender,
            wait_to_vote_slot,
            voted_signatures: vec![],
        };
        let mut shared_context = SharedContext {
            blockstore: blockstore.clone(),
            leader_schedule_cache: leader_schedule_cache.clone(),
            bank_forks: bank_forks.clone(),
            rpc_subscriptions: rpc_subscriptions.clone(),
            vote_receiver,
            my_pubkey,
        };

        // TODO(ashwin): Start loop once migration is complete current_slot from vote history
        loop {
            let leader_end_slot = last_of_consecutive_leader_slots(current_slot);

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
                .map(crate::alpenglow_consensus::skip_timeout)
                .collect();

            if is_leader {
                let start_slot = first_of_consecutive_leader_slots(current_slot).max(1);
                // Let the block creation loop know it is time for it to produce the window
                match cert_pool
                    .parent_ready_tracker
                    .block_production_parent(current_slot)
                {
                    BlockProductionParent::MissedWindow => {
                        warn!(
                            "{my_pubkey}: Leader slot {start_slot} has already been certified, \
                            skipping production of {start_slot}-{leader_end_slot}"
                        );
                    }
                    BlockProductionParent::ParentNotReady => {
                        // This can't happen before we start optimistically producing blocks
                        // When optimistically producing blocks, we can check for the parent in the block creation loop
                        panic!(
                            "Must have a block production parent in sequential voting loop: {:#?}",
                            cert_pool.parent_ready_tracker
                        );
                    }
                    BlockProductionParent::Parent(parent_block) => {
                        Self::notify_block_creation_loop_of_leader_window(
                            &my_pubkey,
                            &leader_window_notifier,
                            start_slot,
                            leader_end_slot,
                            parent_block,
                            skip_timer,
                        );
                    }
                };
            }

            ReplayStage::log_leader_change(
                &my_pubkey,
                current_slot,
                &mut current_leader,
                &leader_pubkey,
            );

            while current_slot <= leader_end_slot {
                let leader_slot_index = leader_slot_index(current_slot);
                let timeout = timeouts[leader_slot_index];
                let cert_log_timer = Instant::now();

                info!(
                    "{my_pubkey}: Entering voting loop for slot: {current_slot}, is_leader: {is_leader}. Skip timer {} vs timeout {}",
                    skip_timer.elapsed().as_millis(), timeout.as_millis()
                );

                // Wait until we either vote notarize or skip
                while !voting_context.vote_history.voted(current_slot) {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }

                    // If timer is reached, vote skip
                    if skip_timer.elapsed().as_millis() > timeout.as_millis() {
                        Self::try_skip_window(
                            &my_pubkey,
                            current_slot,
                            leader_end_slot,
                            &mut root_bank_cache,
                            &mut cert_pool,
                            &mut voting_context,
                        );
                        debug_assert!(voting_context.vote_history.voted(current_slot));
                        break;
                    }

                    // Check if replay has successfully completed
                    if let Some(bank) = pending_blocks.get(&current_slot) {
                        debug_assert!(bank.is_frozen());
                        // Vote notarize
                        if Self::try_notar(
                            &my_pubkey,
                            bank.as_ref(),
                            &blockstore,
                            &mut cert_pool,
                            &mut voting_context,
                        ) {
                            debug_assert!(voting_context.vote_history.voted(current_slot));
                            pending_blocks.remove(&current_slot);
                            break;
                        }
                    }

                    // Ingest replayed blocks
                    match completed_block_receiver
                        .recv_timeout(timeout.saturating_sub(skip_timer.elapsed()))
                    {
                        Ok(CompletedBlock { slot, bank }) => {
                            pending_blocks.insert(slot, bank);
                        }
                        Err(RecvTimeoutError::Timeout) => (),
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }

                // Wait for certificates to indicate we can move to the next slot
                let mut refresh_timer = Instant::now();
                while cert_pool.parent_ready_tracker.highest_parent_ready() <= current_slot {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }

                    if let Err(e) = Self::ingest_votes_into_certificate_pool(
                        &my_pubkey,
                        &shared_context.vote_receiver,
                        &mut cert_pool,
                        &voting_context.commitment_sender,
                    ) {
                        error!("{my_pubkey}: error ingesting votes into certificate pool, exiting: {e:?}");
                        // Finalizer will set exit flag
                        return;
                    }

                    // Send fallback votes
                    Self::try_notar_fallback(
                        &my_pubkey,
                        current_slot,
                        leader_end_slot,
                        &mut root_bank_cache,
                        &mut cert_pool,
                        &mut voting_context,
                    );
                    Self::try_skip_fallback(
                        &my_pubkey,
                        current_slot,
                        leader_end_slot,
                        &mut root_bank_cache,
                        &mut cert_pool,
                        &mut voting_context,
                    );

                    // Refresh votes and latest finalization certificate if no progress is made
                    if refresh_timer.elapsed() > Duration::from_secs(1) {
                        Self::refresh_votes_and_cert(
                            &my_pubkey,
                            current_slot,
                            &mut root_bank_cache,
                            &mut cert_pool,
                            &mut voting_context,
                        );
                        refresh_timer = Instant::now();
                    }
                }

                info!(
                    "{my_pubkey}: Slot {current_slot} certificate observed in {} ms. Skip timer {} vs timeout {}",
                    cert_log_timer.elapsed().as_millis(),
                    skip_timer.elapsed().as_millis(),
                    timeout.as_millis(),
                );

                Self::try_final(
                    &my_pubkey,
                    current_slot,
                    root_bank_cache.root_bank().as_ref(),
                    &mut cert_pool,
                    &mut voting_context,
                );
                current_slot += 1;
            }

            // Set new root
            Self::maybe_set_root(
                leader_end_slot,
                &mut cert_pool,
                &mut pending_blocks,
                &accounts_background_request_sender,
                &bank_notification_sender,
                &drop_bank_sender,
                &mut shared_context,
                &mut voting_context,
            );

            // TODO(ashwin): If we were the leader for `current_slot` and the bank has not completed,
            // we can abandon the bank now
        }
    }

    /// Checks if any slots between `vote_history`'s current root
    /// and `slot` have received a finalization certificate and are frozen
    ///
    /// If so, set the root as the highest slot that fits these conditions
    /// and return the root
    fn maybe_set_root(
        slot: Slot,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        pending_blocks: &mut PendingBlocks,
        accounts_background_request_sender: &AbsRequestSender,
        bank_notification_sender: &Option<BankNotificationSenderConfig>,
        drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
        ctx: &mut SharedContext,
        vctx: &mut VotingContext,
    ) -> Option<Slot> {
        let old_root = vctx.vote_history.root();
        info!(
            "{}: Checking for finalization certificates between {old_root} and {slot}",
            ctx.my_pubkey
        );
        let new_root = (old_root + 1..=slot).rev().find(|slot| {
            cert_pool.is_finalized(*slot) && ctx.bank_forks.read().unwrap().is_frozen(*slot)
        })?;
        trace!("{}: Attempting to set new root {new_root}", ctx.my_pubkey);
        vctx.vote_history.set_root(new_root);
        cert_pool.handle_new_root(ctx.bank_forks.read().unwrap().get(new_root).unwrap());
        *pending_blocks = pending_blocks.split_off(&new_root);
        if let Err(e) = ReplayStage::check_and_handle_new_root(
            &ctx.my_pubkey,
            slot,
            new_root,
            ctx.bank_forks.as_ref(),
            None,
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

    /// Gets the block id of a bank. If this is a leader bank,
    /// shredding might not be complete when initially set, so update
    /// the bank for children checks later
    fn get_set_block_id(my_pubkey: &Pubkey, bank: &Bank, blockstore: &Blockstore) -> Option<Hash> {
        let is_leader = bank.collector_id() == my_pubkey;

        if bank.slot() == 0 {
            // Genesis does not have a block id
            return Some(Hash::default());
        }
        if bank.block_id().is_some() {
            return bank.block_id();
        }

        if !is_leader {
            warn!(
                "{my_pubkey}: Unable to retrieve block id or duplicate block checks have failed
                for non leader slot {} {}",
                bank.slot(),
                bank.hash()
            );
            return None;
        }

        // We are leader attempt to retrieve from blockstore
        // TODO:(ashwin) We are leader ignore duplicate block checks and just get from last shred?
        let block_id = blockstore
            .check_last_fec_set_and_get_block_id(
                bank.slot(),
                bank.hash(),
                is_leader,
                &FeatureSet::all_enabled(),
            )
            .unwrap_or(None);

        if block_id.is_some() {
            // Prior to asynchronous execution, the leader's blocks could be frozen before shredded
            // As such we choose to set the block id here, so that future parent block id lookups
            // succeed. Since the scope of parent block id lookups is the voting loop, doing it here
            // suffices, but if the scope expands we could consider moving this to replay.
            bank.set_block_id(block_id);
        }
        block_id
    }

    /// Attempts to create and send a skip vote for all unvoted slots in `[start, end]`
    fn try_skip_window(
        my_pubkey: &Pubkey,
        start: Slot,
        end: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        voting_context: &mut VotingContext,
    ) {
        let bank = root_bank_cache.root_bank();

        for slot in start..=end {
            if !voting_context.vote_history.voted(slot) {
                info!(
                    "{my_pubkey}: Voting skip for slot {slot} with blockhash from {}",
                    bank.slot()
                );
                let vote = Vote::new_skip_vote(slot);
                Self::send_vote(vote, false, bank.as_ref(), cert_pool, voting_context);
            }
        }
    }

    /// Check if we can vote finalize for `slot` at this time.
    /// If so send the vote, update vote history and return `true`.
    /// The conditions are:
    /// - We have not voted `Skip`, `SkipFallback` or `NotarizeFallback` in `slot`
    /// - We voted notarize for some block `b` in `slot`
    /// - Block `b` has a notarization certificate
    fn try_final(
        my_pubkey: &Pubkey,
        slot: Slot,
        bank: &Bank,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        voting_context: &mut VotingContext,
    ) -> bool {
        if voting_context.vote_history.its_over(slot) {
            // Already voted finalize
            return false;
        }
        if voting_context.vote_history.skipped(slot) {
            // Cannot have voted a skip variant
            return false;
        }
        let Some((block_id, bank_hash)) = voting_context.vote_history.voted_notar(slot) else {
            // Must have voted notarize in order to vote finalize
            return false;
        };
        if !cert_pool.is_notarized(slot, block_id, bank_hash) {
            // Must have a notarization certificate
            return false;
        }
        info!(
            "{my_pubkey}: Voting finalize for slot {slot} with blockhash from {}",
            bank.slot()
        );
        let vote = Vote::new_finalization_vote(slot);
        Self::send_vote(vote, false, bank, cert_pool, voting_context);
        true
    }

    /// Check if the stored certificates and block id allow us to vote notarize for `bank` at this time.
    /// If so update vote history and attempt to vote finalize.
    /// The conditions are:
    /// - If this is the first leader block of this window, check for notarization and skip certificates
    /// - Else ensure that this is a consecutive slot and that we have voted notarize on the parent
    /// - Finally check if the block id for `bank` is present and passes the duplicate checks
    fn try_notar(
        my_pubkey: &Pubkey,
        bank: &Bank,
        blockstore: &Blockstore,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        voting_context: &mut VotingContext,
    ) -> bool {
        debug_assert!(bank.is_frozen());
        let slot = bank.slot();
        let leader_slot_index = leader_slot_index(slot);

        let parent_slot = bank.parent_slot();
        let parent_bank_hash = bank.parent_hash();
        let parent_block_id = Self::get_set_block_id(
            my_pubkey,
            bank.parent().expect("Cannot vote on genesis").as_ref(),
            blockstore,
        )
        // To account for child of genesis and snapshots we allow default block id
        .unwrap_or_default();

        // Check if the certificates are valid for us to vote notarize.
        // - If this is the first leader slot (leader index = 0 or slot = 1) check
        //   that we are parent ready:
        //      - The parent is notarized fallback
        //      - OR the parent is genesis / slot 1 (WFSM hack)
        //      - All slots between the parent and this slot are skip certified
        // - If this is not the first leader slot check
        //      - The slot is consecutive to the parent slot
        //      - We voted notarize on the parent block
        if leader_slot_index == 0 || slot == 1 {
            if !cert_pool
                .parent_ready_tracker
                .parent_ready(slot, (parent_slot, parent_block_id, parent_bank_hash))
            {
                // Need to ingest more votes
                return false;
            }
        } else {
            if parent_slot + 1 != slot {
                // Non consecutive
                return false;
            }
            if voting_context.vote_history.voted_notar(parent_slot)
                != Some((parent_block_id, parent_bank_hash))
            {
                // Voted skip or notarize on a different version of the parent
                return false;
            }
        }

        // Broadcast notarize vote
        let voted = Self::vote_notarize(my_pubkey, bank, blockstore, cert_pool, voting_context);

        // Try to finalize
        Self::try_final(my_pubkey, slot, bank, cert_pool, voting_context);
        voted
    }

    /// Create and send a Notarization or Finalization vote
    /// Attempts to retrieve the block id from blockstore.
    ///
    /// Returns false if we should attempt to retry this vote later
    /// because the block_id/bank hash is not yet populated.
    fn vote_notarize(
        my_pubkey: &Pubkey,
        bank: &Bank,
        blockstore: &Blockstore,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        voting_context: &mut VotingContext,
    ) -> bool {
        debug_assert!(bank.is_frozen());
        let slot = bank.slot();
        let hash = bank.hash();
        let Some(block_id) = Self::get_set_block_id(my_pubkey, bank, blockstore) else {
            return false;
        };

        info!("{my_pubkey}: Voting notarize for slot {slot} hash {hash} block_id {block_id}");
        let vote = Vote::new_notarization_vote(slot, block_id, hash);
        Self::send_vote(vote, false, bank, cert_pool, voting_context);

        Self::alpenglow_update_commitment_cache(
            AlpenglowCommitmentType::Notarize,
            slot,
            &voting_context.commitment_sender,
        );
        true
    }

    /// Consider voting notarize fallback for this slot
    /// for each b' = safeToNotar(slot)
    /// try to skip the window, and vote notarize fallback b'
    fn try_notar_fallback(
        my_pubkey: &Pubkey,
        slot: Slot,
        leader_end_slot: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        voting_context: &mut VotingContext,
    ) -> bool {
        let blocks = cert_pool.safe_to_notar(slot, &voting_context.vote_history);
        if blocks.is_empty() {
            return false;
        }
        Self::try_skip_window(
            my_pubkey,
            slot,
            leader_end_slot,
            root_bank_cache,
            cert_pool,
            voting_context,
        );
        for (block_id, bank_hash) in blocks.into_iter() {
            info!("{my_pubkey}: Voting notarize fallback for slot {slot} hash {bank_hash} block_id {block_id}");
            let vote = Vote::new_notarization_fallback_vote(slot, block_id, bank_hash);
            Self::send_vote(
                vote,
                false,
                root_bank_cache.root_bank().as_ref(),
                cert_pool,
                voting_context,
            );
        }
        true
    }

    /// Consider voting skip fallback for this slot
    /// if safeToSkip(slot)
    /// then try to skip the window, and vote skip fallback
    fn try_skip_fallback(
        my_pubkey: &Pubkey,
        slot: Slot,
        leader_end_slot: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        voting_context: &mut VotingContext,
    ) -> bool {
        if !cert_pool.safe_to_skip(slot, &voting_context.vote_history) {
            return false;
        }
        Self::try_skip_window(
            my_pubkey,
            slot,
            leader_end_slot,
            root_bank_cache,
            cert_pool,
            voting_context,
        );
        info!("{my_pubkey}: Voting skip fallback for slot {slot}");
        let vote = Vote::new_skip_fallback_vote(slot);
        Self::send_vote(
            vote,
            false,
            root_bank_cache.root_bank().as_ref(),
            cert_pool,
            voting_context,
        );
        true
    }

    /// Refresh the highest recent finalization certificate
    /// For each slot past this up to our current slot `slot`, refresh our votes
    fn refresh_votes_and_cert(
        my_pubkey: &Pubkey,
        slot: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        voting_context: &mut VotingContext,
    ) {
        let highest_finalization_slot = cert_pool
            .highest_finalized_slot()
            .max(root_bank_cache.root_bank().slot());
        // TODO: rebroadcast finalization cert for block once we have BLS
        // This includes the notarized fallback cert if it was a slow finalization

        // Refresh votes for all slots up to our current slot
        for s in highest_finalization_slot..=slot {
            for vote in voting_context.vote_history.votes_cast(s) {
                info!("{my_pubkey}: Refreshing vote {vote:?}");
                Self::send_vote(
                    vote,
                    true,
                    root_bank_cache.root_bank().as_ref(),
                    cert_pool,
                    voting_context,
                );
            }
        }
    }

    /// Send an alpenglow vote
    /// `bank` will be used for:
    /// - startup verification
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
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
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

        if let Err(e) = Self::add_vote_and_maybe_update_commitment(
            &context.identity_keypair.pubkey(),
            &vote,
            &context.vote_account_pubkey,
            vote_tx.clone().into(),
            cert_pool,
            &context.commitment_sender,
        ) {
            if !is_refresh {
                warn!("Unable to push our own vote into the pool {}", e);
                return;
            }
        };

        // Update and save the vote history
        if !is_refresh {
            context.vote_history.add_vote(vote);
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
        cert_pool: &mut CertificatePool<LegacyVoteCertificate>,
        commitment_sender: &Sender<CommitmentAggregationData>,
    ) -> Result<(), AddVoteError> {
        let add_to_cert_pool =
            |(vote, vote_account_pubkey, tx): (Vote, Pubkey, VersionedTransaction)| {
                match Self::add_vote_and_maybe_update_commitment(
                    my_pubkey,
                    &vote,
                    &vote_account_pubkey,
                    tx,
                    cert_pool,
                    commitment_sender,
                ) {
                    err @ Err(AddVoteError::CertificateSenderError) => err,
                    Err(e) => {
                        // TODO(ashwin): increment metrics on non duplicate failures
                        trace!("{my_pubkey}: unable to push vote into the pool {}", e);
                        Ok(())
                    }
                    Ok(()) => Ok(()),
                }
            };

        let Ok(first) = vote_receiver.recv_timeout(Duration::from_secs(1)) else {
            // Either timeout or sender disconnected, return so we can check exit
            return Ok(());
        };
        std::iter::once(first)
            .chain(vote_receiver.try_iter())
            .try_for_each(add_to_cert_pool)
    }

    /// Notifies the block creation loop of a new leader window to produce.
    /// Caller should use the highest certified slot (not skip) as the `parent_slot`
    ///
    /// Fails to notify and returns false if the leader window has already
    /// been skipped, or if the parent is greater than or equal to the first leader slot
    fn notify_block_creation_loop_of_leader_window(
        my_pubkey: &Pubkey,
        leader_window_notifier: &LeaderWindowNotifier,
        start_slot: Slot,
        end_slot: Slot,
        parent_block @ (parent_slot, _, _): Block,
        skip_timer: Instant,
    ) -> bool {
        debug_assert!(parent_slot < start_slot);

        // Notify the block creation loop.
        let mut l_window_info = leader_window_notifier.window_info.lock().unwrap();
        if let Some(window_info) = l_window_info.as_ref() {
            error!(
                "{my_pubkey}: Attempting to start leader window for {start_slot}-{end_slot}, however there is \
                already a pending window to produce {}-{}. Something has gone wrong, Overwriting in favor \
                of the newer window",
                window_info.start_slot,
                window_info.end_slot,
            );
        }
        *l_window_info = Some(LeaderWindowInfo {
            start_slot,
            end_slot,
            parent_block,
            skip_timer,
        });
        leader_window_notifier.window_notification.notify_one();
        true
    }

    /// Adds a vote to the certificate pool and updates the commitment cache if necessary
    fn add_vote_and_maybe_update_commitment<VC: VoteCertificate>(
        my_pubkey: &Pubkey,
        vote: &Vote,
        vote_account_pubkey: &Pubkey,
        tx: VC::VoteTransaction,
        cert_pool: &mut CertificatePool<VC>,
        commitment_sender: &Sender<CommitmentAggregationData>,
    ) -> Result<(), AddVoteError> {
        let Some(new_finalized_slot) = cert_pool.add_vote(vote, tx, vote_account_pubkey)? else {
            return Ok(());
        };
        trace!("{my_pubkey}: new finalization certificate for {new_finalized_slot}");
        Self::alpenglow_update_commitment_cache(
            AlpenglowCommitmentType::Finalized,
            new_finalized_slot,
            commitment_sender,
        );
        Ok(())
    }

    fn alpenglow_update_commitment_cache(
        commitment_type: AlpenglowCommitmentType,
        slot: Slot,
        commitment_sender: &Sender<CommitmentAggregationData>,
    ) {
        if let Err(e) = commitment_sender.send(
            CommitmentAggregationData::AlpenglowCommitmentAggregationData(
                AlpenglowCommitmentAggregationData {
                    commitment_type,
                    slot,
                },
            ),
        ) {
            trace!("commitment_sender failed: {:?}", e);
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_voting_loop.join().map(|_| ())
    }
}
