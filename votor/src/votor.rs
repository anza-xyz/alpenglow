use {
    crate::{
        certificate_pool::{self, AddVoteError},
        commitment::AlpenglowCommitmentAggregationData,
        event::{CompletedBlockReceiver, VotorEvent},
        vote_history::{VoteHistory, VoteHistoryError},
        vote_history_storage::VoteHistoryStorage,
        voting_loop::{LeaderWindowNotifier, PendingBlocks},
        voting_utils::{self, BLSOp, VotingContext},
        Block, CertificateId, STANDSTILL_TIMEOUT,
    },
    alpenglow_vote::{bls_message::CertificateMessage, vote::Vote},
    crossbeam_channel::{bounded, select, Receiver, RecvTimeoutError, Sender},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::{
            first_of_consecutive_leader_slots, last_of_consecutive_leader_slots,
        },
    },
    solana_pubkey::Pubkey,
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender, bank_forks::BankForks,
        installed_scheduler_pool::BankWithScheduler, root_bank_cache::RootBankCache,
        vote_sender_types::BLSVerifiedMessageReceiver,
    },
    solana_sdk::{clock::Slot, signature::Keypair, signer::Signer},
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

/// Inputs to Votor
pub struct VotorConfig {
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
    pub bls_sender: Sender<BLSOp>,
    pub commitment_sender: Sender<AlpenglowCommitmentAggregationData>,
    pub drop_bank_sender: Sender<Vec<BankWithScheduler>>,
    pub bank_notification_sender: Option<BankNotificationSenderConfig>,
    pub leader_window_notifier: Arc<LeaderWindowNotifier>,
    pub certificate_sender: Sender<(CertificateId, CertificateMessage)>,

    // Receivers
    pub completed_block_receiver: CompletedBlockReceiver,
    pub bls_receiver: BLSVerifiedMessageReceiver,
}

/// Inputs for the certificate pool thread
struct CertificatePoolContext {
    exit: Arc<AtomicBool>,
    start: Arc<(Mutex<bool>, Condvar)>,

    my_pubkey: Pubkey,
    my_vote_pubkey: Pubkey,
    blockstore: Arc<Blockstore>,
    bank_forks: Arc<RwLock<BankForks>>,

    bls_receiver: BLSVerifiedMessageReceiver,
    event_sender: Sender<VotorEvent>,
    commitment_sender: Sender<AlpenglowCommitmentAggregationData>,
    certificate_sender: Sender<(CertificateId, CertificateMessage)>,
}

/// Context shared with block creation, replay, gossip, banking stage etc
struct SharedContext {
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    bank_forks: Arc<RwLock<BankForks>>,
    cluster_info: Arc<ClusterInfo>,
    rpc_subscriptions: Arc<RpcSubscriptions>,
    leader_window_notifier: Arc<LeaderWindowNotifier>,
    vote_history_storage: Arc<dyn VoteHistoryStorage>,
}

/// Inputs for the voting loop thread
#[allow(dead_code)]
struct VotingLoopContext {
    exit: Arc<AtomicBool>,
    start: Arc<(Mutex<bool>, Condvar)>,

    // VotorEvent receivers
    completed_block_receiver: CompletedBlockReceiver,
    event_receiver: Receiver<VotorEvent>,

    // Contexts
    shared_context: SharedContext,
    voting_context: VotingContext,
}

pub struct Votor {
    // TODO: Just a placeholder for how migration could look like,
    // will fix once we finish the strategy
    #[allow(dead_code)]
    start: Arc<(Mutex<bool>, Condvar)>,

    t_voting_loop: JoinHandle<()>,
    t_cert_pool: JoinHandle<()>,
}

impl Votor {
    pub fn new(config: VotorConfig) -> Self {
        let VotorConfig {
            exit,
            vote_account,
            wait_to_vote_slot,
            wait_for_vote_to_start_leader,
            vote_history,
            vote_history_storage,
            authorized_voter_keypairs,
            blockstore,
            bank_forks,
            cluster_info,
            leader_schedule_cache,
            rpc_subscriptions,
            accounts_background_request_sender: _,
            bls_sender,
            commitment_sender,
            drop_bank_sender: _,
            bank_notification_sender: _,
            leader_window_notifier,
            certificate_sender,
            completed_block_receiver,
            bls_receiver,
        } = config;

        let start = Arc::new((Mutex::new(false), Condvar::new()));

        let identity_keypair = cluster_info.keypair().clone();
        let my_pubkey = identity_keypair.pubkey();
        let has_new_vote_been_rooted = !wait_for_vote_to_start_leader;

        // This should not backup, TODO: add metrics for length
        let (event_sender, event_receiver) = bounded(1000);

        let shared_context = SharedContext {
            blockstore: blockstore.clone(),
            leader_schedule_cache,
            bank_forks: bank_forks.clone(),
            rpc_subscriptions,
            leader_window_notifier,
            cluster_info: cluster_info.clone(),
            vote_history_storage,
        };

        let voting_context = VotingContext {
            vote_history,
            vote_account_pubkey: vote_account,
            identity_keypair: identity_keypair.clone(),
            authorized_voter_keypairs,
            derived_bls_keypairs: HashMap::new(),
            has_new_vote_been_rooted,
            bls_sender,
            commitment_sender: commitment_sender.clone(),
            wait_to_vote_slot,
            voted_signatures: vec![],
            root_bank_cache: RootBankCache::new(bank_forks.clone()),
        };

        let voting_loop_context = VotingLoopContext {
            exit: exit.clone(),
            start: start.clone(),
            completed_block_receiver,
            event_receiver,
            shared_context,
            voting_context,
        };

        let cert_pool_context = CertificatePoolContext {
            exit: exit.clone(),
            start: start.clone(),
            my_pubkey,
            my_vote_pubkey: vote_account,
            blockstore,
            bank_forks,
            bls_receiver,
            event_sender,
            commitment_sender,
            certificate_sender,
        };

        let t_voting_loop = Builder::new()
            .name("solVotingLoop".to_string())
            .spawn(move || {
                if let Err(e) = Self::voting_loop(voting_loop_context) {
                    // A sender disconnected, shutdown
                    info!(
                        "{}: Voting loop sender disconnected {e:?}. Exiting",
                        cluster_info.id()
                    );
                    exit.store(true, Ordering::Relaxed);
                }
            })
            .unwrap();

        let t_cert_pool = Builder::new()
            .name("solCertPoolIngest".to_string())
            .spawn(move || Self::certificate_pool_ingestion(cert_pool_context))
            .unwrap();

        Self {
            start,
            t_voting_loop,
            t_cert_pool,
        }
    }

    #[allow(dead_code)]
    fn start_migration(&self) {
        // TODO: evaluate once we have actual migration logic
        let (lock, cvar) = &*self.start;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_all();
    }

    fn wait_for_migration_or_exit(exit: &AtomicBool, (lock, cvar): &(Mutex<bool>, Condvar)) {
        let mut started = lock.lock().unwrap();
        while !*started {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            // Add timeout to check for exit flag
            (started, _) = cvar.wait_timeout(started, Duration::from_secs(5)).unwrap();
        }
    }

    fn handle_set_identity(
        my_pubkey: &mut Pubkey,
        ctx: &SharedContext,
        vctx: &mut VotingContext,
    ) -> Result<(), VoteHistoryError> {
        let new_identity = ctx.cluster_info.keypair();
        let new_pubkey = new_identity.pubkey();
        // This covers both:
        // - startup set-identity so that vote_history is outdated but my_pubkey == new_pubkey
        // - set-identity during normal operation, vote_history == my_pubkey != new_pubkey
        if *my_pubkey != new_pubkey || vctx.vote_history.node_pubkey != new_pubkey {
            let my_old_pubkey = vctx.vote_history.node_pubkey;
            *my_pubkey = new_pubkey;
            vctx.vote_history = VoteHistory::restore(ctx.vote_history_storage.as_ref(), my_pubkey)?;
            vctx.identity_keypair = new_identity.clone();
            warn!("set-identity: from {my_old_pubkey} to {my_pubkey}");
        }
        Ok(())
    }

    fn voting_loop(context: VotingLoopContext) -> Result<(), RecvTimeoutError> {
        let VotingLoopContext {
            exit,
            start,
            completed_block_receiver,
            event_receiver,
            shared_context: mut ctx,
            voting_context: mut vctx,
        } = context;
        let mut my_pubkey = vctx.identity_keypair.pubkey();
        let mut pending_blocks = PendingBlocks::default();

        // Wait until migration has completed
        Self::wait_for_migration_or_exit(&exit, &start);

        if exit.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Check for set identity
        if let Err(e) = Self::handle_set_identity(&mut my_pubkey, &ctx, &mut vctx) {
            error!(
                "Unable to load new vote history when attempting to change identity from {} \
                 to {} on voting loop startup, Exiting: {}",
                vctx.vote_history.node_pubkey,
                ctx.cluster_info.id(),
                e
            );
            exit.store(true, Ordering::Relaxed);
            return Ok(());
        }

        while !exit.load(Ordering::Relaxed) {
            let event = select! {
                recv(completed_block_receiver) -> msg => {
                    VotorEvent::Block(msg?)
                },
                recv(event_receiver) -> msg => {
                    msg?
                },
                default(Duration::from_secs(1))  => continue
            };

            match event {
                // Block has completed replay
                VotorEvent::Block(completed_block) => todo!(),

                // Block has received a notarization certificate
                VotorEvent::BlockNotarized(block) => {
                    vctx.vote_history.add_block_notarized(block);
                }

                // Received a parent ready notification for `slot`
                VotorEvent::ParentReady { slot, parent_block } => {
                    vctx.vote_history.add_parent_ready(slot, parent_block);
                    // TODO:
                    // check_pending_blocks();
                    // set_timeouts();
                    // notify block creation loop
                }

                // Skip timer for the slot has fired, TODO: plug this in
                VotorEvent::Timeout(slot) => {
                    if vctx.vote_history.voted(slot) {
                        continue;
                    }
                    Self::try_skip_window(&my_pubkey, slot, &mut vctx);
                }

                // We have observed the safe to notar condition, and can send a notar fallback vote
                // TODO: cert pool check for parent block id for intra window slots
                VotorEvent::SafeToNotar((slot, block_id, bank_hash)) => {
                    Self::try_skip_window(&my_pubkey, slot, &mut vctx);
                    if vctx.vote_history.its_over(slot)
                        || vctx
                            .vote_history
                            .voted_notar_fallback(slot, block_id, bank_hash)
                    {
                        continue;
                    }
                    voting_utils::send_vote_and_queue_for_cert_pool(
                        &my_pubkey,
                        Vote::new_notarization_fallback_vote(slot, block_id, bank_hash),
                        false,
                        &mut vctx,
                    );
                }

                // We have observed the safe to skip condition, and can send a skip fallback vote
                VotorEvent::SafeToSkip(slot) => {
                    Self::try_skip_window(&my_pubkey, slot, &mut vctx);
                    if vctx.vote_history.its_over(slot)
                        || vctx.vote_history.voted_skip_fallback(slot)
                    {
                        continue;
                    }
                    voting_utils::send_vote_and_queue_for_cert_pool(
                        &my_pubkey,
                        Vote::new_skip_fallback_vote(slot),
                        false,
                        &mut vctx,
                    );
                }

                // We have not observed a finalization certificate in a while, refresh our votes and certs
                VotorEvent::Standstill(highest_finalized_slot) => {
                    // TODO: once we have certificate broadcast, we should also refresh certs
                    Self::refresh_votes(&my_pubkey, highest_finalized_slot, &mut vctx);
                }

                // Operator called set identity make sure that our keypair is updated for voting
                // TODO: plug this in from cli
                VotorEvent::SetIdentity => {
                    if let Err(e) = Self::handle_set_identity(&mut my_pubkey, &ctx, &mut vctx) {
                        error!(
                            "Unable to load new vote history when attempting to change identity from {} \
                             to {} in voting loop, Exiting: {}",
                             vctx.vote_history.node_pubkey,
                             ctx.cluster_info.id(),
                             e
                        );
                        exit.store(true, Ordering::Relaxed);
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    fn try_final(
        my_pubkey: &Pubkey,
        block @ (slot, _, _): Block,
        voting_context: &mut VotingContext,
    ) {
        // if voting_context.vote_history.
    }

    fn try_skip_window(my_pubkey: &Pubkey, slot: Slot, voting_context: &mut VotingContext) {
        for s in first_of_consecutive_leader_slots(slot)..=last_of_consecutive_leader_slots(slot) {
            if voting_context.vote_history.voted(s) {
                continue;
            }
            if !voting_utils::send_vote_and_queue_for_cert_pool(
                my_pubkey,
                Vote::new_skip_vote(s),
                false,
                voting_context,
            ) {
                warn!("{my_pubkey}: send skip vote failed for {s}");
            }
        }
    }

    /// Refresh all votes cast for slots >= highest_finalized_slot
    fn refresh_votes(
        my_pubkey: &Pubkey,
        highest_finalized_slot: Slot,
        voting_context: &mut VotingContext,
    ) {
        for vote in voting_context
            .vote_history
            .votes_cast_since(highest_finalized_slot)
        {
            info!("{my_pubkey}: Refreshing vote {vote:?}");
            if !voting_utils::send_vote_and_queue_for_cert_pool(
                my_pubkey,
                vote,
                true,
                voting_context,
            ) {
                warn!("{my_pubkey}: send_vote failed for {:?}", vote);
            }
        }
    }

    fn certificate_pool_ingestion(ctx: CertificatePoolContext) {
        let root_bank = ctx.bank_forks.read().unwrap().root_bank();
        let mut events = vec![];
        let mut cert_pool = certificate_pool::load_from_blockstore(
            &ctx.my_pubkey,
            &root_bank,
            ctx.blockstore.as_ref(),
            Some(ctx.certificate_sender),
            &mut events,
        );

        // Wait until migration has completed
        Self::wait_for_migration_or_exit(&ctx.exit, &ctx.start);

        // Standstill tracking
        let mut standstill_timer = Instant::now();
        let mut highest_finalized_slot = cert_pool.highest_finalized_slot();

        // Ingest votes into certificate pool and notify voting loop of new events
        loop {
            if ctx.exit.load(Ordering::Relaxed) {
                return;
            }

            if standstill_timer.elapsed() > STANDSTILL_TIMEOUT {
                events.push(VotorEvent::Standstill(highest_finalized_slot));
                standstill_timer = Instant::now();
            }

            if events
                .drain(..)
                .try_for_each(|event| ctx.event_sender.send(event))
                .is_err()
            {
                // Shutdown
                info!(
                    "{}: Votor event receiver disconnected. Exiting.",
                    ctx.my_pubkey
                );
                ctx.exit.store(true, Ordering::Relaxed);
                return;
            }

            let bls_messages = std::iter::once(
                match ctx.bls_receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(msg) => msg,
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => {
                        // Shutdown
                        info!("{}: BLS sender disconnected. Exiting.", ctx.my_pubkey);
                        ctx.exit.store(true, Ordering::Relaxed);
                        return;
                    }
                },
            )
            .chain(ctx.bls_receiver.try_iter());

            for message in bls_messages {
                match voting_utils::add_message_and_maybe_update_commitment(
                    &ctx.my_pubkey,
                    &ctx.my_vote_pubkey,
                    &message,
                    &mut cert_pool,
                    &mut events,
                    &ctx.commitment_sender,
                ) {
                    Ok(Some(finalized_slot)) => {
                        // Reset standstill timer
                        debug_assert!(finalized_slot > highest_finalized_slot);
                        highest_finalized_slot = finalized_slot;
                        standstill_timer = Instant::now();
                    }
                    Ok(_) => (),
                    Err(AddVoteError::CertificateSenderError) => {
                        // Shutdown
                        info!(
                            "{}: Certificate receiver disconnected. Exiting.",
                            ctx.my_pubkey
                        );
                        ctx.exit.store(true, Ordering::Relaxed);
                        return;
                    }
                    Err(e) => {
                        // This is a non critical error, a duplicate vote for example
                        trace!("{}: unable to push vote into pool {}", ctx.my_pubkey, e);
                        continue;
                    }
                };
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_cert_pool.join()?;
        self.t_voting_loop.join()
    }
}
