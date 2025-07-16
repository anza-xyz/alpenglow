use {
    crate::{
        certificate_pool::{self, AddVoteError},
        commitment::AlpenglowCommitmentAggregationData,
        event::{CompletedBlockReceiver, VotorEvent},
        vote_history::VoteHistory,
        vote_history_storage::VoteHistoryStorage,
        voting_loop::LeaderWindowNotifier,
        voting_utils::{self, BLSOp, VotingContext},
        CertificateId, STANDSTILL_TIMEOUT,
    },
    alpenglow_vote::bls_message::CertificateMessage,
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_pubkey::Pubkey,
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender, bank_forks::BankForks,
        installed_scheduler_pool::BankWithScheduler, vote_sender_types::BLSVerifiedMessageReceiver,
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

/// Inputs for the voting loop thread
#[allow(dead_code)]
struct VotingLoopContext {
    exit: Arc<AtomicBool>,
    start: Arc<(Mutex<bool>, Condvar)>,

    completed_block_receiver: CompletedBlockReceiver,
    event_receiver: Receiver<VotorEvent>,
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
            vote_history_storage: _,
            authorized_voter_keypairs,
            blockstore,
            bank_forks,
            cluster_info,
            leader_schedule_cache: _,
            rpc_subscriptions: _,
            accounts_background_request_sender: _,
            bls_sender,
            commitment_sender,
            drop_bank_sender: _,
            bank_notification_sender: _,
            leader_window_notifier: _,
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
        };

        let voting_loop_context = VotingLoopContext {
            exit: exit.clone(),
            start: start.clone(),
            completed_block_receiver,
            event_receiver,
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
            .spawn(move || Self::voting_loop(voting_loop_context))
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

    fn voting_loop(_context: VotingLoopContext) {}

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
