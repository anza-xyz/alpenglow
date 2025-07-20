//! The entrypoint into votor the module responsible for voting, rooting, and notifying
//! the core to create a new block.
//!
//!                                Votor
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │                                                                            │
//!   │                                                     Push Certificate       │
//!   │        ┌───────────────────────────────────────────────────────────────────│────────┐
//!   │        │                   Parent Ready                                    │        │
//!   │        │                   Standstill                                      │        │
//!   │        │                   Finalized                                       │        │
//!   │        │                   Block Notarized                                 │        │
//!   │        │         ┌─────────Safe To Notar/Skip───┐       Push               │        │
//!   │        │         │         Produce Window       │       Vote               │        │
//!   │        │         │                              │ ┌────────────────────────│──────┐ │
//!   │        │         │                              │ │                        │ ┌────▼─▼───────┐
//!   │        │         │                              │ │                        │ │Voting Service│
//!   │        │         │                              │ │                        │ └──────────────┘
//!   │        │         │                              │ │                        │
//!   │   ┌────┼─────────┼───────────────┐              │ │                        │
//!   │   │                              │              │ │      Block             │ ┌────────────────────┐
//!   │   │   Certificate Pool Service   │              │ │  ┌─────────────────────│─┼ Replay / Broadcast │
//!   │   │                              │              │ │  │                     │ └────────────────────┘
//!   │   │ ┌──────────────────────────┐ │              │ │  │                     │
//!   │   │ │                          │ │              │ │  │                     │
//!   │   │ │     Certificate Pool     │ │              │ │  │                     │
//!   │   │ │ ┌────────────────────┐   │ │         ┌────▼─┼──▼───────┐   Start     │
//!   │   │ │ │Parent ready tracker│   │ │ Vote    │                 │ Leader window ┌──────────────────────┐
//!   │   │ │ └────────────────────┘   │ ◄─────────┼  Event Handler  ┼─────────────│─►  Block creation loop │
//!   │   │ └──────────────────────────┘ │         │                 │             │ └──────────────────────┘
//!   │   │                              │         └─▲───────────┬───┘             │
//!   │   └──────────────────────────────┘           │           │                 │
//!   │                                     Timeout  │           │                 │
//!   │                                              │           │ Set Timeouts    │
//!   │                                              │           │                 │
//!   │                          ┌───────────────────┴┐     ┌────▼───────────────┐ │
//!   │                          │                    │     │                    │ │
//!   │                          │ Skip Timer Service ┼─────┼ Skip timer Manager │ │
//!   │                          │                    │     │                    │ │
//!   │                          └────────────────────┘     └────────────────────┘ │
//!   └────────────────────────────────────────────────────────────────────────────┘
//!
use {
    crate::{
        certificate_pool_service::{CertificatePoolContext, CertificatePoolService},
        commitment::AlpenglowCommitmentAggregationData,
        event::{LeaderWindowInfo, VotorEventReceiver, VotorEventSender},
        event_handler::{EventHandler, EventHandlerContext},
        root_utils::RootContext,
        skip_timer::SkipTimerService,
        vote_history::VoteHistory,
        vote_history_storage::VoteHistoryStorage,
        voting_utils::{BLSOp, VotingContext},
        CertificateId,
    },
    alpenglow_vote::bls_message::CertificateMessage,
    crossbeam_channel::{bounded, Sender},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
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
        thread,
        time::Duration,
    },
};

/// Communication with the block creation loop to notify leader window
#[derive(Default)]
pub struct LeaderWindowNotifier {
    pub window_info: Mutex<Option<LeaderWindowInfo>>,
    pub window_notification: Condvar,
}

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
    pub event_sender: VotorEventSender,

    // Receivers
    pub event_receiver: VotorEventReceiver,
    pub bls_receiver: BLSVerifiedMessageReceiver,
}

/// Context shared with block creation, replay, gossip, banking stage etc
pub(crate) struct SharedContext {
    pub(crate) blockstore: Arc<Blockstore>,
    pub(crate) bank_forks: Arc<RwLock<BankForks>>,
    pub(crate) cluster_info: Arc<ClusterInfo>,
    pub(crate) rpc_subscriptions: Arc<RpcSubscriptions>,
    pub(crate) leader_window_notifier: Arc<LeaderWindowNotifier>,
    pub(crate) vote_history_storage: Arc<dyn VoteHistoryStorage>,
}

pub struct Votor {
    // TODO: Just a placeholder for how migration could look like,
    // will fix once we finish the strategy
    #[allow(dead_code)]
    start: Arc<(Mutex<bool>, Condvar)>,

    event_handler: EventHandler,
    certificate_pool_service: CertificatePoolService,
    skip_timer_service: SkipTimerService,
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
            accounts_background_request_sender,
            bls_sender,
            commitment_sender,
            drop_bank_sender,
            bank_notification_sender,
            leader_window_notifier,
            certificate_sender,
            event_sender,
            event_receiver,
            bls_receiver,
        } = config;

        let start = Arc::new((Mutex::new(false), Condvar::new()));

        let identity_keypair = cluster_info.keypair().clone();
        let my_pubkey = identity_keypair.pubkey();
        let has_new_vote_been_rooted = !wait_for_vote_to_start_leader;

        // These should not backup, TODO: add metrics for length
        let (skip_timeout_sender, skip_timeout_receiver) = bounded(1000);
        let (own_vote_sender, own_vote_receiver) = bounded(1000);

        let shared_context = SharedContext {
            blockstore: blockstore.clone(),
            bank_forks: bank_forks.clone(),
            cluster_info: cluster_info.clone(),
            rpc_subscriptions,
            leader_window_notifier,
            vote_history_storage,
        };

        let voting_context = VotingContext {
            vote_history,
            vote_account_pubkey: vote_account,
            identity_keypair: identity_keypair.clone(),
            authorized_voter_keypairs,
            derived_bls_keypairs: HashMap::new(),
            has_new_vote_been_rooted,
            own_vote_sender,
            bls_sender: bls_sender.clone(),
            commitment_sender: commitment_sender.clone(),
            wait_to_vote_slot,
            voted_signatures: vec![],
            root_bank_cache: RootBankCache::new(bank_forks.clone()),
        };

        let root_context = RootContext {
            leader_schedule_cache: leader_schedule_cache.clone(),
            accounts_background_request_sender,
            bank_notification_sender,
            drop_bank_sender,
        };

        let (skip_timer_service, skip_timer) =
            SkipTimerService::new(exit.clone(), 100, skip_timeout_sender);

        let event_handler_context = EventHandlerContext {
            exit: exit.clone(),
            start: start.clone(),
            event_receiver,
            skip_timeout_receiver,
            skip_timer,
            shared_context,
            voting_context,
            root_context,
        };

        let cert_pool_context = CertificatePoolContext {
            exit: exit.clone(),
            start: start.clone(),
            my_pubkey,
            my_vote_pubkey: vote_account,
            blockstore,
            root_bank_cache: RootBankCache::new(bank_forks.clone()),
            leader_schedule_cache,
            own_vote_receiver,
            bls_receiver,
            bls_sender,
            event_sender,
            commitment_sender,
            certificate_sender,
        };

        let event_handler = EventHandler::new(event_handler_context);
        let certificate_pool_service = CertificatePoolService::new(cert_pool_context);

        Self {
            start,
            event_handler,
            certificate_pool_service,
            skip_timer_service,
        }
    }

    pub fn start_migration(&self) {
        // TODO: evaluate once we have actual migration logic
        let (lock, cvar) = &*self.start;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_all();
    }

    pub(crate) fn wait_for_migration_or_exit(
        exit: &AtomicBool,
        (lock, cvar): &(Mutex<bool>, Condvar),
    ) {
        let mut started = lock.lock().unwrap();
        while !*started {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            // Add timeout to check for exit flag
            (started, _) = cvar.wait_timeout(started, Duration::from_secs(5)).unwrap();
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.certificate_pool_service.join()?;
        self.skip_timer_service.join()?;
        self.event_handler.join()
    }
}
