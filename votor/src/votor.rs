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
//!   │                          │ Timer Service      ┼─────┼ timer Manager      │ │
//!   │                          │                    │     │                    │ │
//!   │                          └────────────────────┘     └────────────────────┘ │
//!   └────────────────────────────────────────────────────────────────────────────┘
//!
use {
    crate::{
        commitment::AlpenglowCommitmentAggregationData,
        consensus_metrics::ConsensusMetrics,
        consensus_pool_service::{ConsensusPoolContext, ConsensusPoolService},
        event::{LeaderWindowInfo, VotorEventReceiver, VotorEventSender},
        event_handler::{EventHandler, EventHandlerContext},
        root_utils::RootContext,
        timer_manager::TimerManager,
        vote_history::VoteHistory,
        vote_history_storage::VoteHistoryStorage,
        voting_service::BLSOp,
        voting_utils::VotingContext,
    },
    crossbeam_channel::{Receiver, Sender},
    parking_lot::RwLock as PlRwLock,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_pubkey::Pubkey,
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        bank_forks::BankForks, installed_scheduler_pool::BankWithScheduler,
        snapshot_controller::SnapshotController,
    },
    solana_votor_messages::consensus_message::ConsensusMessage,
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
    pub highest_parent_ready: RwLock<(Slot, (Slot, Hash))>,
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
    pub rpc_subscriptions: Option<Arc<RpcSubscriptions>>,

    // Senders / Notifiers
    pub snapshot_controller: Option<Arc<SnapshotController>>,
    pub bls_sender: Sender<BLSOp>,
    pub commitment_sender: Sender<AlpenglowCommitmentAggregationData>,
    pub drop_bank_sender: Sender<Vec<BankWithScheduler>>,
    pub bank_notification_sender: Option<BankNotificationSenderConfig>,
    pub leader_window_notifier: Arc<LeaderWindowNotifier>,
    pub event_sender: VotorEventSender,
    pub own_vote_sender: Sender<ConsensusMessage>,

    // Receivers
    pub event_receiver: VotorEventReceiver,
    pub consensus_message_receiver: Receiver<ConsensusMessage>,
}

/// Context shared with block creation, replay, gossip, banking stage etc
pub(crate) struct SharedContext {
    pub(crate) blockstore: Arc<Blockstore>,
    pub(crate) bank_forks: Arc<RwLock<BankForks>>,
    pub(crate) cluster_info: Arc<ClusterInfo>,
    pub(crate) rpc_subscriptions: Option<Arc<RpcSubscriptions>>,
    pub(crate) leader_window_notifier: Arc<LeaderWindowNotifier>,
    pub(crate) vote_history_storage: Arc<dyn VoteHistoryStorage>,
}

pub struct Votor {
    // TODO: Just a placeholder for how migration could look like,
    // will fix once we finish the strategy
    #[allow(dead_code)]
    start: Arc<(Mutex<bool>, Condvar)>,

    event_handler: EventHandler,
    consensus_pool_service: ConsensusPoolService,
    timer_manager: Arc<PlRwLock<TimerManager>>,
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
            snapshot_controller,
            bls_sender,
            commitment_sender,
            drop_bank_sender,
            bank_notification_sender,
            leader_window_notifier,
            event_sender,
            event_receiver,
            own_vote_sender,
            consensus_message_receiver: bls_receiver,
        } = config;

        let start = Arc::new((Mutex::new(false), Condvar::new()));

        let identity_keypair = cluster_info.keypair().clone();
        let has_new_vote_been_rooted = !wait_for_vote_to_start_leader;

        // Get the sharable root bank
        let root_bank = bank_forks.read().unwrap().sharable_root_bank();

        let shared_context = SharedContext {
            blockstore: blockstore.clone(),
            bank_forks: bank_forks.clone(),
            cluster_info: cluster_info.clone(),
            rpc_subscriptions,
            leader_window_notifier,
            vote_history_storage,
        };

        let consensus_metrics = Arc::new(PlRwLock::new(ConsensusMetrics::new(
            root_bank.load().epoch(),
        )));
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
            root_bank: root_bank.clone(),
            consensus_metrics: consensus_metrics.clone(),
        };

        let root_context = RootContext {
            leader_schedule_cache: leader_schedule_cache.clone(),
            snapshot_controller,
            bank_notification_sender,
            drop_bank_sender,
        };

        let timer_manager = Arc::new(PlRwLock::new(TimerManager::new(
            event_sender.clone(),
            exit.clone(),
            consensus_metrics,
        )));

        let event_handler_context = EventHandlerContext {
            exit: exit.clone(),
            start: start.clone(),
            event_receiver,
            timer_manager: Arc::clone(&timer_manager),
            shared_context,
            voting_context,
            root_context,
        };

        let consensus_pool_context = ConsensusPoolContext {
            exit: exit.clone(),
            start: start.clone(),
            cluster_info: cluster_info.clone(),
            my_vote_pubkey: vote_account,
            blockstore,
            root_bank: root_bank.clone(),
            leader_schedule_cache,
            consensus_message_receiver: bls_receiver,
            bls_sender,
            event_sender,
            commitment_sender,
        };

        let event_handler = EventHandler::new(event_handler_context);
        let consensus_pool_service = ConsensusPoolService::new(consensus_pool_context);

        Self {
            start,
            event_handler,
            consensus_pool_service,
            timer_manager,
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
        self.consensus_pool_service.join()?;

        // Loop till we manage to unwrap the Arc and then we can join.
        let mut timer_manager = self.timer_manager;
        loop {
            match Arc::try_unwrap(timer_manager) {
                Ok(manager) => {
                    manager.into_inner().join();
                    break;
                }
                Err(m) => {
                    timer_manager = m;
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
        self.event_handler.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{vote_history_storage::FileVoteHistoryStorage, votor::LeaderWindowNotifier},
        crossbeam_channel::unbounded,
        solana_bls_signatures::Signature as BLSSignature,
        solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::Blockstore, blockstore_options::BlockstoreOptions, get_tmp_ledger_path,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts, ValidatorVoteKeypairs,
            },
        },
        solana_signer::Signer,
        solana_streamer::socket::SocketAddrSpace,
        solana_votor_messages::consensus_message::{Certificate, ConsensusMessage},
        std::{sync::Arc, thread, time::Duration},
    };

    #[test]
    fn test_start_migration_and_exit() {
        solana_logger::setup();
        let blockstore = Arc::new(
            Blockstore::open_with_options(
                &get_tmp_ledger_path!(),
                BlockstoreOptions::default_for_tests(),
            )
            .unwrap(),
        );
        // Create 10 node validatorvotekeypairs vec
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new(Keypair::new(), Keypair::new(), Keypair::new()))
            .collect::<Vec<_>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![100; validator_keypairs.len()],
        );
        let my_index = 0;
        let my_node_keypair = validator_keypairs[my_index].node_keypair.insecure_clone();
        let my_vote_keypair = validator_keypairs[my_index].vote_keypair.insecure_clone();
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);
        let contact_info = ContactInfo::new_localhost(&my_node_keypair.pubkey(), 0);
        let cluster_info = Arc::new(ClusterInfo::new(
            contact_info,
            Arc::new(my_node_keypair.insecure_clone()),
            SocketAddrSpace::Unspecified,
        ));
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(
            &bank_forks.read().unwrap().root_bank(),
        ));
        let exit = Arc::new(AtomicBool::new(false));
        let (bls_sender, bls_receiver) = unbounded();
        let (commitment_sender, _commitment_receiver) = unbounded();
        let (drop_bank_sender, _drop_bank_receiver) = unbounded();
        let (event_sender, event_receiver) = unbounded();
        let (own_vote_sender, _own_vote_receiver) = unbounded();
        let (consensus_message_sender, consensus_message_receiver) = unbounded();
        let vote_history = VoteHistory::new(my_node_keypair.pubkey(), 0);
        let vote_history_storage = Arc::new(FileVoteHistoryStorage::default());
        let votor = Votor::new(VotorConfig {
            exit: exit.clone(),
            vote_account: my_vote_keypair.pubkey(),
            wait_to_vote_slot: None,
            wait_for_vote_to_start_leader: false,
            vote_history,
            vote_history_storage,
            authorized_voter_keypairs: Arc::new(RwLock::new(vec![Arc::new(my_vote_keypair)])),
            blockstore,
            bank_forks,
            cluster_info,
            leader_schedule_cache,
            rpc_subscriptions: None,
            snapshot_controller: None,
            bls_sender,
            commitment_sender,
            drop_bank_sender,
            bank_notification_sender: None,
            leader_window_notifier: Arc::new(LeaderWindowNotifier::default()),
            event_sender,
            event_receiver,
            own_vote_sender,
            consensus_message_receiver,
        });

        // Give some time to ensure the thread is waiting
        votor.start_migration();
        // Send a FastFinalize cert for slot 0 to ensure bls_receiver gets something.
        consensus_message_sender
            .send(ConsensusMessage::new_certificate(
                Certificate::FinalizeFast(0, Hash::default()),
                vec![],
                BLSSignature::default(),
            ))
            .unwrap();
        thread::sleep(Duration::from_secs(6));
        assert!(!bls_receiver.is_empty());
        exit.store(true, Ordering::Relaxed);
        votor.join().unwrap();
    }
}
