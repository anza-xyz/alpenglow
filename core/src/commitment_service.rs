use {
    crate::consensus::{tower_vote_state::TowerVoteState, Stake},
    crossbeam_channel::{bounded, select, unbounded, Receiver, RecvTimeoutError, Sender},
    solana_measure::measure::Measure,
    solana_metrics::datapoint_info,
    solana_rpc::rpc_subscriptions::RpcSubscriptions,
    solana_runtime::{
        bank::Bank,
        commitment::{BlockCommitment, BlockCommitmentCache, CommitmentSlots, VOTE_THRESHOLD_SIZE},
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    solana_votor::commitment::{AlpenglowCommitmentAggregationData, AlpenglowCommitmentType},
    std::{
        cmp::max,
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

pub struct TowerCommitmentAggregationData {
    bank: Arc<Bank>,
    root: Slot,
    total_stake: Stake,
    // The latest local vote state of the node running this service.
    // Used for commitment aggregation if the node's vote account is staked.
    node_vote_state: (Pubkey, TowerVoteState),
}

impl TowerCommitmentAggregationData {
    pub fn new(
        bank: Arc<Bank>,
        root: Slot,
        total_stake: Stake,
        node_vote_state: (Pubkey, TowerVoteState),
    ) -> Self {
        Self {
            bank,
            root,
            total_stake,
            node_vote_state,
        }
    }
}

fn get_highest_super_majority_root(mut rooted_stake: Vec<(Slot, u64)>, total_stake: u64) -> Slot {
    rooted_stake.sort_by(|a, b| a.0.cmp(&b.0).reverse());
    let mut stake_sum = 0;
    for (root, stake) in rooted_stake {
        stake_sum += stake;
        if (stake_sum as f64 / total_stake as f64) > VOTE_THRESHOLD_SIZE {
            return root;
        }
    }
    0
}

pub struct AggregateCommitmentService {
    t_commitment: JoinHandle<()>,
}

impl AggregateCommitmentService {
    pub fn new(
        exit: Arc<AtomicBool>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        subscriptions: Arc<RpcSubscriptions>,
    ) -> (
        Sender<TowerCommitmentAggregationData>,
        Sender<AlpenglowCommitmentAggregationData>,
        Self,
    ) {
        let (sender, receiver): (
            Sender<TowerCommitmentAggregationData>,
            Receiver<TowerCommitmentAggregationData>,
        ) = unbounded();
        // This channel should not grow unbounded, cap at 1000 messages for now
        let (ag_sender, ag_receiver): (
            Sender<AlpenglowCommitmentAggregationData>,
            Receiver<AlpenglowCommitmentAggregationData>,
        ) = bounded(1000);

        (
            sender,
            ag_sender,
            Self {
                t_commitment: Builder::new()
                    .name("solAggCommitSvc".to_string())
                    .spawn(move || loop {
                        if exit.load(Ordering::Relaxed) {
                            break;
                        }

                        if let Err(RecvTimeoutError::Disconnected) = Self::run(
                            &receiver,
                            &ag_receiver,
                            &block_commitment_cache,
                            &subscriptions,
                            &exit,
                        ) {
                            break;
                        }
                    })
                    .unwrap(),
            },
        )
    }

    fn run(
        receiver: &Receiver<TowerCommitmentAggregationData>,
        ag_receiver: &Receiver<AlpenglowCommitmentAggregationData>,
        block_commitment_cache: &RwLock<BlockCommitmentCache>,
        subscriptions: &Arc<RpcSubscriptions>,
        exit: &AtomicBool,
    ) -> Result<(), RecvTimeoutError> {
        loop {
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }

            let mut aggregate_commitment_time = Measure::start("aggregate-commitment-ms");
            let commitment_slots = select! {
                recv(receiver) -> msg => {
                    let data = msg?;
                    let data = receiver.try_iter().last().unwrap_or(data);
                    let ancestors = data.bank.status_cache_ancestors();
                    if ancestors.is_empty() {
                        continue;
                    }
                    Self::update_commitment_cache(block_commitment_cache, data, ancestors)
                }
                recv(ag_receiver) -> msg => {
                    let data = msg?;
                    let data = ag_receiver.try_iter().last().unwrap_or(data);
                    Self::alpenglow_update_commitment_cache(
                        block_commitment_cache,
                        data.commitment_type,
                        data.slot,
                    )
                }
                default(Duration::from_secs(1)) => continue
            };
            aggregate_commitment_time.stop();

            datapoint_info!(
                "block-commitment-cache",
                (
                    "aggregate-commitment-ms",
                    aggregate_commitment_time.as_ms() as i64,
                    i64
                ),
                (
                    "highest-super-majority-root",
                    commitment_slots.highest_super_majority_root as i64,
                    i64
                ),
                (
                    "highest-confirmed-slot",
                    commitment_slots.highest_confirmed_slot as i64,
                    i64
                ),
            );
            // Triggers rpc_subscription notifications as soon as new commitment data is available,
            // sending just the commitment cache slot information that the notifications thread
            // needs
            subscriptions.notify_subscribers(commitment_slots);
        }
    }

    fn alpenglow_update_commitment_cache(
        block_commitment_cache: &RwLock<BlockCommitmentCache>,
        update_type: AlpenglowCommitmentType,
        slot: Slot,
    ) -> CommitmentSlots {
        let mut w_block_commitment_cache = block_commitment_cache.write().unwrap();

        match update_type {
            AlpenglowCommitmentType::Notarize => {
                w_block_commitment_cache.set_slot(slot);
            }
            AlpenglowCommitmentType::Finalized => {
                w_block_commitment_cache.set_highest_confirmed_slot(slot);
                w_block_commitment_cache.set_root(slot);
                w_block_commitment_cache.set_highest_super_majority_root(slot);
            }
        }
        w_block_commitment_cache.commitment_slots()
    }

    fn update_commitment_cache(
        block_commitment_cache: &RwLock<BlockCommitmentCache>,
        aggregation_data: TowerCommitmentAggregationData,
        ancestors: Vec<u64>,
    ) -> CommitmentSlots {
        let (block_commitment, rooted_stake) = Self::aggregate_commitment(
            &ancestors,
            &aggregation_data.bank,
            &aggregation_data.node_vote_state,
        );

        let highest_super_majority_root =
            get_highest_super_majority_root(rooted_stake, aggregation_data.total_stake);

        let mut new_block_commitment = BlockCommitmentCache::new(
            block_commitment,
            aggregation_data.total_stake,
            CommitmentSlots {
                slot: aggregation_data.bank.slot(),
                root: aggregation_data.root,
                highest_confirmed_slot: aggregation_data.root,
                highest_super_majority_root,
            },
        );
        let highest_confirmed_slot = new_block_commitment.calculate_highest_confirmed_slot();
        new_block_commitment.set_highest_confirmed_slot(highest_confirmed_slot);

        let mut w_block_commitment_cache = block_commitment_cache.write().unwrap();

        let highest_super_majority_root = max(
            new_block_commitment.highest_super_majority_root(),
            w_block_commitment_cache.highest_super_majority_root(),
        );
        new_block_commitment.set_highest_super_majority_root(highest_super_majority_root);

        *w_block_commitment_cache = new_block_commitment;
        w_block_commitment_cache.commitment_slots()
    }

    pub fn aggregate_commitment(
        ancestors: &[Slot],
        bank: &Bank,
        (node_vote_pubkey, node_vote_state): &(Pubkey, TowerVoteState),
    ) -> (HashMap<Slot, BlockCommitment>, Vec<(Slot, u64)>) {
        assert!(!ancestors.is_empty());

        // Check ancestors is sorted
        for a in ancestors.windows(2) {
            assert!(a[0] < a[1]);
        }

        let mut commitment = HashMap::new();
        let mut rooted_stake: Vec<(Slot, u64)> = Vec::new();
        for (pubkey, (lamports, account)) in bank.vote_accounts().iter() {
            if *lamports == 0 {
                continue;
            }
            let vote_state = if pubkey == node_vote_pubkey {
                // Override old vote_state in bank with latest one for my own vote pubkey
                node_vote_state.clone()
            } else if let Some(vote_state_view) = account.vote_state_view() {
                TowerVoteState::from(vote_state_view)
            } else {
                // Alpenglow doesn't need to aggregate commitment.
                continue;
            };
            Self::aggregate_commitment_for_vote_account(
                &mut commitment,
                &mut rooted_stake,
                &vote_state,
                ancestors,
                *lamports,
            );
        }

        (commitment, rooted_stake)
    }

    fn aggregate_commitment_for_vote_account(
        commitment: &mut HashMap<Slot, BlockCommitment>,
        rooted_stake: &mut Vec<(Slot, u64)>,
        vote_state: &TowerVoteState,
        ancestors: &[Slot],
        lamports: u64,
    ) {
        assert!(!ancestors.is_empty());
        let mut ancestors_index = 0;
        if let Some(root) = vote_state.root_slot {
            for (i, a) in ancestors.iter().enumerate() {
                if *a <= root {
                    commitment
                        .entry(*a)
                        .or_default()
                        .increase_rooted_stake(lamports);
                } else {
                    ancestors_index = i;
                    break;
                }
            }
            rooted_stake.push((root, lamports));
        }

        for vote in &vote_state.votes {
            while ancestors[ancestors_index] <= vote.slot() {
                commitment
                    .entry(ancestors[ancestors_index])
                    .or_default()
                    .increase_confirmation_stake(vote.confirmation_count() as usize, lamports);
                ancestors_index += 1;

                if ancestors_index == ancestors.len() {
                    return;
                }
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_commitment.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo},
        solana_runtime::{
            accounts_background_service::AbsRequestSender,
            bank_forks::BankForks,
            genesis_utils::{create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs},
        },
        solana_sdk::{account::Account, pubkey::Pubkey, signature::Signer},
        solana_stake_program::stake_state,
        solana_vote::vote_transaction,
        solana_vote_program::vote_state::{
            self, process_slot_vote_unchecked, TowerSync, VoteStateVersions, MAX_LOCKOUT_HISTORY,
        },
    };

    fn new_bank_from_parent_with_bank_forks(
        bank_forks: &RwLock<BankForks>,
        parent: Arc<Bank>,
        collector_id: &Pubkey,
        slot: Slot,
    ) -> Arc<Bank> {
        let bank = Bank::new_from_parent(parent, collector_id, slot);
        bank_forks
            .write()
            .unwrap()
            .insert(bank)
            .clone_without_scheduler()
    }

    #[test]
    fn test_get_highest_super_majority_root() {
        assert_eq!(get_highest_super_majority_root(vec![], 10), 0);
        let rooted_stake = vec![(0, 5), (1, 5)];
        assert_eq!(get_highest_super_majority_root(rooted_stake, 10), 0);
        let rooted_stake = vec![(1, 5), (0, 10), (2, 5), (1, 4)];
        assert_eq!(get_highest_super_majority_root(rooted_stake, 10), 1);
    }

    #[test]
    fn test_aggregate_commitment_for_vote_account_1() {
        let ancestors = vec![3, 4, 5, 7, 9, 11];
        let mut commitment = HashMap::new();
        let mut rooted_stake = vec![];
        let lamports = 5;
        let mut vote_state = TowerVoteState::default();

        let root = *ancestors.last().unwrap();
        vote_state.root_slot = Some(root);
        AggregateCommitmentService::aggregate_commitment_for_vote_account(
            &mut commitment,
            &mut rooted_stake,
            &vote_state,
            &ancestors,
            lamports,
        );

        for a in ancestors {
            let mut expected = BlockCommitment::default();
            expected.increase_rooted_stake(lamports);
            assert_eq!(*commitment.get(&a).unwrap(), expected);
        }
        assert_eq!(rooted_stake[0], (root, lamports));
    }

    #[test]
    fn test_aggregate_commitment_for_vote_account_2() {
        let ancestors = vec![3, 4, 5, 7, 9, 11];
        let mut commitment = HashMap::new();
        let mut rooted_stake = vec![];
        let lamports = 5;
        let mut vote_state = TowerVoteState::default();

        let root = ancestors[2];
        vote_state.root_slot = Some(root);
        vote_state.process_next_vote_slot(*ancestors.last().unwrap());
        AggregateCommitmentService::aggregate_commitment_for_vote_account(
            &mut commitment,
            &mut rooted_stake,
            &vote_state,
            &ancestors,
            lamports,
        );

        for a in ancestors {
            let mut expected = BlockCommitment::default();
            if a <= root {
                expected.increase_rooted_stake(lamports);
            } else {
                expected.increase_confirmation_stake(1, lamports);
            }
            assert_eq!(*commitment.get(&a).unwrap(), expected);
        }
        assert_eq!(rooted_stake[0], (root, lamports));
    }

    #[test]
    fn test_aggregate_commitment_for_vote_account_3() {
        let ancestors = vec![3, 4, 5, 7, 9, 10, 11];
        let mut commitment = HashMap::new();
        let mut rooted_stake = vec![];
        let lamports = 5;
        let mut vote_state = TowerVoteState::default();

        let root = ancestors[2];
        vote_state.root_slot = Some(root);
        assert!(ancestors[4] + 2 >= ancestors[6]);
        vote_state.process_next_vote_slot(ancestors[4]);
        vote_state.process_next_vote_slot(ancestors[6]);
        AggregateCommitmentService::aggregate_commitment_for_vote_account(
            &mut commitment,
            &mut rooted_stake,
            &vote_state,
            &ancestors,
            lamports,
        );

        for (i, a) in ancestors.iter().enumerate() {
            if *a <= root {
                let mut expected = BlockCommitment::default();
                expected.increase_rooted_stake(lamports);
                assert_eq!(*commitment.get(a).unwrap(), expected);
            } else if i <= 4 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(2, lamports);
                assert_eq!(*commitment.get(a).unwrap(), expected);
            } else if i <= 6 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(1, lamports);
                assert_eq!(*commitment.get(a).unwrap(), expected);
            }
        }
        assert_eq!(rooted_stake[0], (root, lamports));
    }

    fn do_test_aggregate_commitment_validity(with_node_vote_state: bool) {
        let ancestors = vec![3, 4, 5, 7, 9, 10, 11];
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(10_000);

        let rooted_stake_amount = 40;

        let sk1 = solana_pubkey::new_rand();
        let pk1 = solana_pubkey::new_rand();
        let mut vote_account1 =
            vote_state::create_account(&pk1, &solana_pubkey::new_rand(), 0, 100);
        let stake_account1 =
            stake_state::create_account(&sk1, &pk1, &vote_account1, &genesis_config.rent, 100);
        let sk2 = solana_pubkey::new_rand();
        let pk2 = solana_pubkey::new_rand();
        let mut vote_account2 = vote_state::create_account(&pk2, &solana_pubkey::new_rand(), 0, 50);
        let stake_account2 =
            stake_state::create_account(&sk2, &pk2, &vote_account2, &genesis_config.rent, 50);
        let sk3 = solana_pubkey::new_rand();
        let pk3 = solana_pubkey::new_rand();
        let mut vote_account3 = vote_state::create_account(&pk3, &solana_pubkey::new_rand(), 0, 1);
        let stake_account3 = stake_state::create_account(
            &sk3,
            &pk3,
            &vote_account3,
            &genesis_config.rent,
            rooted_stake_amount,
        );
        let sk4 = solana_pubkey::new_rand();
        let pk4 = solana_pubkey::new_rand();
        let mut vote_account4 = vote_state::create_account(&pk4, &solana_pubkey::new_rand(), 0, 1);
        let stake_account4 = stake_state::create_account(
            &sk4,
            &pk4,
            &vote_account4,
            &genesis_config.rent,
            rooted_stake_amount,
        );

        genesis_config.accounts.extend(
            vec![
                (pk1, vote_account1.clone()),
                (sk1, stake_account1),
                (pk2, vote_account2.clone()),
                (sk2, stake_account2),
                (pk3, vote_account3.clone()),
                (sk3, stake_account3),
                (pk4, vote_account4.clone()),
                (sk4, stake_account4),
            ]
            .into_iter()
            .map(|(key, account)| (key, Account::from(account))),
        );

        // Create bank
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));

        let mut vote_state1 = vote_state::from(&vote_account1).unwrap();
        process_slot_vote_unchecked(&mut vote_state1, 3);
        process_slot_vote_unchecked(&mut vote_state1, 5);
        if !with_node_vote_state {
            let versioned = VoteStateVersions::new_current(vote_state1.clone());
            vote_state::to(&versioned, &mut vote_account1).unwrap();
            bank.store_account(&pk1, &vote_account1);
        }

        let mut vote_state2 = vote_state::from(&vote_account2).unwrap();
        process_slot_vote_unchecked(&mut vote_state2, 9);
        process_slot_vote_unchecked(&mut vote_state2, 10);
        let versioned = VoteStateVersions::new_current(vote_state2);
        vote_state::to(&versioned, &mut vote_account2).unwrap();
        bank.store_account(&pk2, &vote_account2);

        let mut vote_state3 = vote_state::from(&vote_account3).unwrap();
        vote_state3.root_slot = Some(1);
        let versioned = VoteStateVersions::new_current(vote_state3);
        vote_state::to(&versioned, &mut vote_account3).unwrap();
        bank.store_account(&pk3, &vote_account3);

        let mut vote_state4 = vote_state::from(&vote_account4).unwrap();
        vote_state4.root_slot = Some(2);
        let versioned = VoteStateVersions::new_current(vote_state4);
        vote_state::to(&versioned, &mut vote_account4).unwrap();
        bank.store_account(&pk4, &vote_account4);

        let node_vote_pubkey = if with_node_vote_state {
            pk1
        } else {
            // Use some random pubkey as dummy to suppress the override.
            solana_pubkey::new_rand()
        };

        let (commitment, rooted_stake) = AggregateCommitmentService::aggregate_commitment(
            &ancestors,
            &bank,
            &(node_vote_pubkey, TowerVoteState::from(vote_state1)),
        );

        for a in ancestors {
            if a <= 3 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(2, 150);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if a <= 5 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(1, 100);
                expected.increase_confirmation_stake(2, 50);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if a <= 9 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(2, 50);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if a <= 10 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(1, 50);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else {
                assert!(!commitment.contains_key(&a));
            }
        }
        assert_eq!(rooted_stake.len(), 2);
        assert_eq!(get_highest_super_majority_root(rooted_stake, 100), 1)
    }

    #[test]
    fn test_aggregate_commitment_validity_with_node_vote_state() {
        do_test_aggregate_commitment_validity(true)
    }

    #[test]
    fn test_aggregate_commitment_validity_without_node_vote_state() {
        do_test_aggregate_commitment_validity(false);
    }

    #[test]
    fn test_highest_super_majority_root_advance() {
        fn get_vote_state(vote_pubkey: Pubkey, bank: &Bank) -> TowerVoteState {
            let vote_account = bank.get_vote_account(&vote_pubkey).unwrap();
            TowerVoteState::from(vote_account.vote_state_view().unwrap())
        }

        let block_commitment_cache = RwLock::new(BlockCommitmentCache::new_for_tests());

        let validator_vote_keypairs = ValidatorVoteKeypairs::new_rand();
        let validator_keypairs = vec![&validator_vote_keypairs];
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config_with_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![100; 1],
        );

        let (_bank0, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        // Fill bank_forks with banks with votes landing in the next slot
        // Create enough banks such that vote account will root slots 0 and 1
        for x in 0..33 {
            let previous_bank = bank_forks.read().unwrap().get(x).unwrap();
            let bank = new_bank_from_parent_with_bank_forks(
                bank_forks.as_ref(),
                previous_bank.clone(),
                &Pubkey::default(),
                x + 1,
            );
            let tower_sync = TowerSync::new_from_slot(x, previous_bank.hash());
            let vote = vote_transaction::new_tower_sync_transaction(
                tower_sync,
                previous_bank.last_blockhash(),
                &validator_vote_keypairs.node_keypair,
                &validator_vote_keypairs.vote_keypair,
                &validator_vote_keypairs.vote_keypair,
                None,
            );
            bank.process_transaction(&vote).unwrap();
        }

        let working_bank = bank_forks.read().unwrap().working_bank();
        let vote_pubkey = validator_vote_keypairs.vote_keypair.pubkey();
        let root = get_vote_state(vote_pubkey, &working_bank)
            .root_slot
            .unwrap();
        for x in 0..root {
            bank_forks
                .write()
                .unwrap()
                .set_root(x, &AbsRequestSender::default(), None)
                .unwrap();
        }

        // Add an additional bank/vote that will root slot 2
        let bank33 = bank_forks.read().unwrap().get(33).unwrap();
        let bank34 = new_bank_from_parent_with_bank_forks(
            bank_forks.as_ref(),
            bank33.clone(),
            &Pubkey::default(),
            34,
        );
        let tower_sync = TowerSync::new_from_slot(33, bank33.hash());
        let vote33 = vote_transaction::new_tower_sync_transaction(
            tower_sync,
            bank33.last_blockhash(),
            &validator_vote_keypairs.node_keypair,
            &validator_vote_keypairs.vote_keypair,
            &validator_vote_keypairs.vote_keypair,
            None,
        );
        bank34.process_transaction(&vote33).unwrap();

        let working_bank = bank_forks.read().unwrap().working_bank();
        let vote_state = get_vote_state(vote_pubkey, &working_bank);
        let root = vote_state.root_slot.unwrap();
        let ancestors = working_bank.status_cache_ancestors();
        let _ = AggregateCommitmentService::update_commitment_cache(
            &block_commitment_cache,
            TowerCommitmentAggregationData {
                bank: working_bank,
                root: 0,
                total_stake: 100,
                node_vote_state: (vote_pubkey, vote_state.clone()),
            },
            ancestors,
        );
        let highest_super_majority_root = block_commitment_cache
            .read()
            .unwrap()
            .highest_super_majority_root();
        bank_forks
            .write()
            .unwrap()
            .set_root(
                root,
                &AbsRequestSender::default(),
                Some(highest_super_majority_root),
            )
            .unwrap();
        let highest_super_majority_root_bank =
            bank_forks.read().unwrap().get(highest_super_majority_root);
        assert!(highest_super_majority_root_bank.is_some());

        // Add a forked bank. Because the vote for bank 33 landed in the non-ancestor, the vote
        // account's root (and thus the highest_super_majority_root) rolls back to slot 1
        let bank33 = bank_forks.read().unwrap().get(33).unwrap();
        let _bank35 = new_bank_from_parent_with_bank_forks(
            bank_forks.as_ref(),
            bank33,
            &Pubkey::default(),
            35,
        );

        let working_bank = bank_forks.read().unwrap().working_bank();
        let ancestors = working_bank.status_cache_ancestors();
        let _ = AggregateCommitmentService::update_commitment_cache(
            &block_commitment_cache,
            TowerCommitmentAggregationData {
                bank: working_bank,
                root: 1,
                total_stake: 100,
                node_vote_state: (vote_pubkey, vote_state),
            },
            ancestors,
        );
        let highest_super_majority_root = block_commitment_cache
            .read()
            .unwrap()
            .highest_super_majority_root();
        let highest_super_majority_root_bank =
            bank_forks.read().unwrap().get(highest_super_majority_root);
        assert!(highest_super_majority_root_bank.is_some());

        // Add additional banks beyond lockout built on the new fork to ensure that behavior
        // continues normally
        for x in 35..=37 {
            let previous_bank = bank_forks.read().unwrap().get(x).unwrap();
            let bank = new_bank_from_parent_with_bank_forks(
                bank_forks.as_ref(),
                previous_bank.clone(),
                &Pubkey::default(),
                x + 1,
            );
            // Skip 34 as it is not part of this fork.
            let lowest_slot = x - MAX_LOCKOUT_HISTORY as u64;
            let slots: Vec<_> = (lowest_slot..(x + 1)).filter(|s| *s != 34).collect();
            let tower_sync =
                TowerSync::new_from_slots(slots, previous_bank.hash(), Some(lowest_slot - 1));
            let vote = vote_transaction::new_tower_sync_transaction(
                tower_sync,
                previous_bank.last_blockhash(),
                &validator_vote_keypairs.node_keypair,
                &validator_vote_keypairs.vote_keypair,
                &validator_vote_keypairs.vote_keypair,
                None,
            );
            bank.process_transaction(&vote).unwrap();
        }

        let working_bank = bank_forks.read().unwrap().working_bank();
        let vote_state =
            get_vote_state(validator_vote_keypairs.vote_keypair.pubkey(), &working_bank);
        let root = vote_state.root_slot.unwrap();
        let ancestors = working_bank.status_cache_ancestors();
        let _ = AggregateCommitmentService::update_commitment_cache(
            &block_commitment_cache,
            TowerCommitmentAggregationData {
                bank: working_bank,
                root: 0,
                total_stake: 100,
                node_vote_state: (vote_pubkey, vote_state),
            },
            ancestors,
        );
        let highest_super_majority_root = block_commitment_cache
            .read()
            .unwrap()
            .highest_super_majority_root();
        bank_forks
            .write()
            .unwrap()
            .set_root(
                root,
                &AbsRequestSender::default(),
                Some(highest_super_majority_root),
            )
            .unwrap();
        let highest_super_majority_root_bank =
            bank_forks.read().unwrap().get(highest_super_majority_root);
        assert!(highest_super_majority_root_bank.is_some());
    }
}
