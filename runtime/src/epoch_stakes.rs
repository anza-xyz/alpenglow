use {
    crate::stakes::{serde_stakes_to_delegation_format, SerdeStakesToStakeFormat, StakesEnum},
    serde::{Deserialize, Serialize},
    solana_bls_signatures::Pubkey as BLSPubkey,
    solana_sdk::{clock::Epoch, pubkey::Pubkey},
    solana_vote::vote_account::VoteAccountsHashMap,
    std::{collections::HashMap, sync::Arc},
};

pub type NodeIdToVoteAccounts = HashMap<Pubkey, NodeVoteAccounts>;
pub type EpochAuthorizedVoters = HashMap<Pubkey, Pubkey>;
pub type SortedPubkeys = Vec<(Pubkey, BLSPubkey)>;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "dev-context-only-utils", derive(PartialEq))]
pub struct BLSPubkeyToRankMap {
    rank_map: HashMap<BLSPubkey, u16>,
    //TODO(wen): We can make SortedPubkeys a Vec<BLSPubkey> after we remove ed25519
    // pubkey from certificate pool.
    sorted_pubkeys: Vec<(Pubkey, BLSPubkey)>,
}

impl BLSPubkeyToRankMap {
    pub fn new(epoch_vote_accounts_hash_map: &VoteAccountsHashMap) -> Self {
        let mut pubkey_stake_pair_vec: Vec<(Pubkey, BLSPubkey, u64)> = epoch_vote_accounts_hash_map
            .iter()
            .filter_map(|(pubkey, (stake, account))| {
                if *stake > 0 {
                    account
                        .bls_pubkey()
                        .map(|bls_pubkey| (*pubkey, *bls_pubkey, *stake))
                } else {
                    None
                }
            })
            .collect();
        pubkey_stake_pair_vec.sort_by(|(_, a_pubkey, a_stake), (_, b_pubkey, b_stake)| {
            b_stake.cmp(a_stake).then(a_pubkey.cmp(b_pubkey))
        });
        let mut sorted_pubkeys = Vec::new();
        let mut bls_pubkey_to_rank_map = HashMap::new();
        for (rank, (pubkey, bls_pubkey, _stake)) in pubkey_stake_pair_vec.into_iter().enumerate() {
            sorted_pubkeys.push((pubkey, bls_pubkey));
            bls_pubkey_to_rank_map.insert(bls_pubkey, rank as u16);
        }
        Self {
            rank_map: bls_pubkey_to_rank_map,
            sorted_pubkeys,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rank_map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rank_map.len()
    }

    pub fn get_rank(&self, bls_pubkey: &BLSPubkey) -> Option<&u16> {
        self.rank_map.get(bls_pubkey)
    }

    pub fn get_pubkey(&self, index: usize) -> Option<&(Pubkey, BLSPubkey)> {
        self.sorted_pubkeys.get(index)
    }
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Clone, Serialize, Debug, Deserialize, Default, PartialEq, Eq)]
pub struct NodeVoteAccounts {
    pub vote_accounts: Vec<Pubkey>,
    pub total_stake: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "dev-context-only-utils", derive(PartialEq))]
pub struct EpochStakes {
    #[serde(with = "serde_stakes_to_delegation_format")]
    stakes: Arc<StakesEnum>,
    total_stake: u64,
    node_id_to_vote_accounts: Arc<NodeIdToVoteAccounts>,
    epoch_authorized_voters: Arc<EpochAuthorizedVoters>,
    bls_pubkey_to_rank_map: Arc<BLSPubkeyToRankMap>,
}

impl EpochStakes {
    pub(crate) fn new(stakes: Arc<StakesEnum>, leader_schedule_epoch: Epoch) -> Self {
        let epoch_vote_accounts = stakes.vote_accounts();
        let (total_stake, node_id_to_vote_accounts, epoch_authorized_voters) =
            Self::parse_epoch_vote_accounts(epoch_vote_accounts.as_ref(), leader_schedule_epoch);
        let bls_pubkey_to_rank_map = BLSPubkeyToRankMap::new(epoch_vote_accounts.as_ref());
        Self {
            stakes,
            total_stake,
            node_id_to_vote_accounts: Arc::new(node_id_to_vote_accounts),
            epoch_authorized_voters: Arc::new(epoch_authorized_voters),
            bls_pubkey_to_rank_map: Arc::new(bls_pubkey_to_rank_map),
        }
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests(
        vote_accounts_hash_map: VoteAccountsHashMap,
        leader_schedule_epoch: Epoch,
    ) -> Self {
        Self::new(
            Arc::new(StakesEnum::Accounts(crate::stakes::Stakes::new_for_tests(
                0,
                solana_vote::vote_account::VoteAccounts::from(Arc::new(vote_accounts_hash_map)),
                im::HashMap::default(),
            ))),
            leader_schedule_epoch,
        )
    }

    pub fn stakes(&self) -> &StakesEnum {
        &self.stakes
    }

    pub fn total_stake(&self) -> u64 {
        self.total_stake
    }

    /// For tests
    pub fn set_total_stake(&mut self, total_stake: u64) {
        self.total_stake = total_stake;
    }

    pub fn node_id_to_vote_accounts(&self) -> &Arc<NodeIdToVoteAccounts> {
        &self.node_id_to_vote_accounts
    }

    pub fn node_id_to_stake(&self, node_id: &Pubkey) -> Option<u64> {
        self.node_id_to_vote_accounts
            .get(node_id)
            .map(|x| x.total_stake)
    }

    pub fn epoch_authorized_voters(&self) -> &Arc<EpochAuthorizedVoters> {
        &self.epoch_authorized_voters
    }

    pub fn bls_pubkey_to_rank_map(&self) -> &Arc<BLSPubkeyToRankMap> {
        &self.bls_pubkey_to_rank_map
    }

    pub fn vote_account_stake(&self, vote_account: &Pubkey) -> u64 {
        self.stakes
            .vote_accounts()
            .get_delegated_stake(vote_account)
    }

    fn parse_epoch_vote_accounts(
        epoch_vote_accounts: &VoteAccountsHashMap,
        leader_schedule_epoch: Epoch,
    ) -> (u64, NodeIdToVoteAccounts, EpochAuthorizedVoters) {
        let mut node_id_to_vote_accounts: NodeIdToVoteAccounts = HashMap::new();
        let total_stake = epoch_vote_accounts
            .iter()
            .map(|(_, (stake, _))| stake)
            .sum();
        let epoch_authorized_voters = epoch_vote_accounts
            .iter()
            .filter_map(|(key, (stake, account))| {
                if *stake > 0 {
                    if let Some(authorized_voter) =
                        account.get_authorized_voter(leader_schedule_epoch)
                    {
                        let node_vote_accounts = node_id_to_vote_accounts
                            .entry(*account.node_pubkey())
                            .or_default();

                        node_vote_accounts.total_stake += stake;
                        node_vote_accounts.vote_accounts.push(*key);

                        Some((*key, authorized_voter))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        (
            total_stake,
            node_id_to_vote_accounts,
            epoch_authorized_voters,
        )
    }
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample, AbiEnumVisitor))]
#[cfg_attr(feature = "dev-context-only-utils", derive(PartialEq))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VersionedEpochStakes {
    Current {
        stakes: SerdeStakesToStakeFormat,
        total_stake: u64,
        node_id_to_vote_accounts: Arc<NodeIdToVoteAccounts>,
        epoch_authorized_voters: Arc<EpochAuthorizedVoters>,
    },
}

impl From<VersionedEpochStakes> for EpochStakes {
    fn from(versioned: VersionedEpochStakes) -> Self {
        let VersionedEpochStakes::Current {
            stakes,
            total_stake,
            node_id_to_vote_accounts,
            epoch_authorized_voters,
        } = versioned;

        let stakes: Arc<StakesEnum> = Arc::new(stakes.into());
        let bls_pubkey_to_rank_map = BLSPubkeyToRankMap::new(stakes.vote_accounts().as_ref());
        Self {
            stakes,
            total_stake,
            node_id_to_vote_accounts,
            epoch_authorized_voters,
            bls_pubkey_to_rank_map: Arc::new(bls_pubkey_to_rank_map),
        }
    }
}

/// Only the `StakesEnum::Delegations` variant is unable to be serialized as a
/// `StakesEnum::Stakes` variant, so leave those entries and split off the other
/// epoch stakes enum variants into a new map which will be serialized into the
/// new `versioned_epoch_stakes` snapshot field.  After a cluster transitions to
/// serializing epoch stakes in the new format, `StakesEnum::Delegations`
/// variants for recent epochs will no longer be created and can be deprecated.
pub(crate) fn split_epoch_stakes(
    bank_epoch_stakes: HashMap<Epoch, EpochStakes>,
) -> (
    HashMap<Epoch, EpochStakes>,
    HashMap<Epoch, VersionedEpochStakes>,
) {
    let mut old_epoch_stakes = HashMap::new();
    let mut versioned_epoch_stakes = HashMap::new();
    for (epoch, epoch_stakes) in bank_epoch_stakes.into_iter() {
        let EpochStakes {
            stakes,
            total_stake,
            node_id_to_vote_accounts,
            epoch_authorized_voters,
            bls_pubkey_to_rank_map,
        } = epoch_stakes;
        match stakes.as_ref() {
            StakesEnum::Delegations(_) => {
                old_epoch_stakes.insert(
                    epoch,
                    EpochStakes {
                        stakes: stakes.clone(),
                        total_stake,
                        node_id_to_vote_accounts,
                        epoch_authorized_voters,
                        bls_pubkey_to_rank_map,
                    },
                );
            }
            StakesEnum::Accounts(stakes) => {
                versioned_epoch_stakes.insert(
                    epoch,
                    VersionedEpochStakes::Current {
                        stakes: SerdeStakesToStakeFormat::Account(stakes.clone()),
                        total_stake,
                        node_id_to_vote_accounts,
                        epoch_authorized_voters,
                    },
                );
            }
            StakesEnum::Stakes(stakes) => {
                versioned_epoch_stakes.insert(
                    epoch,
                    VersionedEpochStakes::Current {
                        stakes: SerdeStakesToStakeFormat::Stake(stakes.clone()),
                        total_stake,
                        node_id_to_vote_accounts,
                        epoch_authorized_voters,
                    },
                );
            }
        }
    }
    (old_epoch_stakes, versioned_epoch_stakes)
}

#[cfg(test)]
pub(crate) mod tests {
    use {
        super::*,
        crate::{
            stake_account::StakeAccount,
            stakes::{Stakes, StakesCache},
        },
        solana_bls_signatures::keypair::Keypair as BLSKeypair,
        solana_sdk::{account::AccountSharedData, rent::Rent},
        solana_stake_program::stake_state::{self, Delegation, Stake},
        solana_vote::vote_account::VoteAccount,
        solana_vote_program::vote_state::{self, create_account_with_authorized},
        solana_votor_messages::state::VoteState as AlpenglowVoteState,
        std::iter,
        test_case::test_case,
    };

    struct VoteAccountInfo {
        vote_account: Pubkey,
        account: AccountSharedData,
        authorized_voter: Pubkey,
    }

    fn new_vote_accounts(
        num_nodes: usize,
        num_vote_accounts_per_node: usize,
        is_alpenglow: bool,
    ) -> HashMap<Pubkey, Vec<VoteAccountInfo>> {
        // Create some vote accounts for each pubkey
        (0..num_nodes)
            .map(|_| {
                let node_id = solana_pubkey::new_rand();
                (
                    node_id,
                    iter::repeat_with(|| {
                        let authorized_voter = solana_pubkey::new_rand();
                        let bls_keypair = BLSKeypair::new();
                        let account = if is_alpenglow {
                            AlpenglowVoteState::create_account_with_authorized(
                                &node_id,
                                &authorized_voter,
                                &node_id,
                                0,
                                100,
                                bls_keypair.public.into(),
                            )
                        } else {
                            create_account_with_authorized(
                                &node_id,
                                &authorized_voter,
                                &node_id,
                                0,
                                100,
                            )
                        };
                        VoteAccountInfo {
                            vote_account: solana_pubkey::new_rand(),
                            account,
                            authorized_voter,
                        }
                    })
                    .take(num_vote_accounts_per_node)
                    .collect(),
                )
            })
            .collect()
    }

    fn new_epoch_vote_accounts(
        vote_accounts_map: &HashMap<Pubkey, Vec<VoteAccountInfo>>,
        node_id_to_stake_fn: impl Fn(&Pubkey) -> u64,
    ) -> VoteAccountsHashMap {
        // Create and process the vote accounts
        vote_accounts_map
            .iter()
            .flat_map(|(node_id, vote_accounts)| {
                vote_accounts.iter().map(|v| {
                    let vote_account = VoteAccount::try_from(v.account.clone()).unwrap();
                    (v.vote_account, (node_id_to_stake_fn(node_id), vote_account))
                })
            })
            .collect()
    }

    #[test_case(true; "alpenglow")]
    #[test_case(false; "towerbft")]
    fn test_parse_epoch_vote_accounts(is_alpenglow: bool) {
        let stake_per_account = 100;
        let num_vote_accounts_per_node = 2;
        let num_nodes = 10;

        let vote_accounts_map =
            new_vote_accounts(num_nodes, num_vote_accounts_per_node, is_alpenglow);

        let expected_authorized_voters: HashMap<_, _> = vote_accounts_map
            .iter()
            .flat_map(|(_, vote_accounts)| {
                vote_accounts
                    .iter()
                    .map(|v| (v.vote_account, v.authorized_voter))
            })
            .collect();

        let expected_node_id_to_vote_accounts: HashMap<_, _> = vote_accounts_map
            .iter()
            .map(|(node_pubkey, vote_accounts)| {
                let mut vote_accounts = vote_accounts
                    .iter()
                    .map(|v| (v.vote_account))
                    .collect::<Vec<_>>();
                vote_accounts.sort();
                let node_vote_accounts = NodeVoteAccounts {
                    vote_accounts,
                    total_stake: stake_per_account * num_vote_accounts_per_node as u64,
                };
                (*node_pubkey, node_vote_accounts)
            })
            .collect();

        let epoch_vote_accounts =
            new_epoch_vote_accounts(&vote_accounts_map, |_| stake_per_account);

        let (total_stake, mut node_id_to_vote_accounts, epoch_authorized_voters) =
            EpochStakes::parse_epoch_vote_accounts(&epoch_vote_accounts, 0);

        // Verify the results
        node_id_to_vote_accounts
            .iter_mut()
            .for_each(|(_, node_vote_accounts)| node_vote_accounts.vote_accounts.sort());

        assert!(
            node_id_to_vote_accounts.len() == expected_node_id_to_vote_accounts.len()
                && node_id_to_vote_accounts
                    .iter()
                    .all(|(k, v)| expected_node_id_to_vote_accounts.get(k).unwrap() == v)
        );
        assert!(
            epoch_authorized_voters.len() == expected_authorized_voters.len()
                && epoch_authorized_voters
                    .iter()
                    .all(|(k, v)| expected_authorized_voters.get(k).unwrap() == v)
        );
        assert_eq!(
            total_stake,
            num_nodes as u64 * num_vote_accounts_per_node as u64 * 100
        );
    }

    fn create_test_stakes() -> Stakes<StakeAccount<Delegation>> {
        let stakes_cache = StakesCache::new(Stakes::default());

        let vote_pubkey = Pubkey::new_unique();
        let vote_account = vote_state::create_account_with_authorized(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            0,
            1,
        );

        let stake = 1_000_000_000;
        let stake_pubkey = Pubkey::new_unique();
        let stake_account = stake_state::create_account(
            &Pubkey::new_unique(),
            &vote_pubkey,
            &vote_account,
            &Rent::default(),
            stake,
        );

        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);

        let stakes = Stakes::clone(&stakes_cache.stakes());

        stakes
    }

    #[test]
    fn test_split_epoch_stakes_empty() {
        let bank_epoch_stakes = HashMap::new();
        let (old, versioned) = split_epoch_stakes(bank_epoch_stakes);
        assert!(old.is_empty());
        assert!(versioned.is_empty());
    }

    #[test]
    fn test_split_epoch_stakes_delegations() {
        let mut bank_epoch_stakes = HashMap::new();
        let epoch = 0;
        let stakes = Arc::new(StakesEnum::Delegations(create_test_stakes().into()));
        let epoch_stakes = EpochStakes {
            stakes,
            total_stake: 100,
            node_id_to_vote_accounts: Arc::new(HashMap::new()),
            epoch_authorized_voters: Arc::new(HashMap::new()),
            bls_pubkey_to_rank_map: Arc::new(BLSPubkeyToRankMap::default()),
        };
        bank_epoch_stakes.insert(epoch, epoch_stakes.clone());

        let (old, versioned) = split_epoch_stakes(bank_epoch_stakes);

        assert_eq!(old.len(), 1);
        assert_eq!(old.get(&epoch), Some(&epoch_stakes));
        assert!(versioned.is_empty());
    }

    #[test]
    fn test_split_epoch_stakes_accounts() {
        let mut bank_epoch_stakes = HashMap::new();
        let epoch = 0;
        let test_stakes = create_test_stakes();
        let stakes = Arc::new(StakesEnum::Accounts(test_stakes.clone()));
        let epoch_stakes = EpochStakes {
            stakes,
            total_stake: 100,
            node_id_to_vote_accounts: Arc::new(HashMap::new()),
            epoch_authorized_voters: Arc::new(HashMap::new()),
            bls_pubkey_to_rank_map: Arc::new(BLSPubkeyToRankMap::default()),
        };
        bank_epoch_stakes.insert(epoch, epoch_stakes.clone());

        let (old, versioned) = split_epoch_stakes(bank_epoch_stakes);

        assert!(old.is_empty());
        assert_eq!(versioned.len(), 1);
        assert_eq!(
            versioned.get(&epoch),
            Some(&VersionedEpochStakes::Current {
                stakes: SerdeStakesToStakeFormat::Account(test_stakes),
                total_stake: epoch_stakes.total_stake,
                node_id_to_vote_accounts: epoch_stakes.node_id_to_vote_accounts,
                epoch_authorized_voters: epoch_stakes.epoch_authorized_voters,
            })
        );
    }

    #[test]
    fn test_split_epoch_stakes_stakes() {
        let mut bank_epoch_stakes = HashMap::new();
        let epoch = 0;
        let test_stakes: Stakes<Stake> = create_test_stakes().into();
        let stakes = Arc::new(StakesEnum::Stakes(test_stakes.clone()));
        let epoch_stakes = EpochStakes {
            stakes,
            total_stake: 100,
            node_id_to_vote_accounts: Arc::new(HashMap::new()),
            epoch_authorized_voters: Arc::new(HashMap::new()),
            bls_pubkey_to_rank_map: Arc::new(BLSPubkeyToRankMap::default()),
        };
        bank_epoch_stakes.insert(epoch, epoch_stakes.clone());

        let (old, versioned) = split_epoch_stakes(bank_epoch_stakes);

        assert!(old.is_empty());
        assert_eq!(versioned.len(), 1);
        assert_eq!(
            versioned.get(&epoch),
            Some(&VersionedEpochStakes::Current {
                stakes: SerdeStakesToStakeFormat::Stake(test_stakes),
                total_stake: epoch_stakes.total_stake,
                node_id_to_vote_accounts: epoch_stakes.node_id_to_vote_accounts,
                epoch_authorized_voters: epoch_stakes.epoch_authorized_voters,
            })
        );
    }

    #[test]
    fn test_split_epoch_stakes_mixed() {
        let mut bank_epoch_stakes = HashMap::new();

        // Delegations
        let epoch1 = 0;
        let stakes1 = Arc::new(StakesEnum::Delegations(Stakes::default()));
        let epoch_stakes1 = EpochStakes {
            stakes: stakes1,
            total_stake: 100,
            node_id_to_vote_accounts: Arc::new(HashMap::new()),
            epoch_authorized_voters: Arc::new(HashMap::new()),
            bls_pubkey_to_rank_map: Arc::new(BLSPubkeyToRankMap::default()),
        };
        bank_epoch_stakes.insert(epoch1, epoch_stakes1);

        // Accounts
        let epoch2 = 1;
        let stakes2 = Arc::new(StakesEnum::Accounts(Stakes::default()));
        let epoch_stakes2 = EpochStakes {
            stakes: stakes2,
            total_stake: 200,
            node_id_to_vote_accounts: Arc::new(HashMap::new()),
            epoch_authorized_voters: Arc::new(HashMap::new()),
            bls_pubkey_to_rank_map: Arc::new(BLSPubkeyToRankMap::default()),
        };
        bank_epoch_stakes.insert(epoch2, epoch_stakes2);

        // Stakes
        let epoch3 = 2;
        let stakes3 = Arc::new(StakesEnum::Stakes(Stakes::default()));
        let epoch_stakes3 = EpochStakes {
            stakes: stakes3,
            total_stake: 300,
            node_id_to_vote_accounts: Arc::new(HashMap::new()),
            epoch_authorized_voters: Arc::new(HashMap::new()),
            bls_pubkey_to_rank_map: Arc::new(BLSPubkeyToRankMap::default()),
        };
        bank_epoch_stakes.insert(epoch3, epoch_stakes3);

        let (old, versioned) = split_epoch_stakes(bank_epoch_stakes);

        assert_eq!(old.len(), 1);
        assert!(old.contains_key(&epoch1));

        assert_eq!(versioned.len(), 2);
        assert_eq!(
            versioned.get(&epoch2),
            Some(&VersionedEpochStakes::Current {
                stakes: SerdeStakesToStakeFormat::Account(Stakes::default()),
                total_stake: 200,
                node_id_to_vote_accounts: Arc::default(),
                epoch_authorized_voters: Arc::default(),
            })
        );
        assert_eq!(
            versioned.get(&epoch3),
            Some(&VersionedEpochStakes::Current {
                stakes: SerdeStakesToStakeFormat::Stake(Stakes::default()),
                total_stake: 300,
                node_id_to_vote_accounts: Arc::default(),
                epoch_authorized_voters: Arc::default(),
            })
        );
    }

    #[test_case(true; "alpenglow")]
    #[test_case(false; "towerbft")]
    fn test_node_id_to_stake(is_alpenglow: bool) {
        let num_nodes = 10;
        let num_vote_accounts_per_node = 2;

        let vote_accounts_map =
            new_vote_accounts(num_nodes, num_vote_accounts_per_node, is_alpenglow);
        let node_id_to_stake_map = vote_accounts_map
            .keys()
            .enumerate()
            .map(|(index, node_id)| (*node_id, ((index + 1) * 100) as u64))
            .collect::<HashMap<_, _>>();
        let epoch_vote_accounts = new_epoch_vote_accounts(&vote_accounts_map, |node_id| {
            *node_id_to_stake_map.get(node_id).unwrap()
        });
        let epoch_stakes = EpochStakes::new_for_tests(epoch_vote_accounts, 0);

        assert_eq!(epoch_stakes.total_stake(), 11000);
        for (node_id, stake) in node_id_to_stake_map.iter() {
            assert_eq!(
                epoch_stakes.node_id_to_stake(node_id),
                Some(*stake * num_vote_accounts_per_node as u64)
            );
        }
    }

    #[test_case(1; "single_vote_account")]
    #[test_case(2; "multiple_vote_accounts")]
    fn test_bls_pubkey_rank_map(num_vote_accounts_per_node: usize) {
        let num_nodes = 10;
        let num_vote_accounts = num_nodes * num_vote_accounts_per_node;

        let vote_accounts_map = new_vote_accounts(num_nodes, num_vote_accounts_per_node, true);
        let node_id_to_stake_map = vote_accounts_map
            .keys()
            .enumerate()
            .map(|(index, node_id)| (*node_id, ((index + 1) * 100) as u64))
            .collect::<HashMap<_, _>>();
        let epoch_vote_accounts = new_epoch_vote_accounts(&vote_accounts_map, |node_id| {
            *node_id_to_stake_map.get(node_id).unwrap()
        });
        let epoch_stakes = EpochStakes::new_for_tests(epoch_vote_accounts.clone(), 0);
        let bls_pubkey_to_rank_map = epoch_stakes.bls_pubkey_to_rank_map();
        assert_eq!(bls_pubkey_to_rank_map.len(), num_vote_accounts);
        for (pubkey, (_, vote_account)) in epoch_vote_accounts {
            let index = bls_pubkey_to_rank_map
                .get_rank(vote_account.bls_pubkey().unwrap())
                .unwrap();
            assert!(index >= &0 && index < &(num_vote_accounts as u16));
            assert_eq!(
                bls_pubkey_to_rank_map.get_pubkey(*index as usize),
                Some(&(pubkey, *vote_account.bls_pubkey().unwrap()))
            );
        }

        // Convert it to versioned and back, we should get the same rank map
        let mut bank_epoch_stakes = HashMap::new();
        bank_epoch_stakes.insert(0, epoch_stakes.clone());
        let (_, versioned_epoch_stakes) = split_epoch_stakes(bank_epoch_stakes);
        let epoch_stakes = EpochStakes::from(versioned_epoch_stakes.get(&0).unwrap().clone());
        let bls_pubkey_to_rank_map2 = epoch_stakes.bls_pubkey_to_rank_map();
        assert_eq!(bls_pubkey_to_rank_map2, bls_pubkey_to_rank_map);
    }
}
