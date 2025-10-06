use {
    crate::vote_state_view::VoteStateView,
    itertools::Itertools,
    serde::{
        de::{MapAccess, Visitor},
        ser::{Serialize, Serializer},
    },
    solana_account::{AccountSharedData, ReadableAccount},
    solana_instruction::error::InstructionError,
    solana_pubkey::Pubkey,
    std::{
        cmp::Ordering,
        collections::{hash_map::Entry, HashMap},
        fmt,
        iter::FromIterator,
        mem,
        sync::{Arc, OnceLock},
    },
    thiserror::Error,
};
#[cfg(feature = "dev-context-only-utils")]
use {
    solana_bls_signatures::{
        keypair::Keypair as BLSKeypair, pubkey::PubkeyCompressed as BLSPubkeyCompressed,
    },
    solana_vote_interface::{
        authorized_voters::AuthorizedVoters,
        state::{VoteStateV4, VoteStateVersions},
    },
};

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Clone, Debug, PartialEq)]
pub struct VoteAccount(Arc<VoteAccountInner>);

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    InstructionError(#[from] InstructionError),
    #[error("Invalid vote account owner: {0}")]
    InvalidOwner(/*owner:*/ Pubkey),
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug)]
struct VoteAccountInner {
    account: AccountSharedData,
    vote_state_view: VoteStateView,
}

pub fn sort_pubkey_and_stake_pair<T>(
    (pubkey_a, _, stake_a): &(&Pubkey, T, u64),
    (pubkey_b, _, stake_b): &(&Pubkey, T, u64),
) -> Ordering {
    // Sort by descending stake, then ascending pubkey
    stake_b.cmp(stake_a).then(pubkey_a.cmp(pubkey_b))
}

pub type VoteAccountsHashMap = HashMap<Pubkey, (/*stake:*/ u64, VoteAccount)>;
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug, Serialize, Deserialize)]
pub struct VoteAccounts {
    #[serde(deserialize_with = "deserialize_accounts_hash_map")]
    vote_accounts: Arc<VoteAccountsHashMap>,
    // Inner Arc is meant to implement copy-on-write semantics.
    #[serde(skip)]
    staked_nodes: OnceLock<
        Arc<
            HashMap<
                Pubkey, // VoteAccount.vote_state.node_pubkey.
                u64,    // Total stake across all vote-accounts.
            >,
        >,
    >,
}

impl VoteAccounts {
    pub fn clone_and_filter_for_alpenglow(
        &self,
        max_vote_accounts: usize,
        minimum_identity_account_balance: u64,
        // The identity_account_balance is indexed with vote account pubkey.
        // The value is the lamport balance of the identity account.
        identity_account_balances: &HashMap<Pubkey, u64>,
    ) -> VoteAccounts {
        if max_vote_accounts == 0 {
            panic!("max_vote_accounts must be > 0");
        }
        let mut entries_to_sort: Vec<(&Pubkey, &VoteAccount, u64)> = self
            .vote_accounts
            .iter()
            .filter_map(|(pubkey, (stake, vote_account))| {
                if vote_account
                    .vote_state_view()
                    .bls_pubkey_compressed()
                    .is_some()
                    && *stake != 0u64
                {
                    let identity_account_balance = identity_account_balances.get(pubkey)?;
                    if *identity_account_balance < minimum_identity_account_balance {
                        return None;
                    }
                    Some((pubkey, vote_account, *stake))
                } else {
                    None
                }
            })
            .collect();
        // Sort by stake descending
        if entries_to_sort.len() > max_vote_accounts {
            entries_to_sort.sort_by(|a, b| b.2.cmp(&a.2));
            entries_to_sort.truncate(max_vote_accounts);
            let floor_stake = entries_to_sort.last().unwrap().2;
            // Per SIMD 357, we remove all vote accounts with stake equal to the last one
            entries_to_sort.retain(|(_pubkey, _vote_account, stake)| *stake > floor_stake);
        }
        let valid_entries: HashMap<Pubkey, (u64, VoteAccount)> = entries_to_sort
            .into_iter()
            .map(|(pubkey, vote_account, stake)| (*pubkey, (stake, vote_account.clone())))
            .collect();
        if valid_entries.is_empty() {
            panic!("no valid alpenglow vote accounts found");
        }
        VoteAccounts {
            vote_accounts: Arc::new(valid_entries),
            staked_nodes: OnceLock::new(),
        }
    }

    pub fn identity_accounts_for_staked_nodes(&self) -> Vec<&Pubkey> {
        self.vote_accounts
            .iter()
            .filter_map(|(_, (stake, vote_account))| {
                if *stake != 0u64 {
                    Some(vote_account.node_pubkey())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}

impl Clone for VoteAccounts {
    fn clone(&self) -> Self {
        Self {
            vote_accounts: Arc::clone(&self.vote_accounts),
            // Reset this so that if the previous bank did compute `staked_nodes`, the new bank
            // won't copy-on-write and keep updating the map if the staked nodes on this bank are
            // never accessed. See [`VoteAccounts::add_stake`] [`VoteAccounts::sub_stake`] and
            // [`VoteAccounts::staked_nodes`].
            staked_nodes: OnceLock::new(),
        }
    }
}

impl VoteAccount {
    pub fn account(&self) -> &AccountSharedData {
        &self.0.account
    }

    pub fn lamports(&self) -> u64 {
        self.0.account.lamports()
    }

    pub fn owner(&self) -> &Pubkey {
        self.0.account.owner()
    }

    pub fn vote_state_view(&self) -> &VoteStateView {
        &self.0.vote_state_view
    }

    /// VoteState.node_pubkey of this vote-account.
    pub fn node_pubkey(&self) -> &Pubkey {
        self.0.vote_state_view.node_pubkey()
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_random() -> VoteAccount {
        use {
            rand::Rng as _,
            solana_clock::Clock,
            solana_vote_interface::state::{VoteInit, VoteStateV3, VoteStateVersions},
        };

        let mut rng = rand::thread_rng();

        let vote_init = VoteInit {
            node_pubkey: Pubkey::new_unique(),
            authorized_voter: Pubkey::new_unique(),
            authorized_withdrawer: Pubkey::new_unique(),
            commission: rng.gen(),
        };
        let clock = Clock {
            slot: rng.gen(),
            epoch_start_timestamp: rng.gen(),
            epoch: rng.gen(),
            leader_schedule_epoch: rng.gen(),
            unix_timestamp: rng.gen(),
        };
        let vote_state = VoteStateV3::new(&vote_init, &clock);
        let account = AccountSharedData::new_data(
            rng.gen(), // lamports
            &VoteStateVersions::new_v3(vote_state.clone()),
            &solana_sdk_ids::vote::id(), // owner
        )
        .unwrap();

        VoteAccount::try_from(account).unwrap()
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_random_alpenglow() -> VoteAccount {
        let bls_pubkey_compressed: BLSPubkeyCompressed =
            BLSKeypair::new().public.try_into().unwrap();
        let bls_pubkey_compressed_buffer = bincode::serialize(&bls_pubkey_compressed).unwrap();
        let vote_state = VoteStateV4 {
            node_pubkey: Pubkey::new_unique(),
            authorized_voters: AuthorizedVoters::new(0, Pubkey::new_unique()),
            authorized_withdrawer: Pubkey::new_unique(),
            bls_pubkey_compressed: Some(bls_pubkey_compressed_buffer.try_into().unwrap()),
            ..VoteStateV4::default()
        };
        let account = AccountSharedData::new_data(
            100, // lamports
            &VoteStateVersions::new_v4(vote_state),
            &solana_sdk_ids::vote::id(), // owner
        )
        .unwrap();

        VoteAccount::try_from(account).unwrap()
    }
}

impl VoteAccounts {
    pub fn len(&self) -> usize {
        self.vote_accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vote_accounts.is_empty()
    }

    pub fn staked_nodes(&self) -> Arc<HashMap</*node_pubkey:*/ Pubkey, /*stake:*/ u64>> {
        self.staked_nodes
            .get_or_init(|| {
                Arc::new(
                    self.vote_accounts
                        .values()
                        .filter(|(stake, _)| *stake != 0u64)
                        .map(|(stake, vote_account)| (*vote_account.node_pubkey(), stake))
                        .into_grouping_map()
                        .aggregate(|acc, _node_pubkey, stake| {
                            Some(acc.unwrap_or_default() + stake)
                        }),
                )
            })
            .clone()
    }

    pub fn get(&self, pubkey: &Pubkey) -> Option<&VoteAccount> {
        let (_stake, vote_account) = self.vote_accounts.get(pubkey)?;
        Some(vote_account)
    }

    pub fn get_delegated_stake(&self, pubkey: &Pubkey) -> u64 {
        self.vote_accounts
            .get(pubkey)
            .map(|(stake, _vote_account)| *stake)
            .unwrap_or_default()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Pubkey, &VoteAccount)> {
        self.vote_accounts
            .iter()
            .map(|(vote_pubkey, (_stake, vote_account))| (vote_pubkey, vote_account))
    }

    pub fn delegated_stakes(&self) -> impl Iterator<Item = (&Pubkey, u64)> {
        self.vote_accounts
            .iter()
            .map(|(vote_pubkey, (stake, _vote_account))| (vote_pubkey, *stake))
    }

    pub fn find_max_by_delegated_stake(&self) -> Option<&VoteAccount> {
        let key = |(_pubkey, (stake, _vote_account)): &(_, &(u64, _))| *stake;
        let (_pubkey, (_stake, vote_account)) = self.vote_accounts.iter().max_by_key(key)?;
        Some(vote_account)
    }

    pub fn insert(
        &mut self,
        pubkey: Pubkey,
        new_vote_account: VoteAccount,
        calculate_stake: impl FnOnce() -> u64,
    ) -> Option<VoteAccount> {
        let vote_accounts = Arc::make_mut(&mut self.vote_accounts);
        match vote_accounts.entry(pubkey) {
            Entry::Occupied(mut entry) => {
                // This is an upsert, we need to update the vote state and move the stake if needed.
                let (stake, old_vote_account) = entry.get_mut();

                if let Some(staked_nodes) = self.staked_nodes.get_mut() {
                    let old_node_pubkey = old_vote_account.node_pubkey();
                    let new_node_pubkey = new_vote_account.node_pubkey();
                    if new_node_pubkey != old_node_pubkey {
                        // The node keys have changed, we move the stake from the old node to the
                        // new one
                        Self::do_sub_node_stake(staked_nodes, *stake, old_node_pubkey);
                        Self::do_add_node_stake(staked_nodes, *stake, *new_node_pubkey);
                    }
                }

                // Update the vote state
                Some(mem::replace(old_vote_account, new_vote_account))
            }
            Entry::Vacant(entry) => {
                // This is a new vote account. We don't know the stake yet, so we need to compute it.
                let (stake, vote_account) = entry.insert((calculate_stake(), new_vote_account));
                if let Some(staked_nodes) = self.staked_nodes.get_mut() {
                    Self::do_add_node_stake(staked_nodes, *stake, *vote_account.node_pubkey());
                }
                None
            }
        }
    }

    pub fn remove(&mut self, pubkey: &Pubkey) -> Option<(u64, VoteAccount)> {
        let vote_accounts = Arc::make_mut(&mut self.vote_accounts);
        let entry = vote_accounts.remove(pubkey);
        if let Some((stake, ref vote_account)) = entry {
            self.sub_node_stake(stake, vote_account);
        }
        entry
    }

    pub fn add_stake(&mut self, pubkey: &Pubkey, delta: u64) {
        let vote_accounts = Arc::make_mut(&mut self.vote_accounts);
        if let Some((stake, vote_account)) = vote_accounts.get_mut(pubkey) {
            *stake += delta;
            let vote_account = vote_account.clone();
            self.add_node_stake(delta, &vote_account);
        }
    }

    pub fn sub_stake(&mut self, pubkey: &Pubkey, delta: u64) {
        let vote_accounts = Arc::make_mut(&mut self.vote_accounts);
        if let Some((stake, vote_account)) = vote_accounts.get_mut(pubkey) {
            *stake = stake
                .checked_sub(delta)
                .expect("subtraction value exceeds account's stake");
            let vote_account = vote_account.clone();
            self.sub_node_stake(delta, &vote_account);
        }
    }

    fn add_node_stake(&mut self, stake: u64, vote_account: &VoteAccount) {
        let Some(staked_nodes) = self.staked_nodes.get_mut() else {
            return;
        };

        VoteAccounts::do_add_node_stake(staked_nodes, stake, *vote_account.node_pubkey());
    }

    fn do_add_node_stake(
        staked_nodes: &mut Arc<HashMap<Pubkey, u64>>,
        stake: u64,
        node_pubkey: Pubkey,
    ) {
        if stake == 0u64 {
            return;
        }

        Arc::make_mut(staked_nodes)
            .entry(node_pubkey)
            .and_modify(|s| *s += stake)
            .or_insert(stake);
    }

    fn sub_node_stake(&mut self, stake: u64, vote_account: &VoteAccount) {
        let Some(staked_nodes) = self.staked_nodes.get_mut() else {
            return;
        };

        VoteAccounts::do_sub_node_stake(staked_nodes, stake, vote_account.node_pubkey());
    }

    fn do_sub_node_stake(
        staked_nodes: &mut Arc<HashMap<Pubkey, u64>>,
        stake: u64,
        node_pubkey: &Pubkey,
    ) {
        if stake == 0u64 {
            return;
        }

        let staked_nodes = Arc::make_mut(staked_nodes);
        let current_stake = staked_nodes
            .get_mut(node_pubkey)
            .expect("this should not happen");
        match (*current_stake).cmp(&stake) {
            Ordering::Less => panic!("subtraction value exceeds node's stake"),
            Ordering::Equal => {
                staked_nodes.remove(node_pubkey);
            }
            Ordering::Greater => *current_stake -= stake,
        }
    }
}

impl Serialize for VoteAccount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.account.serialize(serializer)
    }
}

impl<'a> From<&'a VoteAccount> for AccountSharedData {
    fn from(account: &'a VoteAccount) -> Self {
        account.0.account.clone()
    }
}

impl From<VoteAccount> for AccountSharedData {
    fn from(account: VoteAccount) -> Self {
        account.0.account.clone()
    }
}

impl TryFrom<AccountSharedData> for VoteAccount {
    type Error = Error;
    fn try_from(account: AccountSharedData) -> Result<Self, Self::Error> {
        if !solana_sdk_ids::vote::check_id(account.owner()) {
            return Err(Error::InvalidOwner(*account.owner()));
        }

        Ok(Self(Arc::new(VoteAccountInner {
            vote_state_view: VoteStateView::try_new(account.data_clone())
                .map_err(|_| Error::InstructionError(InstructionError::InvalidAccountData))?,
            account,
        })))
    }
}

impl PartialEq<VoteAccountInner> for VoteAccountInner {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            account,
            vote_state_view: _,
        } = self;
        account == &other.account
    }
}

impl Default for VoteAccounts {
    fn default() -> Self {
        Self {
            vote_accounts: Arc::default(),
            staked_nodes: OnceLock::new(),
        }
    }
}

impl PartialEq<VoteAccounts> for VoteAccounts {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            vote_accounts,
            staked_nodes: _,
        } = self;
        vote_accounts == &other.vote_accounts
    }
}

impl From<Arc<VoteAccountsHashMap>> for VoteAccounts {
    fn from(vote_accounts: Arc<VoteAccountsHashMap>) -> Self {
        Self {
            vote_accounts,
            staked_nodes: OnceLock::new(),
        }
    }
}

impl AsRef<VoteAccountsHashMap> for VoteAccounts {
    fn as_ref(&self) -> &VoteAccountsHashMap {
        &self.vote_accounts
    }
}

impl From<&VoteAccounts> for Arc<VoteAccountsHashMap> {
    fn from(vote_accounts: &VoteAccounts) -> Self {
        Arc::clone(&vote_accounts.vote_accounts)
    }
}

impl FromIterator<(Pubkey, (/*stake:*/ u64, VoteAccount))> for VoteAccounts {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Pubkey, (u64, VoteAccount))>,
    {
        Self::from(Arc::new(HashMap::from_iter(iter)))
    }
}

// This custom deserializer is needed to ensure compatibility at snapshot loading with versions
// before https://github.com/anza-xyz/agave/pull/2659 which would theoretically allow invalid vote
// accounts in VoteAccounts.
//
// In the (near) future we should remove this custom deserializer and make it a hard error when we
// find invalid vote accounts in snapshots.
fn deserialize_accounts_hash_map<'de, D>(
    deserializer: D,
) -> Result<Arc<VoteAccountsHashMap>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct VoteAccountsVisitor;

    impl<'de> Visitor<'de> for VoteAccountsVisitor {
        type Value = Arc<VoteAccountsHashMap>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map of vote accounts")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut accounts = HashMap::new();

            while let Some((pubkey, (stake, account))) =
                access.next_entry::<Pubkey, (u64, AccountSharedData)>()?
            {
                match VoteAccount::try_from(account) {
                    Ok(vote_account) => {
                        accounts.insert(pubkey, (stake, vote_account));
                    }
                    Err(e) => {
                        log::warn!("failed to deserialize vote account: {e}");
                    }
                }
            }

            Ok(Arc::new(accounts))
        }
    }

    deserializer.deserialize_map(VoteAccountsVisitor)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bincode::Options,
        rand::Rng,
        solana_account::WritableAccount,
        solana_bls_signatures::{
            keypair::Keypair as BLSKeypair, pubkey::PubkeyCompressed as BLSPubkeyCompressed,
        },
        solana_clock::Clock,
        solana_pubkey::Pubkey,
        solana_vote_interface::{
            authorized_voters::AuthorizedVoters,
            state::{VoteInit, VoteStateV3, VoteStateV4, VoteStateVersions},
        },
        std::iter::repeat_with,
        test_case::test_case,
    };

    fn new_rand_vote_account<R: Rng>(
        rng: &mut R,
        node_pubkey: Option<Pubkey>,
        is_alpenglow: bool,
    ) -> AccountSharedData {
        let vote_init = VoteInit {
            node_pubkey: node_pubkey.unwrap_or_else(Pubkey::new_unique),
            authorized_voter: Pubkey::new_unique(),
            authorized_withdrawer: Pubkey::new_unique(),
            commission: rng.gen(),
        };
        let vote_state_versions = if is_alpenglow {
            let bls_pubkey_compressed: BLSPubkeyCompressed =
                BLSKeypair::new().public.try_into().unwrap();
            let bls_pubkey_compressed_buffer = bincode::serialize(&bls_pubkey_compressed).unwrap();
            VoteStateVersions::new_v4(VoteStateV4 {
                node_pubkey: vote_init.node_pubkey,
                authorized_voters: AuthorizedVoters::new(0, vote_init.authorized_voter),
                authorized_withdrawer: vote_init.authorized_withdrawer,
                bls_pubkey_compressed: Some(bls_pubkey_compressed_buffer.try_into().unwrap()),
                ..VoteStateV4::default()
            })
        } else {
            let clock = Clock {
                slot: rng.gen(),
                epoch_start_timestamp: rng.gen(),
                epoch: rng.gen(),
                leader_schedule_epoch: rng.gen(),
                unix_timestamp: rng.gen(),
            };
            VoteStateVersions::new_v3(VoteStateV3::new(&vote_init, &clock))
        };
        AccountSharedData::new_data(
            rng.gen(), // lamports
            &vote_state_versions,
            &solana_sdk_ids::vote::id(), // owner
        )
        .unwrap()
    }

    fn new_rand_vote_accounts<R: Rng>(
        rng: &mut R,
        num_nodes: usize,
        is_alpenglow: bool,
    ) -> impl Iterator<Item = (Pubkey, (/*stake:*/ u64, VoteAccount))> + '_ {
        let nodes: Vec<_> = repeat_with(Pubkey::new_unique).take(num_nodes).collect();
        repeat_with(move || {
            let node = nodes[rng.gen_range(0..nodes.len())];
            let account = new_rand_vote_account(rng, Some(node), is_alpenglow);
            let stake = rng.gen_range(0..997);
            let vote_account = VoteAccount::try_from(account).unwrap();
            (Pubkey::new_unique(), (stake, vote_account))
        })
    }

    // The difference between this and `new_rand_vote_accounts` is that this
    // generates fixed number of alpenglow nodes, and all vote accounts
    // generated have non-zero stake.
    // Also we return a default HashMap of identity account balances.
    fn new_staked_vote_accounts<R: Rng>(
        rng: &mut R,
        num_nodes: usize,
        num_alpenglow_nodes: usize,
        stake_per_node: Option<u64>,
    ) -> (VoteAccounts, HashMap<Pubkey, u64>) {
        let mut vote_accounts = VoteAccounts::default();
        let mut identity_balances = HashMap::new();
        for index in 0..num_nodes {
            let is_alpenglow = index < num_alpenglow_nodes;
            let pubkey = Pubkey::new_unique();
            let stake = stake_per_node.unwrap_or_else(|| rng.gen_range(1..997));
            let account = new_rand_vote_account(rng, None, is_alpenglow);
            // Give each identity account a large enough balance so they all pass the minimum vat check.
            identity_balances.insert(pubkey, 10_000_000_000);
            vote_accounts.insert(pubkey, VoteAccount::try_from(account).unwrap(), || stake);
        }
        (vote_accounts, identity_balances)
    }

    fn staked_nodes<'a, I>(vote_accounts: I) -> HashMap<Pubkey, u64>
    where
        I: IntoIterator<Item = &'a (Pubkey, (u64, VoteAccount))>,
    {
        let mut staked_nodes = HashMap::new();
        for (_, (stake, vote_account)) in vote_accounts
            .into_iter()
            .filter(|(_, (stake, _))| *stake != 0)
        {
            staked_nodes
                .entry(*vote_account.node_pubkey())
                .and_modify(|s| *s += *stake)
                .or_insert(*stake);
        }
        staked_nodes
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_vote_account_try_from(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let account = new_rand_vote_account(&mut rng, None, is_alpenglow);
        let lamports = account.lamports();
        let vote_account = VoteAccount::try_from(account.clone()).unwrap();
        assert_eq!(lamports, vote_account.lamports());
        assert_eq!(&account, vote_account.account());
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    #[should_panic(expected = "InvalidOwner")]
    fn test_vote_account_try_from_invalid_owner(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let mut account = new_rand_vote_account(&mut rng, None, is_alpenglow);
        account.set_owner(Pubkey::new_unique());
        VoteAccount::try_from(account).unwrap();
    }

    #[test]
    #[should_panic(expected = "InvalidAccountData")]
    fn test_vote_account_try_from_invalid_account() {
        let mut account = AccountSharedData::default();
        account.set_owner(solana_sdk_ids::vote::id());
        VoteAccount::try_from(account).unwrap();
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_vote_account_serialize(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let account = new_rand_vote_account(&mut rng, None, is_alpenglow);
        let vote_account = VoteAccount::try_from(account.clone()).unwrap();
        // Assert that VoteAccount has the same wire format as Account.
        assert_eq!(
            bincode::serialize(&account).unwrap(),
            bincode::serialize(&vote_account).unwrap()
        );
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_vote_accounts_serialize(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let vote_accounts_hash_map: VoteAccountsHashMap =
            new_rand_vote_accounts(&mut rng, 64, is_alpenglow)
                .take(1024)
                .collect();
        let vote_accounts = VoteAccounts::from(Arc::new(vote_accounts_hash_map.clone()));
        assert!(vote_accounts.staked_nodes().len() > 32);
        assert_eq!(
            bincode::serialize(&vote_accounts).unwrap(),
            bincode::serialize(&vote_accounts_hash_map).unwrap(),
        );
        assert_eq!(
            bincode::options().serialize(&vote_accounts).unwrap(),
            bincode::options()
                .serialize(&vote_accounts_hash_map)
                .unwrap(),
        )
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_vote_accounts_deserialize(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let vote_accounts_hash_map: VoteAccountsHashMap =
            new_rand_vote_accounts(&mut rng, 64, is_alpenglow)
                .take(1024)
                .collect();
        let data = bincode::serialize(&vote_accounts_hash_map).unwrap();
        let vote_accounts: VoteAccounts = bincode::deserialize(&data).unwrap();
        assert!(vote_accounts.staked_nodes().len() > 32);
        assert_eq!(*vote_accounts.vote_accounts, vote_accounts_hash_map);
        let data = bincode::options()
            .serialize(&vote_accounts_hash_map)
            .unwrap();
        let vote_accounts: VoteAccounts = bincode::options().deserialize(&data).unwrap();
        assert_eq!(*vote_accounts.vote_accounts, vote_accounts_hash_map);
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_vote_accounts_deserialize_invalid_account(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        // we'll populate the map with 1 valid and 2 invalid accounts, then ensure that we only get
        // the valid one after deserialiation
        let mut vote_accounts_hash_map = HashMap::<Pubkey, (u64, AccountSharedData)>::new();

        let valid_account = new_rand_vote_account(&mut rng, None, is_alpenglow);
        vote_accounts_hash_map.insert(Pubkey::new_unique(), (0xAA, valid_account.clone()));

        // bad data
        let invalid_account_data =
            AccountSharedData::new_data(42, &vec![0xFF; 42], &solana_sdk_ids::vote::id()).unwrap();
        vote_accounts_hash_map.insert(Pubkey::new_unique(), (0xBB, invalid_account_data));

        // wrong owner
        let invalid_account_key =
            AccountSharedData::new_data(42, &valid_account.data().to_vec(), &Pubkey::new_unique())
                .unwrap();
        vote_accounts_hash_map.insert(Pubkey::new_unique(), (0xCC, invalid_account_key));

        let data = bincode::serialize(&vote_accounts_hash_map).unwrap();
        let options = bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes();
        let mut deserializer = bincode::de::Deserializer::from_slice(&data, options);
        let vote_accounts = deserialize_accounts_hash_map(&mut deserializer).unwrap();

        assert_eq!(vote_accounts.len(), 1);
        let (stake, _account) = vote_accounts.values().next().unwrap();
        assert_eq!(*stake, 0xAA);
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_staked_nodes(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let mut accounts: Vec<_> = new_rand_vote_accounts(&mut rng, 64, is_alpenglow)
            .take(1024)
            .collect();
        let mut vote_accounts = VoteAccounts::default();
        // Add vote accounts.
        for (k, (pubkey, (stake, vote_account))) in accounts.iter().enumerate() {
            vote_accounts.insert(*pubkey, vote_account.clone(), || *stake);
            if (k + 1) % 128 == 0 {
                assert_eq!(
                    staked_nodes(&accounts[..k + 1]),
                    *vote_accounts.staked_nodes()
                );
            }
        }
        // Remove some of the vote accounts.
        for k in 0..256 {
            let index = rng.gen_range(0..accounts.len());
            let (pubkey, (_, _)) = accounts.swap_remove(index);
            vote_accounts.remove(&pubkey);
            if (k + 1) % 32 == 0 {
                assert_eq!(staked_nodes(&accounts), *vote_accounts.staked_nodes());
            }
        }
        // Modify the stakes for some of the accounts.
        for k in 0..2048 {
            let index = rng.gen_range(0..accounts.len());
            let (pubkey, (stake, _)) = &mut accounts[index];
            let new_stake = rng.gen_range(0..997);
            if new_stake < *stake {
                vote_accounts.sub_stake(pubkey, *stake - new_stake);
            } else {
                vote_accounts.add_stake(pubkey, new_stake - *stake);
            }
            *stake = new_stake;
            if (k + 1) % 128 == 0 {
                assert_eq!(staked_nodes(&accounts), *vote_accounts.staked_nodes());
            }
        }
        // Remove everything.
        while !accounts.is_empty() {
            let index = rng.gen_range(0..accounts.len());
            let (pubkey, (_, _)) = accounts.swap_remove(index);
            vote_accounts.remove(&pubkey);
            if accounts.len() % 32 == 0 {
                assert_eq!(staked_nodes(&accounts), *vote_accounts.staked_nodes());
            }
        }
        assert!(vote_accounts.staked_nodes.get().unwrap().is_empty());
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_staked_nodes_update(is_alpenglow: bool) {
        let mut vote_accounts = VoteAccounts::default();

        let mut rng = rand::thread_rng();
        let pubkey = Pubkey::new_unique();
        let node_pubkey = Pubkey::new_unique();
        let account1 = new_rand_vote_account(&mut rng, Some(node_pubkey), is_alpenglow);
        let vote_account1 = VoteAccount::try_from(account1).unwrap();

        // first insert
        let ret = vote_accounts.insert(pubkey, vote_account1.clone(), || 42);
        assert_eq!(ret, None);
        assert_eq!(vote_accounts.get_delegated_stake(&pubkey), 42);
        assert_eq!(vote_accounts.staked_nodes().get(&node_pubkey), Some(&42));

        // update with unchanged state
        let ret = vote_accounts.insert(pubkey, vote_account1.clone(), || {
            panic!("should not be called")
        });
        assert_eq!(ret, Some(vote_account1.clone()));
        assert_eq!(vote_accounts.get(&pubkey), Some(&vote_account1));
        // stake is unchanged
        assert_eq!(vote_accounts.get_delegated_stake(&pubkey), 42);
        assert_eq!(vote_accounts.staked_nodes().get(&node_pubkey), Some(&42));

        // update with changed state, same node pubkey
        let account2 = new_rand_vote_account(&mut rng, Some(node_pubkey), is_alpenglow);
        let vote_account2 = VoteAccount::try_from(account2).unwrap();
        let ret = vote_accounts.insert(pubkey, vote_account2.clone(), || {
            panic!("should not be called")
        });
        assert_eq!(ret, Some(vote_account1.clone()));
        assert_eq!(vote_accounts.get(&pubkey), Some(&vote_account2));
        // stake is unchanged
        assert_eq!(vote_accounts.get_delegated_stake(&pubkey), 42);
        assert_eq!(vote_accounts.staked_nodes().get(&node_pubkey), Some(&42));

        // update with new node pubkey, stake must be moved
        let new_node_pubkey = Pubkey::new_unique();
        let account3 = new_rand_vote_account(&mut rng, Some(new_node_pubkey), is_alpenglow);
        let vote_account3 = VoteAccount::try_from(account3).unwrap();
        let ret = vote_accounts.insert(pubkey, vote_account3.clone(), || {
            panic!("should not be called")
        });
        assert_eq!(ret, Some(vote_account2.clone()));
        assert_eq!(vote_accounts.staked_nodes().get(&node_pubkey), None);
        assert_eq!(
            vote_accounts.staked_nodes().get(&new_node_pubkey),
            Some(&42)
        );
    }

    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_staked_nodes_zero_stake(is_alpenglow: bool) {
        let mut vote_accounts = VoteAccounts::default();

        let mut rng = rand::thread_rng();
        let pubkey = Pubkey::new_unique();
        let node_pubkey = Pubkey::new_unique();
        let account1 = new_rand_vote_account(&mut rng, Some(node_pubkey), is_alpenglow);
        let vote_account1 = VoteAccount::try_from(account1).unwrap();

        // we call this here to initialize VoteAccounts::staked_nodes which is a OnceLock
        assert!(vote_accounts.staked_nodes().is_empty());
        let ret = vote_accounts.insert(pubkey, vote_account1.clone(), || 0);
        assert_eq!(ret, None);
        assert_eq!(vote_accounts.get_delegated_stake(&pubkey), 0);
        // ensure that we didn't add a 0 stake entry to staked_nodes
        assert_eq!(vote_accounts.staked_nodes().get(&node_pubkey), None);

        // update with new node pubkey, stake is 0 and should remain 0
        let new_node_pubkey = Pubkey::new_unique();
        let account2 = new_rand_vote_account(&mut rng, Some(new_node_pubkey), is_alpenglow);
        let vote_account2 = VoteAccount::try_from(account2).unwrap();
        let ret = vote_accounts.insert(pubkey, vote_account2.clone(), || {
            panic!("should not be called")
        });
        assert_eq!(ret, Some(vote_account1));
        assert_eq!(vote_accounts.get_delegated_stake(&pubkey), 0);
        assert_eq!(vote_accounts.staked_nodes().get(&node_pubkey), None);
        assert_eq!(vote_accounts.staked_nodes().get(&new_node_pubkey), None);
    }

    // Asserts that returned staked-nodes are copy-on-write references.
    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_staked_nodes_cow(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let mut accounts = new_rand_vote_accounts(&mut rng, 64, is_alpenglow);
        // Add vote accounts.
        let mut vote_accounts = VoteAccounts::default();
        for (pubkey, (stake, vote_account)) in (&mut accounts).take(1024) {
            vote_accounts.insert(pubkey, vote_account, || stake);
        }
        let staked_nodes = vote_accounts.staked_nodes();
        let (pubkey, (more_stake, vote_account)) =
            accounts.find(|(_, (stake, _))| *stake != 0).unwrap();
        let node_pubkey = *vote_account.node_pubkey();
        vote_accounts.insert(pubkey, vote_account, || more_stake);
        assert_ne!(staked_nodes, vote_accounts.staked_nodes());
        assert_eq!(
            vote_accounts.staked_nodes()[&node_pubkey],
            more_stake + staked_nodes.get(&node_pubkey).copied().unwrap_or_default()
        );
        for (pubkey, stake) in vote_accounts.staked_nodes().iter() {
            if pubkey != &node_pubkey {
                assert_eq!(*stake, staked_nodes[pubkey]);
            } else {
                assert_eq!(
                    *stake,
                    more_stake + staked_nodes.get(pubkey).copied().unwrap_or_default()
                );
            }
        }
    }

    // Asserts that returned vote-accounts are copy-on-write references.
    #[test_case(false ; "tower")]
    #[test_case(true ; "alpenglow")]
    fn test_vote_accounts_cow(is_alpenglow: bool) {
        let mut rng = rand::thread_rng();
        let mut accounts = new_rand_vote_accounts(&mut rng, 64, is_alpenglow);
        // Add vote accounts.
        let mut vote_accounts = VoteAccounts::default();
        for (pubkey, (stake, vote_account)) in (&mut accounts).take(1024) {
            vote_accounts.insert(pubkey, vote_account, || stake);
        }
        let vote_accounts_hashmap = Arc::<VoteAccountsHashMap>::from(&vote_accounts);
        assert_eq!(vote_accounts_hashmap, vote_accounts.vote_accounts);
        assert!(Arc::ptr_eq(
            &vote_accounts_hashmap,
            &vote_accounts.vote_accounts
        ));
        let (pubkey, (more_stake, vote_account)) =
            accounts.find(|(_, (stake, _))| *stake != 0).unwrap();
        vote_accounts.insert(pubkey, vote_account.clone(), || more_stake);
        assert!(!Arc::ptr_eq(
            &vote_accounts_hashmap,
            &vote_accounts.vote_accounts
        ));
        assert_ne!(vote_accounts_hashmap, vote_accounts.vote_accounts);
        let other = (more_stake, vote_account);
        for (pk, value) in vote_accounts.vote_accounts.iter() {
            if *pk != pubkey {
                assert_eq!(value, &vote_accounts_hashmap[pk]);
            } else {
                assert_eq!(value, &other);
            }
        }
    }

    #[test]
    fn test_clone_and_filter_for_alpenglow_truncates() {
        let mut rng = rand::thread_rng();
        let current_limit = 3000;
        let (vote_accounts, identity_balances) =
            new_staked_vote_accounts(&mut rng, current_limit, current_limit, None);
        // All vote accounts should be returned if the limit is high enough.
        let filtered = vote_accounts.clone_and_filter_for_alpenglow(
            current_limit + 500,
            1,
            &identity_balances,
        );
        assert_eq!(filtered.len(), vote_accounts.len());

        // If the limit is smaller than number of accounts, truncate it.
        let lower_limit = current_limit - 1000;
        let filtered =
            vote_accounts.clone_and_filter_for_alpenglow(lower_limit, 1, &identity_balances);
        assert!(filtered.len() <= lower_limit);
        // Check that the filtered accounts are the same as the original accounts.
        for (pubkey, (_, vote_account)) in filtered.vote_accounts.iter() {
            assert_eq!(vote_accounts.get(pubkey), Some(vote_account));
        }
        // Check that the stake in any filtered account is higher than truncated accounts.
        let min_stake = filtered
            .vote_accounts
            .iter()
            .map(|(_, (stake, _))| *stake)
            .min()
            .unwrap();
        for (pubkey, (stake, _vote_account)) in vote_accounts.vote_accounts.iter() {
            if *stake < min_stake {
                assert!(filtered.get(pubkey).is_none());
            }
        }
    }

    #[test]
    fn test_clone_and_filter_for_alpenglow_filters_non_alpenglow() {
        let mut rng = rand::thread_rng();
        let num_alpenglow_nodes = 2000;
        // Check that non-alpenglow accounts are kicked out, 2000 accounts with bls pubkey, 1000 accounts without.
        let num_nodes = num_alpenglow_nodes + 1000;
        let (vote_accounts, identity_balances) =
            new_staked_vote_accounts(&mut rng, num_nodes, num_alpenglow_nodes, None);
        let new_limit = num_alpenglow_nodes + 500;
        let filtered =
            vote_accounts.clone_and_filter_for_alpenglow(new_limit, 1, &identity_balances);
        assert!(filtered.len() <= num_alpenglow_nodes);
        // Check that all filtered accounts have bls pubkey
        for (_pubkey, (_stake, vote_account)) in filtered.vote_accounts.iter() {
            assert!(vote_account
                .vote_state_view()
                .bls_pubkey_compressed()
                .is_some());
        }
        // Now get only 1500 accounts, even some alpenglow accounts are kicked out
        let new_limit = num_alpenglow_nodes - 500;
        let filtered =
            vote_accounts.clone_and_filter_for_alpenglow(new_limit, 1, &identity_balances);
        assert!(filtered.len() <= new_limit);
        for (_pubkey, (_stake, vote_account)) in filtered.vote_accounts.iter() {
            assert!(vote_account
                .vote_state_view()
                .bls_pubkey_compressed()
                .is_some());
        }
    }

    #[test]
    fn test_clone_and_filter_for_alpenglow_same_stake_at_border() {
        let mut rng = rand::thread_rng();
        let num_alpenglow_nodes = 2000;
        // Create exactly num_alpenglow_nodes + 2 accounts with the same stake
        let num_accounts = num_alpenglow_nodes + 2;
        let mut identity_balances = HashMap::new();
        let accounts = (0..num_accounts).map(|index| {
            let account = new_rand_vote_account(&mut rng, None, true);
            let vote_account = VoteAccount::try_from(account).unwrap();
            let pubkey = Pubkey::new_unique();
            identity_balances.insert(pubkey, 10_000_000_000);
            let stake = if index < num_alpenglow_nodes - 10 {
                100 + index as u64
            } else {
                10 // same stake for the last 12 accounts
            };
            (pubkey, (stake, vote_account))
        });
        let mut vote_accounts = VoteAccounts::default();
        for (pubkey, (stake, vote_account)) in accounts {
            vote_accounts.insert(pubkey, vote_account, || stake);
        }
        let filtered =
            vote_accounts.clone_and_filter_for_alpenglow(num_accounts, 1, &identity_balances);
        assert_eq!(filtered.len(), num_accounts);
        let filtered = vote_accounts.clone_and_filter_for_alpenglow(
            num_alpenglow_nodes,
            1,
            &identity_balances,
        );
        assert_eq!(filtered.len(), num_alpenglow_nodes - 10);
    }

    #[test]
    fn test_clone_and_filter_for_alpenglow_not_enough_lamports_in_identity() {
        let mut rng = rand::thread_rng();
        let num_alpenglow_nodes = 2000;
        let minimum_identity_balance = 1_600_000_000;
        let (vote_accounts, mut identity_balances) =
            new_staked_vote_accounts(&mut rng, num_alpenglow_nodes, num_alpenglow_nodes, None);
        // for 10% in identity_balances, set the balance below the minimum
        let entries_to_modify = num_alpenglow_nodes / 10;
        let pubkeys_to_modify = identity_balances
            .keys()
            .take(entries_to_modify)
            .copied()
            .collect::<Vec<Pubkey>>();
        for pubkey in pubkeys_to_modify {
            identity_balances.insert(pubkey, minimum_identity_balance - 1);
        }
        let filtered = vote_accounts.clone_and_filter_for_alpenglow(
            num_alpenglow_nodes,
            minimum_identity_balance,
            &identity_balances,
        );
        assert!(filtered.len() <= num_alpenglow_nodes - entries_to_modify);
    }

    #[test]
    #[should_panic(expected = "no valid alpenglow vote accounts found")]
    fn test_clone_and_filter_for_alpenglow_panic_on_empty_accounts() {
        let mut rng = rand::thread_rng();
        let current_limit = 3000;
        let (vote_accounts, identity_balances) = new_staked_vote_accounts(
            &mut rng,
            current_limit,
            current_limit,
            Some(100), // Set all vote accounts to equal stake of 100
        );
        // Since everyone has same stake, and we set the limit to 500 less than the number of accounts,
        // we will end up with no accounts after filtering, because all accounts with the same stake
        // at the border will be removed. This should cause a panic.
        let _ = vote_accounts.clone_and_filter_for_alpenglow(
            current_limit - 500,
            1,
            &identity_balances,
        );
    }
}
