use {
    lru::LruCache,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol},
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::clock::{Epoch, Slot},
    std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

struct StakedValidatorsCacheEntry {
    /// Sockets associated with the staked validators
    validator_sockets: Vec<SocketAddr>,

    /// The time at which this entry was created
    creation_time: Instant,
}

/// Maintain `SocketAddr`s associated with all staked validators for a particular protocol (e.g.,
/// UDP, QUIC) over number of epochs.
///
/// We employ an LRU cache with capped size, mapping Epoch to cache entries that store the socket
/// information. We also track cache entry times, forcing recalculations of cache entries that are
/// accessed after a specified TTL.
pub struct StakedValidatorsCache {
    /// key: the epoch for which we have cached our stake validators list
    /// value: the cache entry
    cache: LruCache<Epoch, StakedValidatorsCacheEntry>,

    /// Time to live for cache entries
    ttl: Duration,

    /// Bank forks
    bank_forks: Arc<RwLock<BankForks>>,

    /// Protocol
    protocol: Protocol,

    /// Whether to include the running validator's socket address in cache entries
    include_self: bool,
}

impl StakedValidatorsCache {
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        protocol: Protocol,
        ttl: Duration,
        max_cache_size: usize,
        include_self: bool,
    ) -> Self {
        Self {
            cache: LruCache::new(max_cache_size),
            ttl,
            bank_forks,
            protocol,
            include_self,
        }
    }

    #[inline]
    fn cur_epoch(&self, slot: Slot) -> Epoch {
        self.bank_forks
            .read()
            .unwrap()
            .working_bank()
            .epoch_schedule()
            .get_epoch(slot)
    }

    fn refresh_cache_entry(
        &mut self,
        epoch: Epoch,
        cluster_info: &ClusterInfo,
        update_time: Instant,
    ) {
        let banks = {
            let bank_forks = self.bank_forks.read().unwrap();
            [bank_forks.root_bank(), bank_forks.working_bank()]
        };

        let epoch_staked_nodes = banks.iter().find_map(|bank| bank.epoch_staked_nodes(epoch)).unwrap_or_else(|| {
            error!("StakedValidatorsCache::get: unknown Bank::epoch_staked_nodes for epoch: {epoch}");
            Arc::<HashMap<Pubkey, u64>>::default()
        });

        struct Node {
            stake: u64,
            tpu_socket: SocketAddr,
        }

        let mut nodes: Vec<_> = epoch_staked_nodes
            .iter()
            .filter(|(pubkey, stake)| {
                let positive_stake = **stake > 0;
                let not_self = pubkey != &&cluster_info.id();

                positive_stake && (self.include_self || not_self)
            })
            .filter_map(|(pubkey, stake)| {
                cluster_info
                    .lookup_contact_info(pubkey, |node| node.tpu_vote(self.protocol))?
                    .map(|socket_addr| Node {
                        stake: *stake,
                        tpu_socket: socket_addr,
                    })
            })
            .collect();

        nodes.dedup_by_key(|node| node.tpu_socket);
        nodes.sort_unstable_by(|a, b| a.stake.cmp(&b.stake));

        let validator_sockets = nodes.into_iter().map(|node| node.tpu_socket).collect();

        self.cache.push(
            epoch,
            StakedValidatorsCacheEntry {
                validator_sockets,
                creation_time: update_time,
            },
        );
    }

    pub fn get_staked_validators_by_slot(
        &mut self,
        slot: Slot,
        cluster_info: &ClusterInfo,
        access_time: Instant,
    ) -> (&[SocketAddr], bool) {
        self.get_staked_validators_by_epoch(self.cur_epoch(slot), cluster_info, access_time)
    }

    pub fn get_staked_validators_by_epoch(
        &mut self,
        epoch: Epoch,
        cluster_info: &ClusterInfo,
        access_time: Instant,
    ) -> (&[SocketAddr], bool) {
        // For a given epoch, if we either:
        //
        // (1) have a cache entry that has expired
        // (2) have no existing cache entry
        //
        // then update the cache.
        let refresh_cache = self
            .cache
            .get(&epoch)
            .map(|v| access_time > v.creation_time + self.ttl)
            .unwrap_or(true);

        if refresh_cache {
            self.refresh_cache_entry(epoch, cluster_info, access_time);
        }

        (
            // Unwrapping is fine here, since update_cache guarantees that we push a cache entry to
            // self.cache[epoch].
            self.cache
                .get(&epoch)
                .map(|v| &*v.validator_sockets)
                .unwrap(),
            refresh_cache,
        )
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::StakedValidatorsCache,
        solana_gossip::{
            cluster_info::{ClusterInfo, Node},
            contact_info::{ContactInfo, Protocol},
            crds::GossipRoute,
            crds_data::CrdsData,
            crds_value::CrdsValue,
        },
        solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo},
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks, epoch_stakes::EpochStakes},
        solana_sdk::{
            account::AccountSharedData,
            clock::{Clock, Slot},
            genesis_config::GenesisConfig,
            signature::Keypair,
            signer::Signer,
            timing::timestamp,
        },
        solana_streamer::socket::SocketAddrSpace,
        solana_vote::vote_account::{VoteAccount, VoteAccountsHashMap},
        solana_vote_program::vote_state::{VoteInit, VoteState, VoteStateVersions},
        std::{
            collections::HashMap,
            iter::{repeat, repeat_with},
            net::Ipv4Addr,
            sync::{Arc, RwLock},
            time::{Duration, Instant},
        },
        test_case::test_case,
    };

    fn new_rand_vote_account<R: rand::Rng>(
        rng: &mut R,
        node_pubkey: Option<Pubkey>,
    ) -> (AccountSharedData, VoteState) {
        let vote_init = VoteInit {
            node_pubkey: node_pubkey.unwrap_or_else(Pubkey::new_unique),
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
        let vote_state = VoteState::new(&vote_init, &clock);
        let account = AccountSharedData::new_data(
            rng.gen(), // lamports
            &VoteStateVersions::new_current(vote_state.clone()),
            &solana_sdk_ids::vote::id(), // owner
        )
        .unwrap();
        (account, vote_state)
    }

    fn new_rand_vote_accounts<R: rand::Rng>(
        rng: &mut R,
        num_nodes: usize,
        num_zero_stake_nodes: usize,
    ) -> impl Iterator<Item = (Keypair, Keypair, /*stake:*/ u64, VoteAccount)> + '_ {
        let node_keypairs: Vec<_> = repeat_with(Keypair::new).take(num_nodes).collect();

        repeat(0..num_nodes).flatten().map(move |node_ix| {
            let node_keypair = node_keypairs[node_ix].insecure_clone();
            let vote_account_keypair = Keypair::new();

            let (account, _) = new_rand_vote_account(rng, Some(node_keypair.pubkey()));
            let stake = if node_ix < num_zero_stake_nodes {
                0
            } else {
                rng.gen_range(1..997)
            };
            let vote_account = VoteAccount::try_from(account).unwrap();
            (vote_account_keypair, node_keypair, stake, vote_account)
        })
    }

    struct StakedValidatorsCacheHarness {
        bank: Bank,
        cluster_info: ClusterInfo,
    }

    impl StakedValidatorsCacheHarness {
        pub fn new(genesis_config: &GenesisConfig, keypair: Keypair) -> Self {
            let bank = Bank::new_for_tests(genesis_config);

            let cluster_info = ClusterInfo::new(
                Node::new_localhost_with_pubkey(&keypair.pubkey()).info,
                Arc::new(keypair),
                SocketAddrSpace::Unspecified,
            );

            Self { bank, cluster_info }
        }

        pub fn with_vote_accounts(
            mut self,
            slot: Slot,
            node_keypair_map: HashMap<Pubkey, Keypair>,
            vote_accounts: VoteAccountsHashMap,
            protocol: Protocol,
        ) -> Self {
            // Update cluster info
            {
                let node_contact_info =
                    node_keypair_map
                        .keys()
                        .enumerate()
                        .map(|(node_ix, pubkey)| {
                            let mut contact_info = ContactInfo::new(*pubkey, 0_u64, 0_u16);

                            assert!(contact_info
                                .set_tpu_vote(
                                    protocol,
                                    (Ipv4Addr::LOCALHOST, 8005 + node_ix as u16),
                                )
                                .is_ok());

                            contact_info
                        });

                for contact_info in node_contact_info {
                    let node_pubkey = *contact_info.pubkey();

                    let entry = CrdsValue::new(
                        CrdsData::ContactInfo(contact_info),
                        &node_keypair_map[&node_pubkey],
                    );

                    assert_eq!(node_pubkey, entry.label().pubkey());

                    {
                        let mut gossip_crds = self.cluster_info.gossip.crds.write().unwrap();

                        gossip_crds
                            .insert(entry, timestamp(), GossipRoute::LocalMessage)
                            .unwrap();
                    }
                }
            }

            // Update bank
            let epoch_num = self.bank.epoch_schedule().get_epoch(slot);
            let epoch_stakes = EpochStakes::new_for_tests(vote_accounts, epoch_num);

            self.bank.set_epoch_stakes_for_test(epoch_num, epoch_stakes);

            self
        }

        pub fn bank_forks(self) -> (Arc<RwLock<BankForks>>, ClusterInfo) {
            let bank_forks = self.bank.wrap_with_bank_forks_for_tests().1;
            (bank_forks, self.cluster_info)
        }
    }

    /// Create a number of nodes; each node will have one or more vote accounts. Each vote account
    /// will have random stake in [1, 997), with the exception of the first few vote accounts
    /// having exactly 0 stake.
    fn build_epoch_stakes(
        num_nodes: usize,
        num_zero_stake_vote_accounts: usize,
        num_vote_accounts: usize,
    ) -> (HashMap<Pubkey, Keypair>, VoteAccountsHashMap) {
        let mut rng = rand::thread_rng();

        let vote_accounts: Vec<_> =
            new_rand_vote_accounts(&mut rng, num_nodes, num_zero_stake_vote_accounts)
                .take(num_vote_accounts)
                .collect();

        let node_keypair_map: HashMap<Pubkey, Keypair> = vote_accounts
            .iter()
            .map(|(_, node_keypair, _, _)| (node_keypair.pubkey(), node_keypair.insecure_clone()))
            .collect();

        let vahm = vote_accounts
            .into_iter()
            .map(|(vote_keypair, _, stake, vote_account)| {
                (vote_keypair.pubkey(), (stake, vote_account))
            })
            .collect();

        (node_keypair_map, vahm)
    }

    #[test_case(325_000_000_u64, 1_usize, 0_usize, 10_usize, 123_u64, Protocol::UDP)]
    #[test_case(325_000_000_u64, 3_usize, 0_usize, 10_usize, 123_u64, Protocol::QUIC)]
    #[test_case(325_000_000_u64, 10_usize, 2_usize, 10_usize, 123_u64, Protocol::UDP)]
    #[test_case(325_000_000_u64, 10_usize, 10_usize, 10_usize, 123_u64, Protocol::QUIC)]
    #[test_case(325_000_000_u64, 50_usize, 7_usize, 60_usize, 123_u64, Protocol::UDP)]
    fn test_detect_only_staked_nodes_and_refresh_after_ttl(
        slot_num: u64,
        num_nodes: usize,
        num_zero_stake_nodes: usize,
        num_vote_accounts: usize,
        genesis_lamports: u64,
        protocol: Protocol,
    ) {
        // Create our harness
        let (keypair_map, vahm) =
            build_epoch_stakes(num_nodes, num_zero_stake_nodes, num_vote_accounts);

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(genesis_lamports);

        let (bank_forks, cluster_info) =
            StakedValidatorsCacheHarness::new(&genesis_config, Keypair::new())
                .with_vote_accounts(slot_num, keypair_map, vahm, protocol)
                .bank_forks();

        // Create our staked validators cache
        let mut svc =
            StakedValidatorsCache::new(bank_forks, protocol, Duration::from_secs(5), 5, true);

        let now = Instant::now();

        let (sockets, refreshed) = svc.get_staked_validators_by_slot(slot_num, &cluster_info, now);

        assert!(refreshed);
        assert_eq!(num_nodes - num_zero_stake_nodes, sockets.len());
        assert_eq!(1, svc.len());

        // Re-fetch from the cache right before the 5-second deadline
        let (sockets, refreshed) = svc.get_staked_validators_by_slot(
            slot_num,
            &cluster_info,
            now + Duration::from_secs_f64(4.999),
        );

        assert!(!refreshed);
        assert_eq!(num_nodes - num_zero_stake_nodes, sockets.len());
        assert_eq!(1, svc.len());

        // Re-fetch from the cache right at the 5-second deadline - we still shouldn't refresh.
        let (sockets, refreshed) = svc.get_staked_validators_by_slot(
            slot_num,
            &cluster_info,
            now + Duration::from_secs(5),
        );

        assert!(!refreshed);
        assert_eq!(num_nodes - num_zero_stake_nodes, sockets.len());
        assert_eq!(1, svc.len());

        // Re-fetch from the cache right after the 5-second deadline - now we should refresh.
        let (sockets, refreshed) = svc.get_staked_validators_by_slot(
            slot_num,
            &cluster_info,
            now + Duration::from_secs_f64(5.001),
        );

        assert!(refreshed);
        assert_eq!(num_nodes - num_zero_stake_nodes, sockets.len());
        assert_eq!(1, svc.len());

        // Re-fetch from the cache well after the 5-second deadline - we should refresh.
        let (sockets, refreshed) = svc.get_staked_validators_by_slot(
            slot_num,
            &cluster_info,
            now + Duration::from_secs(100),
        );

        assert!(refreshed);
        assert_eq!(num_nodes - num_zero_stake_nodes, sockets.len());
        assert_eq!(1, svc.len());
    }

    #[test]
    fn test_cache_eviction() {
        // Create our harness
        let (keypair_map, vahm) = build_epoch_stakes(50, 7, 60);

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(123);

        let base_slot = 325_000_000_000;
        let (bank_forks, cluster_info) =
            StakedValidatorsCacheHarness::new(&genesis_config, Keypair::new())
                .with_vote_accounts(base_slot, keypair_map, vahm, Protocol::UDP)
                .bank_forks();

        // Create our staked validators cache
        let mut svc =
            StakedValidatorsCache::new(bank_forks, Protocol::UDP, Duration::from_secs(5), 5, true);

        assert_eq!(0, svc.len());
        assert!(svc.is_empty());

        let now = Instant::now();

        // Populate the first five entries; accessing the cache once again shouldn't trigger any
        // refreshes.
        for entry_ix in 1..=5 {
            let (_, refreshed) =
                svc.get_staked_validators_by_slot(entry_ix * base_slot, &cluster_info, now);
            assert!(refreshed);
            assert_eq!(entry_ix as usize, svc.len());

            let (_, refreshed) =
                svc.get_staked_validators_by_slot(entry_ix * base_slot, &cluster_info, now);
            assert!(!refreshed);
            assert_eq!(entry_ix as usize, svc.len());
        }

        // Entry 6 - this shouldn't increase the cache length.
        let (_, refreshed) = svc.get_staked_validators_by_slot(6 * base_slot, &cluster_info, now);
        assert!(refreshed);
        assert_eq!(5, svc.len());

        // Epoch 1 should have been evicted
        assert!(!svc.cache.contains(&svc.cur_epoch(base_slot)));

        // Epochs 2 - 6 should have entries
        for entry_ix in 2..=6 {
            assert!(svc.cache.contains(&svc.cur_epoch(entry_ix * base_slot)));
        }

        // Accessing the cache after TTL should recalculate everything; the size remains 5, since
        // we only ever lazily evict cache entries.
        for entry_ix in 1..=5 {
            let (_, refreshed) = svc.get_staked_validators_by_slot(
                entry_ix * base_slot,
                &cluster_info,
                now + Duration::from_secs(10),
            );
            assert!(refreshed);
            assert_eq!(5, svc.len());
        }
    }

    #[test]
    fn test_only_update_once_per_epoch() {
        let slot_num = 325_000_000_u64;
        let num_nodes = 10_usize;
        let num_zero_stake_nodes = 2_usize;
        let num_vote_accounts = 10_usize;
        let genesis_lamports = 123_u64;
        let protocol = Protocol::UDP;

        // Create our harness
        let (keypair_map, vahm) =
            build_epoch_stakes(num_nodes, num_zero_stake_nodes, num_vote_accounts);

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(genesis_lamports);

        let (bank_forks, cluster_info) =
            StakedValidatorsCacheHarness::new(&genesis_config, Keypair::new())
                .with_vote_accounts(slot_num, keypair_map, vahm, protocol)
                .bank_forks();

        // Create our staked validators cache
        let mut svc =
            StakedValidatorsCache::new(bank_forks, protocol, Duration::from_secs(5), 5, true);

        let now = Instant::now();

        let (_, refreshed) = svc.get_staked_validators_by_slot(slot_num, &cluster_info, now);
        assert!(refreshed);

        let (_, refreshed) = svc.get_staked_validators_by_slot(slot_num, &cluster_info, now);
        assert!(!refreshed);

        let (_, refreshed) = svc.get_staked_validators_by_slot(2 * slot_num, &cluster_info, now);
        assert!(refreshed);
    }

    #[test_case(1_usize, Protocol::UDP)]
    #[test_case(1_usize, Protocol::QUIC)]
    #[test_case(10_usize, Protocol::UDP)]
    #[test_case(10_usize, Protocol::QUIC)]
    fn test_exclude_self_from_cache(num_nodes: usize, protocol: Protocol) {
        let slot_num = 325_000_000_u64;
        let num_vote_accounts = 10_usize;
        let genesis_lamports = 123_u64;

        // Create our harness
        let (keypair_map, vahm) = build_epoch_stakes(num_nodes, 0, num_vote_accounts);

        // Fetch some keypair from the keypair map
        let keypair = keypair_map.values().next().unwrap().insecure_clone();

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(genesis_lamports);

        let (bank_forks, cluster_info) =
            StakedValidatorsCacheHarness::new(&genesis_config, keypair.insecure_clone())
                .with_vote_accounts(slot_num, keypair_map, vahm, protocol)
                .bank_forks();

        let my_tpu_vote_socket_addr = cluster_info
            .lookup_contact_info(&keypair.pubkey(), |node| node.tpu_vote(protocol).unwrap())
            .unwrap();

        // Create our staked validators cache - set include_self to true
        let mut svc = StakedValidatorsCache::new(
            bank_forks.clone(),
            protocol,
            Duration::from_secs(5),
            5,
            true,
        );

        let (sockets, _) =
            svc.get_staked_validators_by_slot(slot_num, &cluster_info, Instant::now());
        assert_eq!(sockets.len(), num_nodes);
        assert!(sockets.contains(&my_tpu_vote_socket_addr));

        // Create our staked validators cache - set include_self to false
        let mut svc = StakedValidatorsCache::new(
            bank_forks.clone(),
            protocol,
            Duration::from_secs(5),
            5,
            false,
        );

        let (sockets, _) =
            svc.get_staked_validators_by_slot(slot_num, &cluster_info, Instant::now());
        assert_eq!(sockets.len(), num_nodes - 1);
        assert!(!sockets.contains(&my_tpu_vote_socket_addr));
    }
}
