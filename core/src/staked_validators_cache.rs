use {
    solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol},
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::clock::{Epoch, Slot},
    std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, RwLock},
    },
};

/// Maintain `SocketAddr`s associated with all staked validators for a particular epoch and
/// protocol (e.g., UDP, QUIC).
pub struct StakedValidatorsCache {
    /// The epoch for which we have cached our stake validators list
    cached_epoch: Epoch,

    /// The staked validators list for `cached_epoch`
    staked_validator_tpu_sockets: Vec<SocketAddr>,

    /// Bank forks
    bank_forks: Arc<RwLock<BankForks>>,

    /// Protocol
    protocol: Protocol,
}

impl StakedValidatorsCache {
    pub fn new(bank_forks: Arc<RwLock<BankForks>>, protocol: Protocol) -> Self {
        Self {
            cached_epoch: 0_u64,
            staked_validator_tpu_sockets: Vec::default(),
            bank_forks,
            protocol,
        }
    }

    fn refresh(&mut self, slot: Slot, cluster_info: &ClusterInfo) {
        let bank_forks = self.bank_forks.read().unwrap();
        let epoch = self.cached_epoch;

        let epoch_staked_nodes = [bank_forks.root_bank(), bank_forks.working_bank()].iter().find_map(|bank| bank.epoch_staked_nodes(epoch)).unwrap_or_else(|| {
            error!("StakedValidatorsCache::get: unknown Bank::epoch_staked_nodes for epoch: {epoch}, slot: {slot}");
            Arc::<HashMap<Pubkey, u64>>::default()
        });

        struct Node {
            stake: u64,
            tpu_socket: SocketAddr,
        }

        let mut nodes: Vec<_> = epoch_staked_nodes
            .iter()
            .filter(|(_, stake)| **stake > 0)
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

        self.staked_validator_tpu_sockets = nodes.into_iter().map(|node| node.tpu_socket).collect();
    }

    pub fn update(&mut self, slot: Slot, cluster_info: &ClusterInfo) -> bool {
        let cur_epoch = self
            .bank_forks
            .read()
            .unwrap()
            .working_bank()
            .epoch_schedule()
            .get_epoch(slot);

        if cur_epoch == self.cached_epoch {
            false
        } else {
            self.cached_epoch = cur_epoch;
            self.refresh(slot, cluster_info);

            true
        }
    }

    pub fn get_staked_validators(&self) -> &[SocketAddr] {
        &self.staked_validator_tpu_sockets
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
            clock::Slot, genesis_config::GenesisConfig, signature::Keypair, signer::Signer,
            timing::timestamp,
        },
        solana_streamer::socket::SocketAddrSpace,
        solana_vote::vote_account::{new_rand_vote_account, VoteAccount, VoteAccountsHashMap},
        std::{
            collections::HashMap,
            iter::{repeat, repeat_with},
            net::Ipv4Addr,
            sync::{Arc, RwLock},
        },
        test_case::test_case,
    };

    fn new_rand_vote_account<R: rand::Rng>(
        rng: &mut R,
        node_pubkey: Option<Pubkey>,
    ) -> (AccountSharedData, VoteState) {
        use {
            solana_clock::Clock,
            solana_vote_interface::state::{VoteInit, VoteStateVersions},
        };

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
        pub fn new(genesis_config: &GenesisConfig) -> Self {
            let bank = Bank::new_for_tests(genesis_config);

            let keypair = Keypair::new();
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
    fn test_detect_only_staked_nodes(
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

        let (bank_forks, cluster_info) = StakedValidatorsCacheHarness::new(&genesis_config)
            .with_vote_accounts(slot_num, keypair_map, vahm, protocol)
            .bank_forks();

        // Create our staked validators cache
        let mut svc = StakedValidatorsCache::new(bank_forks, protocol);
        assert!(svc.update(slot_num, &cluster_info));

        assert_eq!(
            num_nodes - num_zero_stake_nodes,
            svc.get_staked_validators().len()
        );
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

        let (bank_forks, cluster_info) = StakedValidatorsCacheHarness::new(&genesis_config)
            .with_vote_accounts(slot_num, keypair_map, vahm, protocol)
            .bank_forks();

        // Create our staked validators cache
        let mut svc = StakedValidatorsCache::new(bank_forks, protocol);

        assert!(svc.update(slot_num, &cluster_info));
        assert!(!svc.update(slot_num, &cluster_info));
        assert!(svc.update(2 * slot_num, &cluster_info));
    }
}
