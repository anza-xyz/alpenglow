use {
    crossbeam_channel::{select_biased, Receiver, Sender},
    entry::Entry,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_votor_messages::{
        consensus_message::VoteMessage,
        rewards_certificate::{NotarRewardCertificate, SkipRewardCertificate},
        vote::Vote,
    },
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

mod entry;

/// Number of slots in the past that the the current leader is responsible for producing the reward certificates.
const NUM_SLOTS_FOR_REWARD: u64 = 8;

/// Returns [`false`] if the rewards container is not interested in the [`VoteMessage`].
/// Returns [`true`] if the rewards container might be interested in the [`VoteMessage`].
pub fn wants_vote(
    cluster_info: &ClusterInfo,
    leader_schedule: &LeaderScheduleCache,
    root_slot: Slot,
    vote: &VoteMessage,
) -> bool {
    match vote.vote {
        Vote::Notarize(_) | Vote::Skip(_) => (),
        Vote::Finalize(_)
        | Vote::NotarizeFallback(_)
        | Vote::SkipFallback(_)
        | Vote::Genesis(_) => return false,
    }
    let vote_slot = vote.vote.slot();
    if vote_slot.saturating_add(NUM_SLOTS_FOR_REWARD) < root_slot {
        return false;
    }
    let my_pubkey = cluster_info.id();
    let Some(leader) =
        leader_schedule.slot_leader_at(vote_slot.saturating_add(NUM_SLOTS_FOR_REWARD), None)
    else {
        return false;
    };
    if leader != my_pubkey {
        return false;
    }
    true
}

/// Container to storing state needed to generate reward certificates.
struct ConsensusRewards {
    /// Per [`Slot`], stores skip and notar votes.
    votes: BTreeMap<Slot, Entry>,
    /// Stores the latest pubkey for the current node.
    cluster_info: Arc<ClusterInfo>,
    /// Stores the leader schedules.
    leader_schedule: Arc<LeaderScheduleCache>,
    /// Flag to indicate when the channel receiving loop should exit.
    exit: Arc<AtomicBool>,
    /// Channel to receive messages to build reward certificates.
    build_reward_certs_receiver: Receiver<BuildRewardCertsRequest>,
    /// Channel send the built reward certificates.
    reward_certs_sender: Sender<BuildRewardCertsResponse>,
    /// Channel to receive verified votes.
    votes_receiver: Receiver<AddVoteMessage>,
}

impl ConsensusRewards {
    /// Constructs a new instance of [`ConsensusRewards`].
    fn new(
        cluster_info: Arc<ClusterInfo>,
        leader_schedule: Arc<LeaderScheduleCache>,
        exit: Arc<AtomicBool>,
        build_reward_certs_receiver: Receiver<BuildRewardCertsRequest>,
        reward_certs_sender: Sender<BuildRewardCertsResponse>,
        votes_receiver: Receiver<AddVoteMessage>,
    ) -> Self {
        Self {
            votes: BTreeMap::default(),
            cluster_info,
            leader_schedule,
            exit,
            build_reward_certs_receiver,
            reward_certs_sender,
            votes_receiver,
        }
    }

    /// Runs a loop receiving and handling messages over different channels.
    fn run(&mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            // bias messages to build certificates as that is on the critical path
            select_biased! {
                recv(self.build_reward_certs_receiver) -> msg => {
                    match msg {
                        Ok(msg) => {
                            let (skip, notar) = self.build_certs(msg.slot);
                            let resp = BuildRewardCertsResponse {
                                skip,
                                notar,
                            };
                            if self.reward_certs_sender.send(resp).is_err() {
                                warn!("cert sender channel is disconnected; exiting.");
                                break;
                            }
                        }
                        Err(_) => {
                            warn!("build reward certs channel is disconnected; exiting.");
                            break;
                        }
                    }
                }
                recv(self.votes_receiver) -> msg => {
                    match msg {
                        Ok(msg) => {
                            for entry in msg.votes {
                                self.add_vote(msg.root_slot, entry.max_validators, &entry.vote);
                            }
                        }
                        Err(_) => {
                            warn!("votes receiver channel is disconnected; exiting.");
                            break;
                        }
                    }
                }
                default(Duration::from_secs(1)) => {
                    continue;
                }
            }
        }
    }

    /// Returns [`true`] if the rewards container is interested in this vote else [`false`].
    fn wants_vote(&self, root_slot: Slot, vote: &VoteMessage) -> bool {
        if !wants_vote(&self.cluster_info, &self.leader_schedule, root_slot, vote) {
            return false;
        }
        let Some(entry) = self.votes.get(&vote.vote.slot()) else {
            return true;
        };
        entry.wants_vote(vote)
    }

    /// Adds received [`VoteMessage`]s from other validators.
    fn add_vote(&mut self, root_slot: Slot, max_validators: usize, vote: &VoteMessage) {
        // drop old state no longer needed
        self.votes = self.votes.split_off(
            &(root_slot
                .saturating_add(NUM_SLOTS_FOR_REWARD)
                .saturating_add(1)),
        );

        if !self.wants_vote(root_slot, vote) {
            return;
        }
        match self
            .votes
            .entry(vote.vote.slot())
            .or_insert(Entry::new(max_validators))
            .add_vote(vote)
        {
            Ok(()) => (),
            Err(e) => {
                warn!("Adding vote {vote:?} failed with {e}");
            }
        }
    }

    /// Builds reward certificates.
    fn build_certs(
        &self,
        slot: Slot,
    ) -> (
        Option<SkipRewardCertificate>,
        Option<NotarRewardCertificate>,
    ) {
        match self.votes.get(&slot) {
            None => (None, None),
            Some(entry) => {
                let skip = entry.build_skip_cert(slot);
                let notar = entry.build_notar_cert(slot);
                let skip = match skip {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Build skip reward cert failed with {e}");
                        None
                    }
                };
                let notar = match notar {
                    Ok(n) => n,
                    Err(e) => {
                        warn!("Build notar reward cert failed with {e}");
                        None
                    }
                };
                (skip, notar)
            }
        }
    }
}

/// Message to add votes to the rewards container.
pub struct AddVoteMessage {
    /// The current root slot.
    pub root_slot: Slot,
    /// List of [`AddVoteEntry`], one per vote.
    pub votes: Vec<AddVoteEntry>,
}

/// Data structure for sending per vote state in the [`AddVoteMessage`].
pub struct AddVoteEntry {
    /// Maximum number of validators in the slot that this vote is for.
    pub max_validators: usize,
    /// The actual vote.
    pub vote: VoteMessage,
}

/// Request to build reward certificates.
pub struct BuildRewardCertsRequest {
    pub slot: Slot,
}

/// Response of building reward certificates.
pub struct BuildRewardCertsResponse {
    /// Skip reward certificate.  None if building failed or no skip votes were registered.
    pub skip: Option<SkipRewardCertificate>,
    /// Notar reward certificate.  None if building failed or no notar votes were registered.
    pub notar: Option<NotarRewardCertificate>,
}

/// Service to run the consensus reward container in a dedicated thread.
pub struct ConsensusRewardsService {
    handle: JoinHandle<()>,
}

impl ConsensusRewardsService {
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        leader_schedule: Arc<LeaderScheduleCache>,
        votes_receiver: Receiver<AddVoteMessage>,
        build_reward_certs_receiver: Receiver<BuildRewardCertsRequest>,
        reward_certs_sender: Sender<BuildRewardCertsResponse>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let handle = Builder::new()
            .name("solConsRew".to_string())
            .spawn(move || {
                ConsensusRewards::new(
                    cluster_info,
                    leader_schedule,
                    exit,
                    build_reward_certs_receiver,
                    reward_certs_sender,
                    votes_receiver,
                )
                .run();
            })
            .unwrap();
        Self { handle }
    }

    pub fn join(self) -> thread::Result<()> {
        self.handle.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::{bounded, unbounded},
        solana_bls_signatures::Keypair as BLSKeypair,
        solana_gossip::contact_info::ContactInfo,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_runtime::{
            bank::Bank,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts, ValidatorVoteKeypairs,
            },
        },
        solana_signer::Signer,
        solana_signer_store::{decode, Decoded},
        solana_streamer::socket::SocketAddrSpace,
    };

    fn validate_bitmap(bitmap: &[u8], num_set: usize, max_len: usize) {
        let bitvec = decode(bitmap, max_len).unwrap();
        match bitvec {
            Decoded::Base2(bitvec) => assert_eq!(bitvec.count_ones(), num_set),
            Decoded::Base3(_, _) => panic!("unexpected variant"),
        }
    }

    fn new_vote(vote: Vote, rank: usize, max_validators: usize) -> AddVoteEntry {
        let serialized = bincode::serialize(&vote).unwrap();
        let keypair = BLSKeypair::new();
        let signature = keypair.sign(&serialized).into();
        let vote = VoteMessage {
            vote,
            signature,
            rank: rank.try_into().unwrap(),
        };
        AddVoteEntry {
            max_validators,
            vote,
        }
    }

    #[test]
    fn validate_service() {
        let keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = Arc::new(ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        ));
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let stakes_vec = (0..validator_keypairs.len())
            .map(|i| 1_000 - i as u64)
            .collect::<Vec<_>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            stakes_vec,
        );
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        let leader_schedule = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
        let exit = Arc::new(AtomicBool::new(false));
        let (votes_sender, votes_receiver) = unbounded();
        let (build_reward_certs_sender, build_reward_certs_receiver) = bounded(1);
        let (reward_certs_sender, reward_certs_receiver) = bounded(1);
        let service = ConsensusRewardsService::new(
            cluster_info,
            leader_schedule,
            votes_receiver,
            build_reward_certs_receiver,
            reward_certs_sender,
            exit.clone(),
        );

        let slot = 123;
        let max_validators = 4095;
        let request = BuildRewardCertsRequest { slot };
        build_reward_certs_sender.send(request).unwrap();
        let response = reward_certs_receiver.recv().unwrap();
        assert!(response.skip.is_none());
        assert!(response.notar.is_none());

        let skip = Vote::new_skip_vote(slot);
        let vote0 = new_vote(skip, 0, max_validators);
        let vote1 = new_vote(skip, 0, max_validators);
        let notar = Vote::new_notarization_vote(slot, Hash::new_unique());
        let vote2 = new_vote(notar, 1, max_validators);
        let request = AddVoteMessage {
            root_slot: slot - 1,
            votes: vec![vote0, vote1, vote2],
        };
        votes_sender.send(request).unwrap();
        let request = BuildRewardCertsRequest { slot };
        build_reward_certs_sender.send(request).unwrap();
        let response = reward_certs_receiver.recv().unwrap();
        let skip_cert = response.skip.unwrap();
        let notar_cert = response.notar.unwrap();
        validate_bitmap(&skip_cert.bitmap, 1, max_validators);
        validate_bitmap(&notar_cert.bitmap, 1, max_validators);

        exit.store(true, Ordering::Relaxed);
        service.join().unwrap();
    }
}
