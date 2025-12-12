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
