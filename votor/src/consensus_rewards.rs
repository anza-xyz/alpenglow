use {
    entry::Entry,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_votor_messages::{
        consensus_message::VoteMessage,
        rewards_certificate::{NotarRewardCertificate, SkipRewardCertificate},
    },
    std::{collections::BTreeMap, sync::Arc},
};

mod entry;

const NUM_SLOTS_FOR_REWARD: u64 = 8;

pub struct ConsensusRewards {
    /// Per [`Slot`], stores skip and notar votes.
    votes: BTreeMap<Slot, Entry>,
    /// Stores the latest pubkey for the current node.
    cluster_info: Arc<ClusterInfo>,
    /// Stores the leader schedules.
    leader_schedule_cache: Arc<LeaderScheduleCache>,
}

impl ConsensusRewards {
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> Self {
        Self {
            votes: BTreeMap::default(),
            cluster_info,
            leader_schedule_cache,
        }
    }

    /// Returns [`true`] if the rewards container is interested in this vote else [`false`].
    pub fn wants_vote(&self, root_slot: Slot, vote: &VoteMessage) -> bool {
        let vote_slot = vote.vote.slot();
        if vote_slot.saturating_add(NUM_SLOTS_FOR_REWARD) < root_slot {
            return false;
        }
        let my_pubkey = self.cluster_info.id();
        let Some(leader) = self
            .leader_schedule_cache
            .slot_leader_at(vote_slot.saturating_add(NUM_SLOTS_FOR_REWARD), None)
        else {
            return false;
        };
        if leader != my_pubkey {
            return false;
        }
        let Some(entry) = self.votes.get(&vote.vote.slot()) else {
            return true;
        };
        entry.wants_vote(vote)
    }

    /// Adds received [`VoteMessage`]s from other nodes.
    pub fn add_vote(&mut self, root_slot: Slot, max_validators: usize, vote: VoteMessage) {
        // drop old state no longer needed
        self.votes = self.votes.split_off(
            &(root_slot
                .saturating_add(NUM_SLOTS_FOR_REWARD)
                .saturating_add(1)),
        );

        // repeat check to prevent TOCTOU issues
        if !self.wants_vote(root_slot, &vote) {
            return;
        }
        self.votes
            .entry(vote.vote.slot())
            .or_insert(Entry::new(max_validators))
            .add_vote(vote);
    }

    /// Builds [`RewardsCertificates`] from the receives votes.
    pub fn build_rewards_certs(
        &self,
        slot: Slot,
    ) -> (
        Option<SkipRewardCertificate>,
        Option<NotarRewardCertificate>,
    ) {
        match self.votes.get(&slot) {
            None => (None, None),
            Some(entry) => entry.build_certs(slot),
        }
    }
}
