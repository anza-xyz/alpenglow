use {
    crate::consensus_pool::vote_certificate_builder::VoteCertificateBuilder,
    rewards_entry::RewardsEntry,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateMessage, VoteMessage},
        vote::Vote,
    },
    std::{
        collections::{btree_map::Entry, BTreeMap},
        sync::Arc,
    },
};

mod rewards_entry;

fn should_record(
    cluster_info: &ClusterInfo,
    leader_schedule_cache: &LeaderScheduleCache,
    earliest_slot: Slot,
    slot: Slot,
) -> bool {
    if slot < earliest_slot {
        return false;
    }
    if let Some(addr) = leader_schedule_cache.slot_leader_at(slot + 8, None) {
        if addr == cluster_info.id() {
            return true;
        }
    }
    false
}

/// Storage for tracking per slot certificate builders and vote messages for rewards purposes.
pub struct ConsensusRewards {
    /// We do not expect to see requests to generate certs for slot earlier than this one.
    earliest_slot: Slot,
    /// Actual per slot reward entries.
    rewards_entries: BTreeMap<Slot, RewardsEntry>,
    /// Contains information about which node will be a leader in which window.
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    /// Contains information about this node's current pubkey.
    cluster_info: Arc<ClusterInfo>,
}

impl ConsensusRewards {
    /// Creates a new instance of [`ConsensusRewards`].
    pub(crate) fn new(
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Self {
        Self {
            earliest_slot: 0,
            rewards_entries: BTreeMap::default(),
            leader_schedule_cache,
            cluster_info,
        }
    }

    /// If the node needs this builder i.e. it will be producing rewards for the associated slot and the certificate is relevant for rewards, then clones and stores the builder.
    pub(super) fn maybe_add_builder(
        &mut self,
        certificate: Certificate,
        builder: &VoteCertificateBuilder,
    ) {
        let slot = match certificate {
            Certificate::Notarize(slot, _) => slot,
            Certificate::Skip(slot) => slot,
            _ => return,
        };
        if !should_record(
            &self.cluster_info,
            &self.leader_schedule_cache,
            self.earliest_slot,
            slot,
        ) {
            return;
        }

        match certificate {
            Certificate::Notarize(slot, _) => self.add_notar_builder(slot, builder.clone()),
            Certificate::Skip(slot) => self.add_skip_builder(slot, builder.clone()),
            _ => (),
        }
    }

    /// Add a builder for notar certificates.
    fn add_notar_builder(&mut self, slot: Slot, builder: VoteCertificateBuilder) {
        match self.rewards_entries.entry(slot) {
            Entry::Vacant(map_entry) => {
                map_entry.insert(RewardsEntry::with_notar_builder(builder));
            }
            Entry::Occupied(mut map_entry) => {
                let rewards_entry = map_entry.get_mut();
                rewards_entry.add_notar_builder(builder);
            }
        }
    }

    /// Add a builder for skip certificates.
    fn add_skip_builder(&mut self, slot: Slot, builder: VoteCertificateBuilder) {
        match self.rewards_entries.entry(slot) {
            Entry::Vacant(map_entry) => {
                map_entry.insert(RewardsEntry::with_skip_builder(builder));
            }
            Entry::Occupied(mut map_entry) => {
                let rewards_entry = map_entry.get_mut();
                rewards_entry.add_skip_builder(builder);
            }
        }
    }

    /// Adds vote messages that were not already included in the builders above.
    //
    // TODO: this needs to be called from BLSSigVerifier
    pub fn add_vote_messages(&mut self, msgs: Vec<VoteMessage>) {
        for msg in msgs {
            let vote_slot = msg.vote.slot();
            if !should_record(
                &self.cluster_info,
                &self.leader_schedule_cache,
                self.earliest_slot,
                vote_slot,
            ) {
                continue;
            }
            let rewards_entry = self.rewards_entries.entry(vote_slot).or_default();
            match msg.vote {
                Vote::Notarize(_) => {
                    rewards_entry.add_notar_msg(msg);
                }
                Vote::Skip(_) => {
                    rewards_entry.add_skip_msg(msg);
                }
                _ => (),
            }
        }
    }

    /// Generates certs for rewards purposes.
    //
    // TODO: this needs to be called from the block producer.
    pub fn generate_certs(&mut self, slot: Slot) -> Vec<CertificateMessage> {
        self.earliest_slot = slot;
        self.rewards_entries.split_off(&slot);
        match self.rewards_entries.remove(&slot) {
            Some(r) => r.generate_certs(),
            None => vec![],
        }
    }
}
