use {
    crate::consensus_pool::vote_certificate_builder::VoteCertificateBuilder,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_runtime::bank_forks::SharableBanks,
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateMessage, VoteMessage},
        vote::Vote,
    },
    std::{collections::BTreeMap, sync::Arc},
};

/// A container for storing [`VoteMessage`]s to help pay rewards.
pub struct ConsensusRewards {
    /// Per [`Slot`], stores a list of received skip and notarization [`VoteMessage`]s.
    //
    // TODO: use Arc for storing vote messages to reducing cloning.
    votes: BTreeMap<Slot, (Vec<VoteMessage>, Vec<VoteMessage>)>,
    /// Contains information about the latest root bank and root slot.
    //
    // TODO: BLSSigVerifier has access to the root slot, maybe it can send it instead of needing to store this here.
    sharable_banks: SharableBanks,
    /// Stores the latest pubkey for the current node.
    cluster_info: Arc<ClusterInfo>,
    /// Stores the leader schedules.
    leader_schedule_cache: Arc<LeaderScheduleCache>,
}

impl ConsensusRewards {
    /// Creates a new instance of [`ConsensusRewards`].
    pub fn new(
        sharable_banks: SharableBanks,
        cluster_info: Arc<ClusterInfo>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> Self {
        Self {
            votes: BTreeMap::default(),
            sharable_banks,
            cluster_info,
            leader_schedule_cache,
        }
    }

    /// Adds received [`VoteMessage`]s from other nodes.
    //
    // TODO: this will probably be called from BLSSigVerifier when it receives a vote message.
    pub fn add_vote_message(&mut self, vote_message: VoteMessage) {
        let root_slot = self.sharable_banks.root().slot();
        let vote_slot = vote_message.vote.slot();
        if vote_slot + 8 < root_slot {
            return;
        }
        let my_pubkey = self.cluster_info.id();
        if let Some(addr) = self
            .leader_schedule_cache
            .slot_leader_at(vote_slot + 8, None)
        {
            if addr == my_pubkey {
                let (notar_votes, skip_votes) = self.votes.entry(vote_slot).or_default();
                match vote_message.vote {
                    Vote::Notarize(_) => notar_votes.push(vote_message),
                    Vote::Skip(_) => skip_votes.push(vote_message),
                    _ => (),
                }
            }
        }
    }

    /// Retrives notar and skip certificates for paying out rewards.
    ///
    /// Data for all slots <= to the slot being queried is discarded.
    //
    // TODO: fix the various panics below.
    // TODO: this will probably be called somewhere in the block production loop.
    pub fn get_aggregate(&mut self, slot: Slot) -> Vec<CertificateMessage> {
        self.votes = self.votes.split_off(&slot);
        let (notar_votes, skip_votes) = self.votes.remove(&slot).unwrap_or_default();

        let mut ret = vec![];
        if let Some(vote_message) = notar_votes.last() {
            let mut cert_builder = VoteCertificateBuilder::new(Certificate::Notarize(
                slot,
                *vote_message.vote.block_id().unwrap(),
            ));
            cert_builder.aggregate(&notar_votes).unwrap();
            let cert = cert_builder.build().unwrap();
            ret.push(cert);
        }
        if !skip_votes.is_empty() {
            let mut cert_builder = VoteCertificateBuilder::new(Certificate::Skip(slot));
            cert_builder.aggregate(&skip_votes).unwrap();
            let cert = cert_builder.build().unwrap();
            ret.push(cert);
        }
        ret
    }
}
