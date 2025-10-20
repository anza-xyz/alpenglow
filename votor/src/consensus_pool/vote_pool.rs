use {
    crate::{
        common::{
            Stake, VoteType, MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE,
            MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES,
        },
        consensus_pool::vote_certificate_builder::VoteCertificateBuilder,
    },
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_votor_messages::consensus_message::VoteMessage,
    std::collections::{HashMap, HashSet},
};

#[derive(Debug)]
struct VoteEntry {
    vote_messages: Vec<VoteMessage>,
    total_stake_by_key: Stake,
}

impl VoteEntry {
    fn new() -> Self {
        Self {
            vote_messages: Vec::new(),
            total_stake_by_key: 0,
        }
    }
}

/// There are two types of vote pools:
/// - SimpleVotePool: Tracks all votes of a specfic vote type made by validators for some slot N, but only one vote per block.
/// - DuplicateBlockVotePool: Tracks all votes of a specfic vote type made by validators for some slot N,
///   but allows votes for different blocks by the same validator. Only relevant for VotePool's that are of type
///   Notarization or NotarizationFallback
pub(super) enum VotePool {
    Simple(SimplePool),
    DuplicateBlock(DuplicateBlockPool),
}

impl VotePool {
    pub(super) fn new(vote_type: VoteType) -> Self {
        match vote_type {
            VoteType::NotarizeFallback => Self::DuplicateBlock(DuplicateBlockPool::new(
                MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE,
            )),
            VoteType::Notarize => Self::DuplicateBlock(DuplicateBlockPool::new(
                MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES,
            )),
            _ => Self::Simple(SimplePool::new()),
        }
    }

    pub(super) fn total_stake(&self, block_id: Option<&Hash>) -> Stake {
        match self {
            Self::Simple(pool) => pool.vote_entry.total_stake_by_key,
            Self::DuplicateBlock(pool) => {
                // XXX: the unwrap was expect originally and the msg contains info this function does not have have.
                pool.votes
                    .get(block_id.unwrap())
                    .map_or(0, |vote_entries| vote_entries.total_stake_by_key)
            }
        }
    }

    pub(super) fn has_conflicting_vote(
        &self,
        validator_vote_key: &Pubkey,
        block_id: &Option<Hash>,
    ) -> bool {
        match self {
            Self::Simple(pool) => pool.prev_voted_validators.contains(validator_vote_key),
            Self::DuplicateBlock(pool) => match block_id {
                Some(block_id) => {
                    pool.has_prev_validator_vote_for_block(validator_vote_key, block_id)
                }
                None => pool.prev_voted_block_ids.contains_key(validator_vote_key),
            },
        }
    }

    pub(super) fn add_vote(
        &mut self,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
        vote_message: VoteMessage,
    ) -> Option<Stake> {
        let block_id = vote_message.vote.block_id();
        match self {
            Self::Simple(pool) => pool.add_vote(validator_vote_key, validator_stake, vote_message),
            Self::DuplicateBlock(pool) => pool.add_vote(
                validator_vote_key,
                *block_id.expect("Duplicate block pool expects a block id"),
                vote_message,
                validator_stake,
            ),
        }
    }

    pub(super) fn add_to_certificate(
        &self,
        vote_certificate_builder: &mut VoteCertificateBuilder,
        block_id: Option<&Hash>,
    ) {
        match self {
            Self::Simple(pool) => pool.add_to_certificate(vote_certificate_builder),
            Self::DuplicateBlock(pool) => {
                // XXX: the original had an except with additional variables that we are not including here.
                pool.add_to_certificate(block_id.unwrap(), vote_certificate_builder)
            }
        }
    }
}

pub(super) struct SimplePool {
    /// Tracks all votes of a specfic vote type made by validators for some slot N.
    vote_entry: VoteEntry,
    prev_voted_validators: HashSet<Pubkey>,
}

impl SimplePool {
    fn new() -> Self {
        Self {
            vote_entry: VoteEntry::new(),
            prev_voted_validators: HashSet::new(),
        }
    }

    fn add_vote(
        &mut self,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
        vote_message: VoteMessage,
    ) -> Option<Stake> {
        if self.prev_voted_validators.contains(validator_vote_key) {
            return None;
        }
        self.prev_voted_validators.insert(*validator_vote_key);
        self.vote_entry.vote_messages.push(vote_message);
        self.vote_entry.total_stake_by_key = self
            .vote_entry
            .total_stake_by_key
            .saturating_add(validator_stake);
        Some(self.vote_entry.total_stake_by_key)
    }

    fn add_to_certificate(&self, output: &mut VoteCertificateBuilder) {
        output
            .aggregate(&self.vote_entry.vote_messages)
            .expect("Incoming vote message signatures are assumed to be valid")
    }
}

pub(super) struct DuplicateBlockPool {
    max_entries_per_pubkey: usize,
    votes: HashMap<Hash, VoteEntry>,
    prev_voted_block_ids: HashMap<Pubkey, Vec<Hash>>,
}

impl DuplicateBlockPool {
    fn new(max_entries_per_pubkey: usize) -> Self {
        Self {
            max_entries_per_pubkey,
            votes: HashMap::new(),
            prev_voted_block_ids: HashMap::new(),
        }
    }

    fn add_vote(
        &mut self,
        validator_vote_key: &Pubkey,
        voted_block_id: Hash,
        vote_message: VoteMessage,
        validator_stake: Stake,
    ) -> Option<Stake> {
        // Check whether the validator_vote_key already used the same voted_block_id or exceeded max_entries_per_pubkey
        // If so, return false, otherwise add the voted_block_id to the prev_votes
        let prev_voted_block_ids = self
            .prev_voted_block_ids
            .entry(*validator_vote_key)
            .or_default();
        if prev_voted_block_ids.contains(&voted_block_id) {
            return None;
        }
        if prev_voted_block_ids.len() >= self.max_entries_per_pubkey {
            return None;
        }
        prev_voted_block_ids.push(voted_block_id);

        let vote_entry = self
            .votes
            .entry(voted_block_id)
            .or_insert_with(VoteEntry::new);
        vote_entry.vote_messages.push(vote_message);
        vote_entry.total_stake_by_key = vote_entry
            .total_stake_by_key
            .saturating_add(validator_stake);

        Some(vote_entry.total_stake_by_key)
    }

    fn add_to_certificate(&self, block_id: &Hash, output: &mut VoteCertificateBuilder) {
        if let Some(vote_entries) = self.votes.get(block_id) {
            output
                .aggregate(&vote_entries.vote_messages)
                .expect("Incoming vote message signatures are assumed to be valid")
        }
    }

    fn has_prev_validator_vote_for_block(
        &self,
        validator_vote_key: &Pubkey,
        block_id: &Hash,
    ) -> bool {
        self.prev_voted_block_ids
            .get(validator_vote_key)
            .is_some_and(|vs| vs.contains(block_id))
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_bls_signatures::Signature as BLSSignature,
        solana_votor_messages::{consensus_message::VoteMessage, vote::Vote},
    };

    #[test]
    fn test_skip_vote_pool() {
        let mut pool = VotePool::new(VoteType::Skip);
        let vote = Vote::new_skip_vote(5);
        let vote_message = VoteMessage {
            vote,
            signature: BLSSignature::default(),
            rank: 1,
        };
        let my_pubkey = Pubkey::new_unique();

        assert_eq!(pool.add_vote(&my_pubkey, 10, vote_message), Some(10));
        assert_eq!(pool.total_stake(None), 10);

        // Adding the same key again should fail
        assert_eq!(pool.add_vote(&my_pubkey, 10, vote_message), None);
        assert_eq!(pool.total_stake(None), 10);

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        assert_eq!(pool.add_vote(&new_pubkey, 60, vote_message), Some(70));
        assert_eq!(pool.total_stake(None), 70);
    }

    #[test]
    fn test_notarization_pool() {
        let mut pool = VotePool::DuplicateBlock(DuplicateBlockPool::new(1));
        let my_pubkey = Pubkey::new_unique();
        let block_id = Hash::new_unique();
        let vote = Vote::new_notarization_vote(3, block_id);
        let vote_message = VoteMessage {
            vote,
            signature: BLSSignature::default(),
            rank: 1,
        };
        assert_eq!(pool.add_vote(&my_pubkey, 10, vote_message), Some(10));
        assert_eq!(pool.total_stake(Some(&block_id)), 10);

        // Adding the same key again should fail
        assert_eq!(pool.add_vote(&my_pubkey, 10, vote_message), None);

        // Adding a different bankhash should fail
        assert_eq!(pool.add_vote(&my_pubkey, 10, vote_message), None);

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        assert_eq!(pool.add_vote(&new_pubkey, 60, vote_message), Some(70));
        assert_eq!(pool.total_stake(Some(&block_id)), 70);
    }

    #[test]
    fn test_notarization_fallback_pool() {
        solana_logger::setup();
        let mut pool = VotePool::DuplicateBlock(DuplicateBlockPool::new(3));
        let my_pubkey = Pubkey::new_unique();

        let msgs = (0..4)
            .map(|_| {
                let vote = Vote::new_notarization_fallback_vote(7, Hash::new_unique());
                VoteMessage {
                    vote,
                    signature: BLSSignature::default(),
                    rank: 1,
                }
            })
            .collect::<Vec<_>>();

        // Adding the first 3 votes should succeed, but total_stake should remain at 10
        for msg in msgs.iter().take(3).cloned() {
            assert_eq!(pool.add_vote(&my_pubkey, 10, msg), Some(10));
            assert_eq!(pool.total_stake(msg.vote.block_id()), 10);
        }
        // Adding the 4th vote should fail
        assert_eq!(pool.add_vote(&my_pubkey, 10, msgs[3]), None);
        assert_eq!(pool.total_stake(msgs[3].vote.block_id()), 0);

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        for msg in msgs.iter().skip(1).take(2).cloned() {
            assert_eq!(pool.add_vote(&new_pubkey, 60, msg), Some(70));
            assert_eq!(pool.total_stake(msg.vote.block_id()), 70);
        }

        // The new key only added 2 votes, so adding block_ids[3] should succeed
        assert_eq!(pool.add_vote(&new_pubkey, 60, msgs[3]), Some(60));
        assert_eq!(pool.total_stake(msgs[3].vote.block_id()), 60);

        // Now if adding the same key again, it should fail
        assert_eq!(pool.add_vote(&new_pubkey, 60, msgs[3]), None);
    }
}
