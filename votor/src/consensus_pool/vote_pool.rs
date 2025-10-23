use {
    crate::common::Stake,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_votor_messages::{
        consensus_message::VoteMessage,
        vote::{Vote, VoteType},
    },
    std::collections::{btree_map::Entry, BTreeMap},
    thiserror::Error,
};

#[derive(Debug, PartialEq, Eq, Error)]
pub(super) enum AddVoteError {
    #[error("duplicate vote")]
    Duplicate,
    #[error("slashable behavior")]
    Slash,
}

/// Container to store per slot votes.
#[derive(Default)]
struct Votes {
    /// Skip votes are stored in map indexed by validator.
    skip: BTreeMap<Pubkey, VoteMessage>,
    /// Skip fallback votes are stored in map indexed by validator.
    skip_fallback: BTreeMap<Pubkey, VoteMessage>,
    /// Finalize votes are stored in map indexed by validator.
    finalize: BTreeMap<Pubkey, VoteMessage>,
    /// Notar votes are stored in map indexed by validator.
    notar: BTreeMap<Pubkey, VoteMessage>,
    /// A validator can vote notar fallback on upto 3 blocks.
    ///
    /// Per validator, we store a map of which block ids the validator has voted notar fallback on.
    notar_fallback: BTreeMap<Pubkey, BTreeMap<Hash, VoteMessage>>,
}

impl Votes {
    /// Adds votes.
    ///
    /// Checks for different types of slashable behavior and duplicate votes returning appropriate errors.
    fn add_vote(&mut self, voter: Pubkey, vote: VoteMessage) -> Result<(), AddVoteError> {
        match vote.vote {
            Vote::Notarize(notar) => {
                if self.skip.contains_key(&voter) {
                    return Err(AddVoteError::Slash);
                }
                match self.notar.entry(voter) {
                    Entry::Occupied(e) => {
                        if e.get().vote.block_id().unwrap() == &notar.block_id {
                            Err(AddVoteError::Duplicate)
                        } else {
                            Err(AddVoteError::Slash)
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(vote);
                        Ok(())
                    }
                }
            }
            Vote::NotarizeFallback(nf) => {
                if self.finalize.contains_key(&voter) {
                    return Err(AddVoteError::Slash);
                }
                match self.notar_fallback.entry(voter) {
                    Entry::Vacant(e) => {
                        let mut map = BTreeMap::new();
                        map.insert(nf.block_id, vote);
                        e.insert(map);
                        Ok(())
                    }
                    Entry::Occupied(mut e) => {
                        let map = e.get_mut();
                        let len = map.len();
                        match map.entry(nf.block_id) {
                            Entry::Vacant(map_e) => {
                                if len == 3 {
                                    Err(AddVoteError::Slash)
                                } else {
                                    map_e.insert(vote);
                                    Ok(())
                                }
                            }
                            Entry::Occupied(_) => Err(AddVoteError::Duplicate),
                        }
                    }
                }
            }
            Vote::Skip(_) => {
                if self.notar.contains_key(&voter) || self.finalize.contains_key(&voter) {
                    return Err(AddVoteError::Slash);
                }
                match self.skip.entry(voter) {
                    Entry::Occupied(_) => Err(AddVoteError::Duplicate),
                    Entry::Vacant(e) => {
                        e.insert(vote);
                        Ok(())
                    }
                }
            }
            Vote::SkipFallback(_) => {
                if self.finalize.contains_key(&voter) {
                    return Err(AddVoteError::Slash);
                }
                match self.skip_fallback.entry(voter) {
                    Entry::Occupied(_) => Err(AddVoteError::Duplicate),
                    Entry::Vacant(e) => {
                        e.insert(vote);
                        Ok(())
                    }
                }
            }
            Vote::Finalize(_) => {
                if self.skip.contains_key(&voter) || self.skip_fallback.contains_key(&voter) {
                    return Err(AddVoteError::Slash);
                }
                if let Some(map) = self.notar_fallback.get(&voter) {
                    assert!(!map.is_empty());
                    return Err(AddVoteError::Slash);
                }
                match self.finalize.entry(voter) {
                    Entry::Occupied(_) => Err(AddVoteError::Duplicate),
                    Entry::Vacant(e) => {
                        e.insert(vote);
                        Ok(())
                    }
                }
            }
            Vote::Genesis(_) => {
                unimplemented!();
            }
        }
    }

    /// Get votes for the corresponding [`VoteType`] and block id.
    ///
    /// Depending on the type of of vote, block_id is expected to be Some().
    //
    // TODO: figure out how to return an iterator here instead which would require `CertificateBuilder::aggregate()` to accept an iterator.
    fn get_votes(&self, vote_type: VoteType, block_id: Option<&Hash>) -> Vec<VoteMessage> {
        match vote_type {
            VoteType::Finalize => self.finalize.values().cloned().collect(),
            VoteType::Notarize => self
                .notar
                .values()
                .filter(|vote| vote.vote.block_id().unwrap() == block_id.unwrap())
                .cloned()
                .collect(),
            VoteType::NotarizeFallback => self
                .notar_fallback
                .values()
                .filter_map(|map| map.get(block_id.unwrap()))
                .cloned()
                .collect(),
            VoteType::Skip => self.skip.values().cloned().collect(),
            VoteType::SkipFallback => self.skip_fallback.values().cloned().collect(),
            VoteType::Genesis => {
                unimplemented!();
            }
        }
    }
}

/// Container to store the total stakes for different types of votes.
#[derive(Default)]
struct Stakes {
    /// Total stake that has voted skip.
    skip: Stake,
    /// Total stake that has voted skil fallback.
    skip_fallback: Stake,
    /// Total stake that has voted finalize.
    finalize: Stake,
    /// Stake that has voted notar.
    ///
    /// Different validators may vote notar for different blocks, so this tracks stake per block id.
    notar: BTreeMap<Hash, Stake>,
    /// Stake that has voted notar fallback.
    ///
    /// A single validator may vote for upto 3 blocks and different validators can vote for different blocks.
    /// Hence, this tracks stake per block id.
    notar_fallback: BTreeMap<Hash, Stake>,
}

impl Stakes {
    /// Updates the corresponding stake after a vote has been successfully added to the pool.
    ///
    /// Returns the total stake of the corresponding type (and block id in case of notar or notar-fallback) after the update.
    fn add_stake(&mut self, voter_stake: Stake, vote: &Vote) -> Stake {
        match vote {
            Vote::Notarize(notar) => {
                let stake = self.notar.entry(notar.block_id).or_default();
                *stake = (*stake).saturating_add(voter_stake);
                *stake
            }
            Vote::NotarizeFallback(nf) => {
                let stake = self.notar_fallback.entry(nf.block_id).or_default();
                *stake = (*stake).saturating_add(voter_stake);
                *stake
            }
            Vote::Skip(_) => {
                self.skip = self.skip.saturating_add(voter_stake);
                self.skip
            }
            Vote::SkipFallback(_) => {
                self.skip_fallback = self.skip_fallback.saturating_add(voter_stake);
                self.skip_fallback
            }
            Vote::Finalize(_) => {
                self.finalize = self.finalize.saturating_add(voter_stake);
                self.finalize
            }
            Vote::Genesis(_) => {
                unimplemented!();
            }
        }
    }

    /// Get the stake corresponding to the [`VoteType`] and block id.
    ///
    /// Depending on the type of vote_type, block_id is expected to be Some.
    fn get_stake(&self, vote_type: VoteType, block_id: Option<&Hash>) -> Stake {
        match vote_type {
            VoteType::Notarize => *self.notar.get(block_id.unwrap()).unwrap_or(&0),
            VoteType::NotarizeFallback => *self.notar_fallback.get(block_id.unwrap()).unwrap_or(&0),
            VoteType::Skip => self.skip,
            VoteType::SkipFallback => self.skip_fallback,
            VoteType::Finalize => self.finalize,
            VoteType::Genesis => unimplemented!(),
        }
    }
}

/// Container to store per slot votes and associated stake.
#[derive(Default)]
pub(super) struct VotePool {
    /// Stores seen votes.
    votes: Votes,
    /// Stores total stake that voted.
    stakes: Stakes,
}

impl VotePool {
    /// Adds a vote to the pool.
    ///
    /// On success, returns the total stake of the corresponding vote type.
    pub(super) fn add_vote(
        &mut self,
        voter: Pubkey,
        voter_stake: Stake,
        msg: VoteMessage,
    ) -> Result<Stake, AddVoteError> {
        let vote = msg.vote;
        self.votes.add_vote(voter, msg)?;
        Ok(self.stakes.add_stake(voter_stake, &vote))
    }

    /// Returns the [`Stake`] corresponding to the specific [`VoteType`] and block_id.
    pub(super) fn get_stake(&self, vote_type: VoteType, block_id: Option<&Hash>) -> Stake {
        self.stakes.get_stake(vote_type, block_id)
    }

    /// Returns a list of votes corresponding to the specific [`VoteType`] and block id.
    pub(super) fn get_votes(
        &self,
        vote_type: VoteType,
        block_id: Option<&Hash>,
    ) -> Vec<VoteMessage> {
        self.votes.get_votes(vote_type, block_id)
    }
}

#[cfg(test)]
mod test {
    // use {
    //     super::*,
    //     solana_bls_signatures::Signature as BLSSignature,
    //     solana_votor_messages::{consensus_message::VoteMessage, vote::Vote},
    // };

    // #[test]
    // fn test_skip_vote_pool() {
    //     let mut vote_pool = SimpleVotePool::default();
    //     let vote = Vote::new_skip_vote(5);
    //     let vote_message = VoteMessage {
    //         vote,
    //         signature: BLSSignature::default(),
    //         rank: 1,
    //     };
    //     let my_pubkey = Pubkey::new_unique();

    //     assert_eq!(vote_pool.add_vote(my_pubkey, 10, vote_message), Some(10));
    //     assert_eq!(vote_pool.total_stake(), 10);

    //     // Adding the same key again should fail
    //     assert_eq!(vote_pool.add_vote(my_pubkey, 10, vote_message), None);
    //     assert_eq!(vote_pool.total_stake(), 10);

    //     // Adding a different key should succeed
    //     let new_pubkey = Pubkey::new_unique();
    //     assert_eq!(vote_pool.add_vote(new_pubkey, 60, vote_message), Some(70));
    //     assert_eq!(vote_pool.total_stake(), 70);
    // }

    // #[test]
    // fn test_notarization_pool() {
    //     let mut vote_pool = DuplicateBlockVotePool::new(1);
    //     let my_pubkey = Pubkey::new_unique();
    //     let block_id = Hash::new_unique();
    //     let vote = Vote::new_notarization_vote(3, block_id);
    //     let vote = VoteMessage {
    //         vote,
    //         signature: BLSSignature::default(),
    //         rank: 1,
    //     };
    //     assert_eq!(vote_pool.add_vote(my_pubkey, 10, vote), Some(10));
    //     assert_eq!(vote_pool.total_stake_by_block_id(&block_id), 10);

    //     // Adding the same key again should fail
    //     assert_eq!(vote_pool.add_vote(my_pubkey, 10, vote), None);

    //     // Adding a different bankhash should fail
    //     assert_eq!(vote_pool.add_vote(my_pubkey, 10, vote), None);

    //     // Adding a different key should succeed
    //     let new_pubkey = Pubkey::new_unique();
    //     assert_eq!(vote_pool.add_vote(new_pubkey, 60, vote), Some(70));
    //     assert_eq!(vote_pool.total_stake_by_block_id(&block_id), 70);
    // }

    // #[test]
    // fn test_notarization_fallback_pool() {
    //     solana_logger::setup();
    //     let mut vote_pool = DuplicateBlockVotePool::new(3);
    //     let my_pubkey = Pubkey::new_unique();

    //     let votes = (0..4)
    //         .map(|_| {
    //             let vote = Vote::new_notarization_fallback_vote(7, Hash::new_unique());
    //             VoteMessage {
    //                 vote,
    //                 signature: BLSSignature::default(),
    //                 rank: 1,
    //             }
    //         })
    //         .collect::<Vec<_>>();

    //     // Adding the first 3 votes should succeed, but total_stake should remain at 10
    //     for vote in votes.iter().take(3).cloned() {
    //         assert_eq!(vote_pool.add_vote(my_pubkey, 10, vote), Some(10));
    //         assert_eq!(
    //             vote_pool.total_stake_by_block_id(vote.vote.block_id().unwrap()),
    //             10
    //         );
    //     }
    //     // Adding the 4th vote should fail
    //     assert_eq!(vote_pool.add_vote(my_pubkey, 10, votes[3]), None);
    //     assert_eq!(
    //         vote_pool.total_stake_by_block_id(votes[3].vote.block_id().unwrap()),
    //         0
    //     );

    //     // Adding a different key should succeed
    //     let new_pubkey = Pubkey::new_unique();
    //     for vote in votes.iter().skip(1).take(2).cloned() {
    //         assert_eq!(vote_pool.add_vote(new_pubkey, 60, vote), Some(70));
    //         assert_eq!(
    //             vote_pool.total_stake_by_block_id(vote.vote.block_id().unwrap()),
    //             70
    //         );
    //     }

    //     // The new key only added 2 votes, so adding block_ids[3] should succeed
    //     assert_eq!(vote_pool.add_vote(new_pubkey, 60, votes[3]), Some(60));
    //     assert_eq!(
    //         vote_pool.total_stake_by_block_id(votes[3].vote.block_id().unwrap()),
    //         60
    //     );

    //     // Now if adding the same key again, it should fail
    //     assert_eq!(vote_pool.add_vote(new_pubkey, 60, votes[0]), None);
    // }
}
