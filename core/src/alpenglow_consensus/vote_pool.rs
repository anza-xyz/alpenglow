use {
    super::{vote_certificate::VoteCertificate, Stake},
    solana_pubkey::Pubkey,
    solana_sdk::hash::Hash,
    std::{collections::HashMap, sync::Arc},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct VoteKey {
    pub bankhash: Option<Hash>,
    pub blockid: Option<Hash>,
}

#[derive(Debug)]
struct VoteEntry<VC: VoteCertificate> {
    pub transactions: Vec<Arc<VC::VoteTransaction>>,
    pub total_stake_by_key: Stake,
}

impl<VC: VoteCertificate> VoteEntry<VC> {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
            total_stake_by_key: 0,
        }
    }
}

pub struct VotePool<VC: VoteCertificate> {
    max_entries_per_pubkey: usize,
    votes: HashMap<VoteKey, VoteEntry<VC>>,
    total_stake: Stake,
    prev_votes: HashMap<Pubkey, Vec<VoteKey>>,
    top_entry_stake: Stake,
}

impl<VC: VoteCertificate> VotePool<VC> {
    pub fn new(max_entries_per_pubkey: usize) -> Self {
        Self {
            max_entries_per_pubkey,
            votes: HashMap::new(),
            total_stake: 0,
            prev_votes: HashMap::new(),
            top_entry_stake: 0,
        }
    }

    pub fn add_vote(
        &mut self,
        validator_key: &Pubkey,
        bankhash: Option<Hash>,
        blockid: Option<Hash>,
        transaction: Arc<VC::VoteTransaction>,
        validator_stake: Stake,
    ) -> bool {
        // Check whether the validator_key already used the same vote_key or exceeded max_entries_per_pubkey
        // If so, return false, otherwise add the vote_key to the prev_votes
        let vote_key = VoteKey { bankhash, blockid };
        let prev_vote_keys = self.prev_votes.entry(*validator_key).or_default();
        if prev_vote_keys.contains(&vote_key) {
            return false;
        }
        let inserted_first_time = prev_vote_keys.is_empty();
        if prev_vote_keys.len() >= self.max_entries_per_pubkey {
            return false;
        }
        prev_vote_keys.push(vote_key.clone());

        let vote_entry = self.votes.entry(vote_key).or_insert_with(VoteEntry::new);
        vote_entry.transactions.push(transaction.clone());
        vote_entry.total_stake_by_key += validator_stake;

        if inserted_first_time {
            self.total_stake += validator_stake;
        }
        if vote_entry.total_stake_by_key > self.top_entry_stake {
            self.top_entry_stake = vote_entry.total_stake_by_key;
        }
        true
    }

    pub fn has_prev_vote(&self, validator_key: &Pubkey) -> bool {
        self.prev_votes.contains_key(validator_key)
    }

    pub fn has_same_prev_vote(
        &self,
        validator_key: &Pubkey,
        bankhash: Option<Hash>,
        blockid: Option<Hash>,
    ) -> bool {
        self.prev_votes
            .get(validator_key)
            .is_some_and(|vote_keys| vote_keys.contains(&VoteKey { bankhash, blockid }))
    }

    // This is only used in safe_to_notar, where only 1 vote is allowed per validator in Notarize.
    // So we only need to check if the first vote is the same to make the decision.
    pub fn first_prev_vote_different(
        &self,
        validator_key: &Pubkey,
        bankhash: Option<Hash>,
        blockid: Option<Hash>,
    ) -> bool {
        self.prev_votes
            .get(validator_key)
            .is_some_and(|vote_keys| vote_keys[0] != VoteKey { bankhash, blockid })
    }

    pub fn total_stake_by_key(&self, bankhash: Option<Hash>, blockid: Option<Hash>) -> Stake {
        self.votes
            .get(&VoteKey { bankhash, blockid })
            .map_or(0, |vote_entries| vote_entries.total_stake_by_key)
    }

    pub fn total_stake(&self) -> Stake {
        self.total_stake
    }

    pub fn top_entry_stake(&self) -> Stake {
        self.top_entry_stake
    }

    pub fn copy_out_transactions(
        &self,
        bankhash: Option<Hash>,
        blockid: Option<Hash>,
        output: &mut Vec<Arc<VC::VoteTransaction>>,
    ) {
        if let Some(vote_entries) = self.votes.get(&VoteKey { bankhash, blockid }) {
            output.extend(vote_entries.transactions.iter().cloned());
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::{super::vote_certificate::LegacyVoteCertificate, *},
        solana_sdk::transaction::VersionedTransaction,
        std::sync::Arc,
    };

    #[test]
    fn test_skip_vote_pool() {
        let mut vote_pool = VotePool::<LegacyVoteCertificate>::new(1);
        let transaction = Arc::new(VersionedTransaction::default());
        let my_pubkey = Pubkey::new_unique();

        assert!(vote_pool.add_vote(&my_pubkey, None, None, transaction.clone(), 10));
        assert_eq!(vote_pool.total_stake(), 10);
        assert_eq!(vote_pool.total_stake_by_key(None, None), 10);
        assert!(vote_pool.has_prev_vote(&my_pubkey));
        assert!(vote_pool.has_same_prev_vote(&my_pubkey, None, None));
        assert!(!vote_pool.first_prev_vote_different(&my_pubkey, None, None));

        // Adding the same key again should fail
        assert!(!vote_pool.add_vote(&my_pubkey, None, None, transaction.clone(), 10));
        assert_eq!(vote_pool.total_stake(), 10);

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        assert!(vote_pool.add_vote(&new_pubkey, None, None, transaction.clone(), 60),);
        assert_eq!(vote_pool.total_stake(), 70);
        assert_eq!(vote_pool.total_stake_by_key(None, None), 70);
        assert!(vote_pool.has_prev_vote(&new_pubkey));
        assert!(vote_pool.has_same_prev_vote(&new_pubkey, None, None));
        assert!(!vote_pool.first_prev_vote_different(&new_pubkey, None, None));
    }

    #[test]
    fn test_notarization_ppool() {
        let mut vote_pool = VotePool::<LegacyVoteCertificate>::new(1);
        let transaction = Arc::new(VersionedTransaction::default());
        let my_pubkey = Pubkey::new_unique();
        let block_id = Hash::new_unique();
        let bank_hash = Hash::new_unique();

        assert!(vote_pool.add_vote(
            &my_pubkey,
            Some(bank_hash),
            Some(block_id),
            transaction.clone(),
            10
        ));
        assert_eq!(vote_pool.total_stake(), 10);
        assert_eq!(
            vote_pool.total_stake_by_key(Some(bank_hash), Some(block_id)),
            10
        );
        assert!(vote_pool.has_prev_vote(&my_pubkey));
        assert!(vote_pool.has_same_prev_vote(&my_pubkey, Some(bank_hash), Some(block_id)));
        assert!(!vote_pool.first_prev_vote_different(&my_pubkey, Some(bank_hash), Some(block_id)));

        // Adding the same key again should fail
        assert!(!vote_pool.add_vote(
            &my_pubkey,
            Some(bank_hash),
            Some(block_id),
            transaction.clone(),
            10
        ));
        assert_eq!(vote_pool.total_stake(), 10);

        // Adding a different bankhash should fail
        assert!(!vote_pool.add_vote(
            &my_pubkey,
            Some(Hash::new_unique()),
            Some(block_id),
            transaction.clone(),
            10
        ));
        assert_eq!(vote_pool.total_stake(), 10);
        assert!(vote_pool.has_prev_vote(&my_pubkey));
        assert!(vote_pool.has_same_prev_vote(&my_pubkey, Some(bank_hash), Some(block_id)));
        assert!(!vote_pool.first_prev_vote_different(&my_pubkey, Some(bank_hash), Some(block_id)));

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        assert!(vote_pool.add_vote(
            &new_pubkey,
            Some(bank_hash),
            Some(block_id),
            transaction.clone(),
            60
        ),);
        assert_eq!(vote_pool.total_stake(), 70);
        assert_eq!(
            vote_pool.total_stake_by_key(Some(bank_hash), Some(block_id)),
            70
        );
        assert!(vote_pool.has_prev_vote(&new_pubkey));
        assert!(vote_pool.has_same_prev_vote(&new_pubkey, Some(bank_hash), Some(block_id)));
        assert!(!vote_pool.first_prev_vote_different(&new_pubkey, Some(bank_hash), Some(block_id)));
    }

    #[test]
    fn test_notarization_fallback_pool() {
        solana_logger::setup();
        let mut vote_pool = VotePool::<LegacyVoteCertificate>::new(3);
        let transaction = Arc::new(VersionedTransaction::default());
        let my_pubkey = Pubkey::new_unique();

        let block_ids: Vec<Hash> = (0..4).map(|_| Hash::new_unique()).collect();
        let bank_hashes: Vec<Hash> = (0..4).map(|_| Hash::new_unique()).collect();

        // Adding the first 3 votes should succeed, but total_stake should remain at 10
        for i in 0..3 {
            assert!(vote_pool.add_vote(
                &my_pubkey,
                Some(bank_hashes[i]),
                Some(block_ids[i]),
                transaction.clone(),
                10
            ));
            assert_eq!(vote_pool.total_stake(), 10);
            assert_eq!(
                vote_pool.total_stake_by_key(Some(bank_hashes[i]), Some(block_ids[i])),
                10
            );
            assert!(vote_pool.has_prev_vote(&my_pubkey));
            assert!(vote_pool.has_same_prev_vote(
                &my_pubkey,
                Some(bank_hashes[i]),
                Some(block_ids[i])
            ));
            warn!(
                "{} {}",
                i,
                vote_pool.first_prev_vote_different(
                    &my_pubkey,
                    Some(bank_hashes[i]),
                    Some(block_ids[i])
                )
            );
            if i == 0 {
                assert!(!vote_pool.first_prev_vote_different(
                    &my_pubkey,
                    Some(bank_hashes[i]),
                    Some(block_ids[i])
                ));
            } else {
                assert!(vote_pool.first_prev_vote_different(
                    &my_pubkey,
                    Some(bank_hashes[i]),
                    Some(block_ids[i])
                ));
            }
        }
        // Adding the 4th vote should fail
        assert!(!vote_pool.add_vote(
            &my_pubkey,
            Some(bank_hashes[3]),
            Some(block_ids[3]),
            transaction.clone(),
            10
        ));
        assert_eq!(vote_pool.total_stake(), 10);
        assert_eq!(
            vote_pool.total_stake_by_key(Some(bank_hashes[3]), Some(block_ids[3])),
            0
        );

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        for i in 1..3 {
            assert!(vote_pool.add_vote(
                &new_pubkey,
                Some(bank_hashes[i]),
                Some(block_ids[i]),
                transaction.clone(),
                60
            ));
            assert_eq!(vote_pool.total_stake(), 70);
            assert_eq!(
                vote_pool.total_stake_by_key(Some(bank_hashes[i]), Some(block_ids[i])),
                70
            );
            assert!(vote_pool.has_prev_vote(&new_pubkey));
            assert!(vote_pool.has_same_prev_vote(
                &new_pubkey,
                Some(bank_hashes[i]),
                Some(block_ids[i])
            ));
            if i == 1 {
                assert!(!vote_pool.first_prev_vote_different(
                    &new_pubkey,
                    Some(bank_hashes[i]),
                    Some(block_ids[i])
                ));
            } else {
                assert!(vote_pool.first_prev_vote_different(
                    &new_pubkey,
                    Some(bank_hashes[i]),
                    Some(block_ids[i])
                ));
            }
        }

        // The new key only added 2 votes, so adding bank_hashes[3] should succeed
        assert!(vote_pool.add_vote(
            &new_pubkey,
            Some(bank_hashes[3]),
            Some(block_ids[3]),
            transaction.clone(),
            60
        ));
        assert_eq!(vote_pool.total_stake(), 70);
        assert_eq!(
            vote_pool.total_stake_by_key(Some(bank_hashes[3]), Some(block_ids[3])),
            60
        );
        assert!(vote_pool.has_prev_vote(&new_pubkey));
        assert!(vote_pool.has_same_prev_vote(
            &new_pubkey,
            Some(bank_hashes[3]),
            Some(block_ids[3])
        ));
        assert!(vote_pool.first_prev_vote_different(
            &new_pubkey,
            Some(bank_hashes[3]),
            Some(block_ids[3])
        ));

        // Now if adding the same key again, it should fail
        assert!(!vote_pool.add_vote(
            &new_pubkey,
            Some(bank_hashes[0]),
            Some(block_ids[0]),
            transaction.clone(),
            60
        ));
    }
}
