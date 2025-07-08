use {
    super::Stake,
    crate::alpenglow_consensus::vote_certificate::{CertificateError, VoteCertificate},
    solana_pubkey::Pubkey,
    solana_sdk::hash::Hash,
    std::collections::HashMap,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct VoteKey {
    pub(crate) bank_hash: Option<Hash>,
    pub(crate) block_id: Option<Hash>,
}

#[derive(Debug)]
pub(crate) struct VoteEntry<VC: VoteCertificate> {
    pub(crate) transactions: Vec<VC::VoteTransaction>,
    pub(crate) total_stake_by_key: Stake,
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
    pub(crate) votes: HashMap<VoteKey, VoteEntry<VC>>,
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
        bank_hash: Option<Hash>,
        block_id: Option<Hash>,
        transaction: VC::VoteTransaction,
        validator_stake: Stake,
    ) -> bool {
        // Check whether the validator_key already used the same vote_key or exceeded max_entries_per_pubkey
        // If so, return false, otherwise add the vote_key to the prev_votes
        let vote_key = VoteKey {
            bank_hash,
            block_id,
        };
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
        vote_entry.transactions.push(transaction);
        vote_entry.total_stake_by_key += validator_stake;

        if inserted_first_time {
            self.total_stake += validator_stake;
        }
        if vote_entry.total_stake_by_key > self.top_entry_stake {
            self.top_entry_stake = vote_entry.total_stake_by_key;
        }
        true
    }

    pub fn total_stake_by_key(&self, bank_hash: Option<Hash>, block_id: Option<Hash>) -> Stake {
        self.votes
            .get(&VoteKey {
                bank_hash,
                block_id,
            })
            .map_or(0, |vote_entries| vote_entries.total_stake_by_key)
    }

    pub fn total_stake(&self) -> Stake {
        self.total_stake
    }

    pub fn top_entry_stake(&self) -> Stake {
        self.top_entry_stake
    }

    pub fn add_to_certificate(
        &self,
        bank_hash: Option<Hash>,
        block_id: Option<Hash>,
        output: &mut VC,
    ) -> Result<(), CertificateError> {
        if let Some(vote_entries) = self.votes.get(&VoteKey {
            bank_hash,
            block_id,
        }) {
            output.aggregate(vote_entries.transactions.iter())?;
        }
        Ok(())
    }

    pub(crate) fn has_prev_vote(&self, validator_key: &Pubkey, vote_key: Option<&VoteKey>) -> bool {
        if let Some(vote_key) = vote_key {
            self.prev_votes
                .get(validator_key)
                .is_some_and(|vs| vs.contains(vote_key))
        } else {
            self.prev_votes.contains_key(validator_key)
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::{
            super::{
                transaction::AlpenglowVoteTransaction, vote_certificate::LegacyVoteCertificate,
            },
            *,
        },
        alpenglow_vote::{bls_message::CertificateMessage, vote::Vote},
        solana_bls::Signature as BLSSignature,
    };

    #[test]
    fn test_skip_vote_pool() {
        test_skip_vote_pool_for_type::<LegacyVoteCertificate>();
        test_skip_vote_pool_for_type::<CertificateMessage>();
    }

    fn test_skip_vote_pool_for_type<VC: VoteCertificate>() {
        let mut vote_pool = VotePool::<VC>::new(1);
        let vote = Vote::new_skip_vote(5);
        let transaction = VC::VoteTransaction::new_for_test(BLSSignature::default(), vote, 1);
        let my_pubkey = Pubkey::new_unique();

        assert!(vote_pool.add_vote(&my_pubkey, None, None, transaction.clone(), 10));
        assert_eq!(vote_pool.total_stake(), 10);
        assert_eq!(vote_pool.total_stake_by_key(None, None), 10);

        // Adding the same key again should fail
        assert!(!vote_pool.add_vote(&my_pubkey, None, None, transaction.clone(), 10));
        assert_eq!(vote_pool.total_stake(), 10);

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        assert!(vote_pool.add_vote(&new_pubkey, None, None, transaction.clone(), 60),);
        assert_eq!(vote_pool.total_stake(), 70);
        assert_eq!(vote_pool.total_stake_by_key(None, None), 70);
    }

    #[test]
    fn test_notarization_pool() {
        test_notarization_pool_for_type::<LegacyVoteCertificate>();
        test_notarization_pool_for_type::<CertificateMessage>();
    }

    fn test_notarization_pool_for_type<VC: VoteCertificate>() {
        let mut vote_pool = VotePool::<VC>::new(1);
        let my_pubkey = Pubkey::new_unique();
        let block_id = Hash::new_unique();
        let bank_hash = Hash::new_unique();
        let vote = Vote::new_notarization_vote(3, block_id, bank_hash);
        let transaction = VC::VoteTransaction::new_for_test(BLSSignature::default(), vote, 1);

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
    }

    #[test]
    fn test_notarization_fallback_pool() {
        test_notarization_fallback_pool_for_type::<LegacyVoteCertificate>();
        test_notarization_fallback_pool_for_type::<CertificateMessage>();
    }

    fn test_notarization_fallback_pool_for_type<VC: VoteCertificate>() {
        solana_logger::setup();
        let mut vote_pool = VotePool::<VC>::new(3);
        let vote = Vote::new_notarization_fallback_vote(7, Hash::new_unique(), Hash::new_unique());
        let transaction = VC::VoteTransaction::new_for_test(BLSSignature::default(), vote, 1);
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
