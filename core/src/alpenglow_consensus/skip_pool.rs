use {
    super::{Stake, SUPERMAJORITY},
    solana_pubkey::Pubkey,
    solana_sdk::{clock::Slot, transaction::VersionedTransaction},
    std::{
        collections::{BTreeMap, HashMap},
        fmt::Debug,
        ops::RangeInclusive,
    },
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    #[error("Skip vote {0:?} already exists")]
    AlreadyExists(RangeInclusive<Slot>),

    #[error("Newer skip vote {0:?} than {1:?} already exists for this pubkey")]
    TooOld(RangeInclusive<Slot>, RangeInclusive<Slot>),

    #[error("Overlapping skip vote old {0:?} and new {1:?}")]
    Overlapping(RangeInclusive<Slot>, RangeInclusive<Slot>),
}

/// A trait for objects that provide a stake value.
pub trait HasStake {
    fn stake_value(&self) -> Stake;
    fn pubkey(&self) -> Pubkey;
}

/// Implement `HasStake` for `(Pubkey, Stake)`
impl HasStake for (Pubkey, Stake) {
    fn stake_value(&self) -> Stake {
        self.1
    }

    fn pubkey(&self) -> Pubkey {
        self.0
    }
}

/// Dynamic Segment Tree that works with any type implementing `HasStake`
struct DynamicSegmentTree<T: Ord + Clone + HasStake> {
    tree: BTreeMap<Slot, Vec<T>>, // Single Vec<T> per slot (first occurrence = add, second = remove)
}

impl<T: Ord + Clone + Debug + HasStake> DynamicSegmentTree<T> {
    /// Initializes an empty dynamic segment tree
    fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
        }
    }

    /// Inserts a given range `[start, end]` with an item `value`
    fn insert(&mut self, start: Slot, end: Slot, new_value: T) {
        self.tree.entry(start).or_default().push(new_value.clone());
        self.tree.entry(end).or_default().push(new_value);
    }

    /// Removes a given range `[start, end]` with an item `value`
    fn remove(&mut self, start: Slot, end: Slot, new_value: T) {
        if let Some(events) = self.tree.get_mut(&start) {
            events.retain(|v| v.pubkey() != new_value.pubkey());
        }
        if let Some(events) = self.tree.get_mut(&end) {
            events.retain(|v| v.pubkey() != new_value.pubkey());
        }
    }

    /// Queries the first slot range where the accumulated stake exceeds `threshold`
    /// Returns the range and contributing Pubkeys
    fn query(&self, threshold_stake: f64) -> Option<(RangeInclusive<Slot>, Vec<T>)> {
        let mut accumulated = 0f64;
        let mut start = None;
        let mut contributing_items: Vec<T> = Vec::new();
        let mut active_contributors: HashMap<Pubkey, Stake> = HashMap::new();

        for (&slot, events) in &self.tree {
            for item in events {
                let pubkey = item.pubkey();
                let stake = item.stake_value();

                if let std::collections::hash_map::Entry::Vacant(e) =
                    active_contributors.entry(pubkey)
                {
                    // First occurrence, adding stake
                    accumulated += stake as f64;
                    contributing_items.push(item.clone());
                    e.insert(stake);
                } else {
                    // If the pubkey is already in active_contributors, it's the end of the range
                    accumulated -= active_contributors.remove(&pubkey).unwrap() as f64;
                }
                if accumulated >= threshold_stake {
                    if start.is_none() {
                        start = Some(slot as Slot);
                    }
                } else if start.is_some() {
                    return Some((start.unwrap()..=slot as Slot, contributing_items));
                }
            }
        }
        None
    }
}

/// Structure to store a skip vote, including the range and transaction
pub struct SkipVote {
    skip_range: RangeInclusive<Slot>,
    skip_transaction: VersionedTransaction,
}

/// `SkipPool` tracks validator skip votes and aggregates stake using a dynamic segment tree.
pub struct SkipPool {
    skips: HashMap<Pubkey, SkipVote>, // Stores latest skip range for each validator
    segment_tree: DynamicSegmentTree<(Pubkey, Stake)>, // Generic tree tracking validators' stake
    max_skip_certificate_range: RangeInclusive<Slot>, // The largest valid skip range (initialized to 0..=0)
}

impl Default for SkipPool {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipPool {
    /// Initializes the `SkipPool`
    pub fn new() -> Self {
        Self {
            skips: HashMap::new(),
            segment_tree: DynamicSegmentTree::new(),
            max_skip_certificate_range: 0..=0, // Default range
        }
    }

    /// Adds a skip vote for a validator and updates the segment tree
    pub fn add_vote(
        &mut self,
        pubkey: &Pubkey,
        skip_range: RangeInclusive<Slot>,
        skip_transaction: VersionedTransaction,
        stake: Stake,
        total_stake: Stake,
    ) -> Result<(), AddVoteError> {
        // Remove previous skip vote if it exists
        if let Some(prev_skip_vote) = self.skips.get(pubkey) {
            if prev_skip_vote.skip_range == skip_range {
                return Err(AddVoteError::AlreadyExists(
                    prev_skip_vote.skip_range.clone(),
                ));
            }
            if prev_skip_vote.skip_range.end() > skip_range.end() {
                return Err(AddVoteError::TooOld(
                    prev_skip_vote.skip_range.clone(),
                    skip_range,
                ));
            }
            if skip_range.start() <= prev_skip_vote.skip_range.end() {
                return Err(AddVoteError::Overlapping(
                    prev_skip_vote.skip_range.clone(),
                    skip_range,
                ));
            }

            self.segment_tree.remove(
                *prev_skip_vote.skip_range.start(),
                *prev_skip_vote.skip_range.end(),
                (*pubkey, (stake as Stake)), // stake doesn't actually matter here
            );
        }

        // Add new skip range
        self.segment_tree.insert(
            *skip_range.start(),
            *skip_range.end(),
            (*pubkey, stake as Stake), // Add stake
        );

        // Store the validator's updated skip vote
        self.skips.insert(
            *pubkey,
            SkipVote {
                skip_range: skip_range.clone(),
                skip_transaction,
            },
        );

        // Find the largest range where the cumulative stake exceeds 2/3 of total stake
        let threshold = (2.0 * total_stake as f64) / 3.0; // Calculate required stake threshold
        if let Some((max_range, _)) = self.segment_tree.query(threshold) {
            self.max_skip_certificate_range = max_range; // Update to the full range
        }

        Ok(())
    }

    /// Returns the maximal skip range
    pub fn max_skip_certificate_range(&self) -> &RangeInclusive<Slot> {
        &self.max_skip_certificate_range
    }

    /// Returns the full skip certificate range and contributing `Pubkey -> VersionedTransaction` mappings
    pub fn get_skip_certificate(
        &self,
        total_stake: Stake,
    ) -> Option<(RangeInclusive<Slot>, Vec<VersionedTransaction>)> {
        let threshold = SUPERMAJORITY * total_stake as f64;
        self.segment_tree
            .query(threshold)
            .map(|(range, contributors)| {
                let mut transactions = vec![];
                for (pubkey, _) in contributors {
                    if let Some(skip_vote) = self.skips.get(&pubkey) {
                        transactions.push(skip_vote.skip_transaction.clone());
                    }
                }
                (range, transactions)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_transaction() -> VersionedTransaction {
        VersionedTransaction::default() // Creates a dummy transaction for testing
    }

    #[test]
    fn test_add_single_vote() {
        let mut pool = SkipPool::new();
        let validator = Pubkey::new_unique();
        let skip_range = 10..=20;
        let skip_tx = dummy_transaction();
        let stake = 70;
        let total_stake = 100;

        pool.add_vote(
            &validator,
            skip_range.clone(),
            skip_tx.clone(),
            stake,
            total_stake,
        )
        .unwrap();

        let stored_vote = pool.skips.get(&validator).unwrap();
        assert_eq!(stored_vote.skip_range, skip_range);
        assert_eq!(stored_vote.skip_transaction, skip_tx);
        assert_eq!(pool.max_skip_certificate_range, RangeInclusive::new(10, 20));
    }

    #[test]
    fn test_add_singleton_range() {
        let mut pool = SkipPool::new();
        let validator = Pubkey::new_unique();
        let skip_range = 1..=1;
        let skip_tx = dummy_transaction();
        let stake = 70;
        let total_stake = 100;

        pool.add_vote(
            &validator,
            skip_range.clone(),
            skip_tx.clone(),
            stake,
            total_stake,
        )
        .unwrap();

        let stored_vote = pool.skips.get(&validator).unwrap();
        assert_eq!(stored_vote.skip_range, skip_range);
        assert_eq!(stored_vote.skip_transaction, skip_tx);
        assert_eq!(pool.max_skip_certificate_range, RangeInclusive::new(1, 1));
    }

    #[test]
    fn test_add_multiple_votes() {
        let mut pool = SkipPool::new();
        let validator1 = Pubkey::new_unique();
        let validator2 = Pubkey::new_unique();

        pool.add_vote(&validator1, 5..=15, dummy_transaction(), 50, 100)
            .unwrap();
        pool.add_vote(&validator2, 20..=30, dummy_transaction(), 50, 100)
            .unwrap();

        assert_eq!(pool.max_skip_certificate_range, RangeInclusive::new(0, 0));
    }

    #[test]
    fn test_two_validators_overlapping_votes() {
        let mut pool = SkipPool::new();
        let validator1 = Pubkey::new_unique();
        let validator2 = Pubkey::new_unique();

        let tx1 = dummy_transaction();
        let tx2 = dummy_transaction();

        pool.add_vote(&validator1, 10..=20, tx1.clone(), 50, 100)
            .unwrap();
        pool.add_vote(&validator2, 15..=25, tx2.clone(), 50, 100)
            .unwrap();
        assert_eq!(pool.max_skip_certificate_range, 15..=20);

        // Test certificate is correct
        let (range, transactions) = pool.get_skip_certificate(100).unwrap();
        assert_eq!(range, 15..=20);
        assert_eq!(transactions.len(), 2);
        assert!(transactions.contains(&tx1));
        assert!(transactions.contains(&tx2));
    }

    #[test]
    fn test_update_existing_vote() {
        let mut pool = SkipPool::new();
        let validator = Pubkey::new_unique();

        pool.add_vote(&validator, 10..=20, dummy_transaction(), 70, 100)
            .unwrap();
        assert_eq!(pool.max_skip_certificate_range, RangeInclusive::new(10, 20));

        // AlreadyExists failure
        assert_eq!(
            pool.add_vote(&validator, 10..=20, dummy_transaction(), 70, 100),
            Err(AddVoteError::AlreadyExists(10..=20))
        );

        // TooOld failure (trying to add 15..=17 when 10..=20 already exists)
        assert_eq!(
            pool.add_vote(&validator, 15..=17, dummy_transaction(), 70, 100),
            Err(AddVoteError::TooOld(10..=20, 15..=17))
        );

        // Overlapping failures
        assert_eq!(
            pool.add_vote(&validator, 15..=25, dummy_transaction(), 70, 100),
            Err(AddVoteError::Overlapping(10..=20, 15..=25))
        );

        assert_eq!(
            pool.add_vote(&validator, 20..=25, dummy_transaction(), 70, 100),
            Err(AddVoteError::Overlapping(10..=20, 20..=25))
        );

        // Adding a new, non-overlapping range
        pool.add_vote(&validator, 21..=22, dummy_transaction(), 70, 100)
            .unwrap();
        assert_eq!(pool.max_skip_certificate_range, RangeInclusive::new(21, 22));
    }

    #[test]
    fn test_threshold_not_reached() {
        let mut pool = SkipPool::new();
        let validator1 = Pubkey::new_unique();
        let validator2 = Pubkey::new_unique();

        pool.add_vote(&validator1, 5..=15, dummy_transaction(), 30, 100)
            .unwrap();
        pool.add_vote(&validator2, 20..=30, dummy_transaction(), 30, 100)
            .unwrap();

        assert_eq!(pool.max_skip_certificate_range, RangeInclusive::new(0, 0));
    }
}
