use solana_sdk::transaction::VersionedTransaction;

pub trait AlpenglowVoteTransaction: Clone + std::fmt::Debug {
    fn new_for_test() -> Self;
}

impl AlpenglowVoteTransaction for VersionedTransaction {
    fn new_for_test() -> Self {
        Self::default()
    }
}
