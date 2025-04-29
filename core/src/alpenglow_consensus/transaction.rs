use solana_sdk::transaction::VersionedTransaction;

pub trait AlpenglowVoteTransaction: Clone + Default {}

impl AlpenglowVoteTransaction for VersionedTransaction {}
