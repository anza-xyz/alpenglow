use solana_sdk::transaction::VersionedTransaction;

/// A trait for signature types used in vote transactions
pub trait VoteSignature {
    /// Type of an aggregate set of signatures
    type Aggregate: Clone;

    /// Extract signature from a transaction
    fn from_transaction(transaction: VersionedTransaction) -> Self;

    /// Create an empty aggregate set of signatures
    fn empty_aggregate() -> Self::Aggregate;

    /// Add a set of signatures to an aggregate set
    fn aggregate_with<'a, I>(aggregate: &mut Self::Aggregate, signatures: I)
    where
        I: IntoIterator<Item = &'a Self>,
        Self: 'a;

    /// Aggregate a set of signatures
    fn aggregate<'a, I>(signatures: I) -> Self::Aggregate
    where
        I: IntoIterator<Item = &'a Self>,
        Self: 'a,
    {
        let mut aggregate = Self::empty_aggregate();
        Self::aggregate_with(&mut aggregate, signatures);
        aggregate
    }
}

/// `VersionedTransaction`s can be used as vote signatures
impl VoteSignature for VersionedTransaction {
    type Aggregate = Vec<VersionedTransaction>;

    fn from_transaction(transaction: VersionedTransaction) -> Self {
        transaction
    }

    fn empty_aggregate() -> Self::Aggregate {
        Vec::new()
    }

    fn aggregate_with<'a, I>(aggregate: &mut Self::Aggregate, signatures: I)
    where
        I: IntoIterator<Item = &'a Self>,
        Self: 'a,
    {
        aggregate.extend(signatures.into_iter().cloned());
    }
}
