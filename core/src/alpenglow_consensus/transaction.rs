use {solana_bls::keypair::Keypair as BLSKeypair, solana_sdk::transaction::VersionedTransaction};

pub trait AlpenglowVoteTransaction: Clone + std::fmt::Debug {
    fn new_for_test(bls_keypair: BLSKeypair) -> Self;
}

impl AlpenglowVoteTransaction for VersionedTransaction {
    fn new_for_test(_bls_keypair: BLSKeypair) -> Self {
        Self::default()
    }
}
