use {solana_bls::keypair::Keypair as BLSKeypair, solana_sdk::transaction::VersionedTransaction};

pub trait AlpenglowVoteTransaction: Clone + std::fmt::Debug {
    fn new_for_test(bls_keypair: Option<BLSKeypair>) -> Self;
}

impl AlpenglowVoteTransaction for VersionedTransaction {
    fn new_for_test(_: Option<BLSKeypair>) -> Self {
        Self::default()
    }
}
