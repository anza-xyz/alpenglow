use {
    alpenglow_vote::{bls_message::VoteMessage, vote::Vote},
    solana_bls::keypair::Keypair as BLSKeypair,
    solana_sdk::{hash::Hash, transaction::VersionedTransaction},
};

pub trait AlpenglowVoteTransaction: Clone + std::fmt::Debug {
    fn new_for_test(bls_keypair: BLSKeypair) -> Self;
}

impl AlpenglowVoteTransaction for VersionedTransaction {
    fn new_for_test(_bls_keypair: BLSKeypair) -> Self {
        Self::default()
    }
}

impl AlpenglowVoteTransaction for VoteMessage {
    fn new_for_test(bls_keypair: BLSKeypair) -> Self {
        // use notarization vote since this is just for tests
        let vote = Vote::new_notarization_vote(5, Hash::default(), Hash::default());

        // unwrap since this is just for tests
        let signature = bls_keypair.sign(&bincode::serialize(&vote).unwrap()).into();

        VoteMessage {
            vote,
            signature,
            rank: 0,
        }
    }
}
