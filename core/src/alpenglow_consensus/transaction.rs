use {
    alpenglow_vote::{
        bls_message::{BLSMessage, VoteMessage},
        vote::Vote,
    },
    solana_bls::Signature as BLSSignature,
    solana_sdk::transaction::VersionedTransaction,
};

pub trait AlpenglowVoteTransaction: Clone + std::fmt::Debug {
    fn new_for_test(signature: BLSSignature, vote: Vote, rank: usize) -> Self;

    fn get_vote_and_rank(&self) -> Option<(&Vote, u16)>;
}

impl AlpenglowVoteTransaction for VersionedTransaction {
    fn new_for_test(_signature: BLSSignature, _vote: Vote, _rank: usize) -> Self {
        Self::default()
    }

    fn get_vote_and_rank(&self) -> Option<(&Vote, u16)> {
        unimplemented!("LegacyVoteCertificate does not support get_vote_and_rank");
    }
}

impl AlpenglowVoteTransaction for BLSMessage {
    fn new_for_test(signature: BLSSignature, vote: Vote, rank: usize) -> Self {
        BLSMessage::Vote(VoteMessage {
            vote,
            signature,
            rank: rank as u16,
        })
    }

    fn get_vote_and_rank(&self) -> Option<(&Vote, u16)> {
        if let BLSMessage::Vote(vote_message) = self {
            Some((&vote_message.vote, vote_message.rank))
        } else {
            None
        }
    }
}
