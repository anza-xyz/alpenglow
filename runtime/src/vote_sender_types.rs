use {
    crossbeam_channel::{Receiver, Sender},
    solana_vote::{alpenglow::bls_message::BLSMessage, vote_parser::ParsedVote},
};

pub type ReplayVoteSender = Sender<ParsedVote>;
pub type ReplayVoteReceiver = Receiver<ParsedVote>;

pub type BLSVerifiedMessageSender = Sender<BLSMessage>;
pub type BLSVerifiedMessageReceiver = Receiver<BLSMessage>;
