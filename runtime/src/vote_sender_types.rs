use {
    alpenglow_vote::bls_message::BLSMessage,
    crossbeam_channel::{Receiver, Sender},
    solana_vote::vote_parser::ParsedVote,
};

pub type ReplayVoteSender = Sender<ParsedVote>;
pub type ReplayVoteReceiver = Receiver<ParsedVote>;

pub type BLSVerifiedMessageSender = Sender<BLSMessage>;
pub type BLSVerifiedMessageReceiver = Receiver<BLSMessage>;
