use {
    crossbeam_channel::{Receiver, Sender},
    solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction},
    solana_vote::alpenglow::{bls_message::BLSMessage, vote::Vote as AlpenglowVote},
    solana_vote::vote_parser::ParsedVote,
};

pub type ReplayVoteSender = Sender<ParsedVote>;
pub type ReplayVoteReceiver = Receiver<ParsedVote>;

pub type AlpenglowVoteSender = Sender<(AlpenglowVote, Pubkey, VersionedTransaction)>;
pub type AlpenglowVoteReceiver = Receiver<(AlpenglowVote, Pubkey, VersionedTransaction)>;

pub type BLSVerifiedMessageSender = Sender<BLSMessage>;
pub type BLSVerifiedMessageReceiver = Receiver<BLSMessage>;
