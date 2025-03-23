use {
    agave_votor_messages::vote::Vote as AlpenglowVote,
    crossbeam_channel::{Receiver, Sender},
    solana_pubkey::Pubkey,
    solana_transaction::Transaction,
    solana_vote::vote_parser::ParsedVote,
};

pub type ReplayVoteSender = Sender<ParsedVote>;
pub type ReplayVoteReceiver = Receiver<ParsedVote>;

pub type AlpenglowVoteSender = Sender<(AlpenglowVote, Pubkey, Transaction)>;
pub type AlpenglowVoteReceiver = Receiver<(AlpenglowVote, Pubkey, Transaction)>;
