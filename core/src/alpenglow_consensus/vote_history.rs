use {
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey},
    solana_vote_program::vote_state::VoteTransaction,
    std::collections::BTreeMap,
    thiserror::Error,
};

pub const VOTE_THRESHOLD_SIZE: f64 = 2f64 / 3f64;

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(PartialEq, Eq, Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum BlockhashStatus {
    /// No vote since restart
    #[default]
    Uninitialized,
    /// Non voting validator
    NonVoting,
    /// Hot spare validator
    HotSpare,
    /// Successfully generated vote tx with blockhash
    Blockhash(Slot, Hash),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum VoteHistoryVersions {
    Current(VoteHistory),
}
impl VoteHistoryVersions {
    pub fn new_current(vote_history: VoteHistory) -> Self {
        Self::Current(vote_history)
    }

    pub fn convert_to_current(self) -> VoteHistory {
        match self {
            VoteHistoryVersions::Current(vote_history) => vote_history,
        }
    }
}

#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "8ziHa1vA7WG5RCvXiE3g1f2qjSTNa47FB7e2czo7en7a")
)]
#[derive(Clone, Serialize, Default, Deserialize, Debug, PartialEq)]
pub struct VoteHistory {
    pub node_pubkey: Pubkey,
    // Important to avoid double voting Skip and Finalization for the
    // same slot
    pub votes_since_root: BTreeMap<Slot, VoteTransaction>,
    pub root: Slot,
}

#[derive(Error, Debug)]
pub enum VoteHistoryError {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization Error: {0}")]
    SerializeError(#[from] bincode::Error),

    #[error("The signature on the saved vote history is invalid")]
    InvalidSignature,

    #[error("The vote history does not match this validator: {0}")]
    WrongVoteHistory(String),

    #[error("The vote history is useless because of new hard fork: {0}")]
    HardFork(Slot),
}

impl VoteHistoryError {
    pub fn is_file_missing(&self) -> bool {
        if let VoteHistoryError::IoError(io_err) = &self {
            io_err.kind() == std::io::ErrorKind::NotFound
        } else {
            false
        }
    }
}
