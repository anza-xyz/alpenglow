use {
    crate::alpenglow_consensus::certificate_pool::Vote,
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey},
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
    frozen_abi(digest = "B22qs7ro2VKH9tXzimwhhgC3rPTLmrJaKMycQTfEB3e5")
)]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct VoteHistory {
    pub node_pubkey: Pubkey,
    pub latest_notarize_vote: Vote,
    // Important to avoid double voting Skip and Finalization for the
    // same slot.
    // Not sufficient to track just the latest skip vote because you
    // can potentially
    // 1. Vote skip on block X
    // 2. block X gets notarized
    // 3. Vote skip on block X + 1
    // 4. Now you need the skip vote from 1. to avoid voting to finalize
    // block X, which you wouldn't have if 3. overwrites 1.
    pub skip_votes: Vec<Vote>,
    pub latest_finalize_vote: Vote,
    pub root: Slot,
}

impl VoteHistory {
    pub fn new(node_pubkey: Pubkey, root: Slot) -> Self {
        Self {
            node_pubkey,
            latest_notarize_vote: Vote::Notarize(0),
            skip_votes: vec![],
            latest_finalize_vote: Vote::Finalize(0),
            root,
        }
    }

    pub fn push_skip_vote(&mut self, new_skip_vote: Vote) {
        let new_skip_range = new_skip_vote.skip_range().unwrap();

        match self.skip_votes.last_mut() {
            Some(last_vote)
                if last_vote.skip_range().unwrap().start() == new_skip_range.start() =>
            {
                assert!(new_skip_range.end() >= last_vote.skip_range().unwrap().end());
                // The new vote is a superset of the last vote, so just replace the last vote
                *last_vote = new_skip_vote;
            }
            Some(last_vote) => {
                // Skip votes must be monotonically increasing
                assert!(new_skip_range.start() > last_vote.skip_range().unwrap().end());
                self.skip_votes.push(new_skip_vote);
            }
            None => self.skip_votes.push(new_skip_vote),
        }
    }

    pub fn is_slot_skipped(&self, slot: Slot) -> bool {
        self.skip_votes.iter().any(|vote| {
            vote.skip_range()
                .expect("must be a skip vote")
                .contains(&slot)
        })
    }

    pub fn set_root(&mut self, root: Slot) {
        self.skip_votes
            .retain(|skip_vote| *skip_vote.skip_range().expect("must be a skip vote").end() > root)
    }
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
