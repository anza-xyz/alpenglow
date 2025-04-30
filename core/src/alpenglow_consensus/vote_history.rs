use {
    alpenglow_vote::vote::Vote,
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey},
    std::collections::{HashMap, HashSet},
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
    frozen_abi(digest = "J6QWp12N4Npvs3DFzp7Hn5ynoc8rwLfJttQXdJpc9Moi")
)]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct VoteHistory {
    /// The validator identity that cast votes
    pub(crate) node_pubkey: Pubkey,

    /// The slots which this node has cast either a notarization or skip vote
    voted: HashSet<Slot>,

    /// The blocks for which this node has cast a notarization vote
    /// In the format of slot, block_id, bank_hash
    voted_notar: HashMap<Slot, (Hash, Hash)>,

    /// The slots in which this node has cast at least one of:
    /// - `SkipVote`
    /// - `SkipFallback`
    /// - `NotarizeFallback`
    skipped: HashSet<Slot>,

    /// The slots for which this node has cast a finalization vote. This node
    /// will not cast any additional votes for these slots
    its_over: HashSet<Slot>,

    /// The latest skip vote cast, for the purpose of refresh
    latest_skip_vote: Option<Vote>,

    /// The latest root set by the voting loop. The above structures will not
    /// contain votes for slots before `root`
    root: Slot,
}

impl VoteHistory {
    pub fn new(node_pubkey: Pubkey, root: Slot) -> Self {
        Self {
            node_pubkey,
            root,
            ..Self::default()
        }
    }

    /// Have we cast a notarization or skip vote for `slot`
    pub fn voted(&self, slot: Slot) -> bool {
        self.voted.contains(&slot)
    }

    /// The block for which we voted notarize in slot `slot`
    pub fn voted_notar(&self, slot: Slot) -> Option<(Hash, Hash)> {
        self.voted_notar.get(&slot).copied()
    }

    /// Have we cast any skip vote variation for `slot`
    pub fn skipped(&self, slot: Slot) -> bool {
        self.skipped.contains(&slot)
    }

    /// Have we casted a finalization vote for `slot`
    pub fn its_over(&self, slot: Slot) -> bool {
        self.its_over.contains(&slot)
    }

    /// The latest skip vote for use in refresh
    pub fn latest_skip_vote(&self) -> Option<Vote> {
        self.latest_skip_vote
    }

    /// The latest root slot set by the voting loop
    pub fn root(&self) -> Slot {
        self.root
    }

    /// Add a new vote to the voting history
    pub fn add_vote(&mut self, vote: Vote) {
        // TODO: these assert!s are for my debugging, can consider removing
        // in final version
        match vote {
            Vote::Notarize(vote) => {
                assert!(self.voted.insert(vote.slot()));
                assert!(self
                    .voted_notar
                    .insert(vote.slot(), (*vote.block_id(), *vote.replayed_bank_hash()))
                    .is_none());
            }
            Vote::Finalize(vote) => {
                assert!(!self.skipped(vote.slot()));
                self.its_over.insert(vote.slot());
            }
            skip_vote @ Vote::Skip(vote) => {
                self.voted.insert(vote.slot());
                self.skipped.insert(vote.slot());
                self.latest_skip_vote = Some(skip_vote);
            }
            Vote::NotarizeFallback(vote) => {
                assert!(self.voted(vote.slot()));
                assert!(!self.its_over(vote.slot()));
                self.skipped.insert(vote.slot());
            }
            Vote::SkipFallback(vote) => {
                assert!(self.voted(vote.slot()));
                assert!(!self.its_over(vote.slot()));
                self.skipped.insert(vote.slot());
            }
        }
    }

    /// Sets the new root slot and cleans up outdated slots < `root`
    pub fn set_root(&mut self, root: Slot) {
        self.root = root;
        self.voted.retain(|s| *s > root);
        self.voted_notar.retain(|s, (_, _)| *s > root);
        self.skipped.retain(|s| *s > root);
        self.its_over.retain(|s| *s > root);
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
