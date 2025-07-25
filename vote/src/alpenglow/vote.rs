//! Vote data types for use by clients

use serde::{Deserialize, Serialize};
use solana_hash::Hash;
use solana_program::clock::Slot;
use solana_program::instruction::Instruction;
use solana_program::program_error::ProgramError;
use solana_program::pubkey::Pubkey;

use crate::alpenglow::instruction::{
    self, decode_instruction_data, decode_instruction_type, VoteInstruction,
};
use crate::alpenglow::state::PodSlot;
use crate::alpenglow::vote_processor::NotarizationVoteInstructionData;

/// Enum that clients can use to parse and create the vote
/// structures expected by the program
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample, AbiEnumVisitor),
    frozen_abi(digest = "CSKLGPPbBibWP9rptpJyWSJ4vELSoUkA8MLqrRdLhYr8")
)]
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Vote {
    /// A notarization vote
    Notarize(NotarizationVote),
    /// A finalization vote
    Finalize(FinalizationVote),
    /// A skip vote
    Skip(SkipVote),
    /// A notarization fallback vote
    NotarizeFallback(NotarizationFallbackVote),
    /// A skip fallback vote
    SkipFallback(SkipFallbackVote),
}

impl Vote {
    /// Create a new notarization vote
    pub fn new_notarization_vote(slot: Slot, block_id: Hash, bank_hash: Hash) -> Self {
        Self::from(NotarizationVote::new(
            slot, block_id, 0, /*_replayed_slot not used */
            bank_hash,
        ))
    }

    /// Create a new finalization vote
    pub fn new_finalization_vote(slot: Slot) -> Self {
        Self::from(FinalizationVote::new(slot))
    }

    /// Create a new skip vote
    pub fn new_skip_vote(slot: Slot) -> Self {
        Self::from(SkipVote::new(slot))
    }

    /// Create a new notarization fallback vote
    pub fn new_notarization_fallback_vote(slot: Slot, block_id: Hash, bank_hash: Hash) -> Self {
        Self::from(NotarizationFallbackVote::new(
            slot, block_id, 0, /*_replayed_slot not used */
            bank_hash,
        ))
    }

    /// Create a new skip fallback vote
    pub fn new_skip_fallback_vote(slot: Slot) -> Self {
        Self::from(SkipFallbackVote::new(slot))
    }

    /// If this instruction represented by `instruction_data` is a vote
    pub fn is_simple_vote(instruction_data: &[u8]) -> Result<bool, ProgramError> {
        let instruction_type = decode_instruction_type(instruction_data)?;
        Ok(matches!(
            instruction_type,
            VoteInstruction::Notarize
                | VoteInstruction::Finalize
                | VoteInstruction::Skip
                | VoteInstruction::NotarizeFallback
                | VoteInstruction::SkipFallback
        ))
    }

    /// Deserializes instruction represented by `instruction_data` into a `Vote`
    /// Must be guarded by `is_simple_vote`
    pub fn deserialize_simple_vote(instruction_data: &[u8]) -> Result<Vote, ProgramError> {
        debug_assert!(Self::is_simple_vote(instruction_data)?);
        let instruction_type = decode_instruction_type(instruction_data)?;
        match instruction_type {
            VoteInstruction::Notarize => {
                let notarization_vote =
                    decode_instruction_data::<NotarizationVoteInstructionData>(instruction_data)?;
                Ok(Vote::from(NotarizationVote::new_internal(
                    notarization_vote,
                )))
            }
            VoteInstruction::Finalize => {
                let finalization_slot = decode_instruction_data::<PodSlot>(instruction_data)?;
                Ok(Vote::from(FinalizationVote::new_internal(
                    finalization_slot,
                )))
            }
            VoteInstruction::Skip => {
                let skip_slot = decode_instruction_data::<PodSlot>(instruction_data)?;
                Ok(Vote::from(SkipVote::new_internal(skip_slot)))
            }
            VoteInstruction::NotarizeFallback => {
                let notarization_fallback_vote =
                    decode_instruction_data::<NotarizationVoteInstructionData>(instruction_data)?;
                Ok(Vote::from(NotarizationFallbackVote::new_internal(
                    notarization_fallback_vote,
                )))
            }
            VoteInstruction::SkipFallback => {
                let skip_fallback_slot = decode_instruction_data::<PodSlot>(instruction_data)?;
                Ok(Vote::from(SkipFallbackVote::new_internal(
                    skip_fallback_slot,
                )))
            }
            _ => panic!("Programmer error"),
        }
    }

    /// Generate a vote instruction from this vote
    pub fn to_vote_instruction(&self, vote_pubkey: Pubkey, vote_authority: Pubkey) -> Instruction {
        match self {
            Self::Notarize(vote) => instruction::notarize(vote_pubkey, vote_authority, vote),
            Self::Finalize(vote) => instruction::finalize(vote_pubkey, vote_authority, vote),
            Self::Skip(vote) => instruction::skip(vote_pubkey, vote_authority, vote),
            Self::NotarizeFallback(vote) => {
                instruction::notarize_fallback(vote_pubkey, vote_authority, vote)
            }
            Self::SkipFallback(vote) => {
                instruction::skip_fallback(vote_pubkey, vote_authority, vote)
            }
        }
    }

    /// The slot which was voted for
    pub fn slot(&self) -> Slot {
        match self {
            Self::Notarize(vote) => vote.slot(),
            Self::Finalize(vote) => vote.slot(),
            Self::Skip(vote) => vote.slot(),
            Self::NotarizeFallback(vote) => vote.slot(),
            Self::SkipFallback(vote) => vote.slot(),
        }
    }

    /// The block id associated with the block which was voted for
    pub fn block_id(&self) -> Option<&Hash> {
        match self {
            Self::Notarize(vote) => Some(vote.block_id()),
            Self::NotarizeFallback(vote) => Some(vote.block_id()),
            Self::Finalize(_) | Self::Skip(_) | Self::SkipFallback(_) => None,
        }
    }

    /// The replayed bank hash associated with the block which was voted for
    pub fn replayed_bank_hash(&self) -> Option<&Hash> {
        match self {
            Self::Notarize(vote) => Some(vote.replayed_bank_hash()),
            Self::NotarizeFallback(vote) => Some(vote.replayed_bank_hash()),
            Self::Finalize(_) | Self::Skip(_) | Self::SkipFallback(_) => None,
        }
    }

    /// Whether the vote is a notarization vote
    pub fn is_notarization(&self) -> bool {
        matches!(self, Self::Notarize(_))
    }

    /// Whether the vote is a finalization vote
    pub fn is_finalize(&self) -> bool {
        matches!(self, Self::Finalize(_))
    }

    /// Whether the vote is a skip vote
    pub fn is_skip(&self) -> bool {
        matches!(self, Self::Skip(_))
    }

    /// Whether the vote is a notarization fallback vote
    pub fn is_notarize_fallback(&self) -> bool {
        matches!(self, Self::NotarizeFallback(_))
    }

    /// Whether the vote is a skip fallback vote
    pub fn is_skip_fallback(&self) -> bool {
        matches!(self, Self::SkipFallback(_))
    }

    /// Whether the vote is a notarization or finalization
    pub fn is_notarization_or_finalization(&self) -> bool {
        matches!(self, Self::Notarize(_) | Self::Finalize(_))
    }
}

impl From<NotarizationVote> for Vote {
    fn from(vote: NotarizationVote) -> Self {
        Self::Notarize(vote)
    }
}

impl From<FinalizationVote> for Vote {
    fn from(vote: FinalizationVote) -> Self {
        Self::Finalize(vote)
    }
}

impl From<SkipVote> for Vote {
    fn from(vote: SkipVote) -> Self {
        Self::Skip(vote)
    }
}

impl From<NotarizationFallbackVote> for Vote {
    fn from(vote: NotarizationFallbackVote) -> Self {
        Self::NotarizeFallback(vote)
    }
}

impl From<SkipFallbackVote> for Vote {
    fn from(vote: SkipFallbackVote) -> Self {
        Self::SkipFallback(vote)
    }
}

/// A notarization vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "Hi27FUrTftp2qjyZ96pqYYVyAofVqcZDWSQL4ZgLFpBF")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NotarizationVote {
    slot: Slot,
    block_id: Hash,
    replayed_slot: Slot, // NOTE: replayed_slot will be unused until we support APE
    replayed_bank_hash: Hash,
}

impl NotarizationVote {
    fn new_internal(notarization_vote: &NotarizationVoteInstructionData) -> Self {
        Self {
            slot: Slot::from(notarization_vote.slot),
            block_id: notarization_vote.block_id,
            replayed_slot: 0,
            replayed_bank_hash: notarization_vote.replayed_bank_hash,
        }
    }

    /// Construct a notarization vote for `slot`
    pub fn new(slot: Slot, block_id: Hash, replayed_slot: Slot, replayed_bank_hash: Hash) -> Self {
        Self {
            slot,
            block_id,
            replayed_slot,
            replayed_bank_hash,
        }
    }

    /// The slot to notarize
    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// The block_id of the notarization slot
    pub fn block_id(&self) -> &Hash {
        &self.block_id
    }

    /// The bank hash of the latest replayed slot
    pub fn replayed_bank_hash(&self) -> &Hash {
        &self.replayed_bank_hash
    }
}

/// A finalization vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "2XQ5N6YLJjF28w7cMFFUQ9SDgKuf9JpJNtAiXSPA8vR2")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct FinalizationVote {
    slot: Slot,
}

impl FinalizationVote {
    fn new_internal(finalization_slot: &PodSlot) -> Self {
        Self {
            slot: Slot::from(*finalization_slot),
        }
    }

    /// Construct a finalization vote for `slot`
    pub fn new(slot: Slot) -> Self {
        Self { slot }
    }

    /// The slot to finalize
    pub fn slot(&self) -> Slot {
        self.slot
    }
}

/// A skip vote
/// Represents a range of slots to skip
/// inclusive on both ends
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "G8Nrx3sMYdnLpHsCNark3BGA58BmW2sqNnqjkYhQHtN")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SkipVote {
    pub(crate) slot: Slot,
}

impl SkipVote {
    fn new_internal(slot: &PodSlot) -> Self {
        Self {
            slot: Slot::from(*slot),
        }
    }

    /// Construct a skip vote for `slot`
    pub fn new(slot: Slot) -> Self {
        Self { slot }
    }

    /// The slot to skip
    pub fn slot(&self) -> Slot {
        self.slot
    }
}

/// A notarization fallback vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "91HNr78MNiTLAW5JioukfYBUgfPtch3aNgsYVgBJH6dC")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NotarizationFallbackVote {
    slot: Slot,
    block_id: Hash,
    replayed_slot: Slot, // NOTE: replayed_slot will be unused until we support APE
    replayed_bank_hash: Hash,
}

impl NotarizationFallbackVote {
    fn new_internal(notarization_vote: &NotarizationVoteInstructionData) -> Self {
        Self {
            slot: Slot::from(notarization_vote.slot),
            block_id: notarization_vote.block_id,
            replayed_slot: 0,
            replayed_bank_hash: notarization_vote.replayed_bank_hash,
        }
    }

    /// Construct a notarization vote for `slot`
    pub fn new(slot: Slot, block_id: Hash, replayed_slot: Slot, replayed_bank_hash: Hash) -> Self {
        Self {
            slot,
            block_id,
            replayed_slot,
            replayed_bank_hash,
        }
    }

    /// The slot to notarize
    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// The block_id of the notarization slot
    pub fn block_id(&self) -> &Hash {
        &self.block_id
    }

    /// The bank hash of the latest replayed slot
    pub fn replayed_bank_hash(&self) -> &Hash {
        &self.replayed_bank_hash
    }
}

/// A skip fallback vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "WsUNum8V62gjRU1yAnPuBMAQui4YvMwD1RwrzHeYkeF")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SkipFallbackVote {
    pub(crate) slot: Slot,
}

impl SkipFallbackVote {
    fn new_internal(slot: &PodSlot) -> Self {
        Self {
            slot: Slot::from(*slot),
        }
    }

    /// Construct a skip fallback vote for `slot`
    pub fn new(slot: Slot) -> Self {
        Self { slot }
    }

    /// The slot to skip
    pub fn slot(&self) -> Slot {
        self.slot
    }
}
