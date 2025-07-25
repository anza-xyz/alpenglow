//! Error types

use {
    num_derive::FromPrimitive,
    solana_program::{decode_error::DecodeError, program_error::ProgramError},
    thiserror::Error,
};

/// Errors that may be returned by the program.
#[derive(Clone, Copy, Debug, Eq, Error, FromPrimitive, PartialEq)]
pub enum VoteError {
    /// Closing an active vote account
    #[error("Cannot close vote account unless it stopped voting at least one full epoch ago")]
    ActiveVoteAccountClose,

    /// Increasing commission too late into the epoch
    #[error("Cannot update commission at this point in the epoch")]
    CommissionUpdateTooLate,

    /// Invalid instruction
    #[error("Invalid instruction")]
    InvalidInstruction,

    /// Invalid Vote Authorize enum
    #[error("Invalid vote authorize")]
    InvalidAuthorizeType,

    /// Missing epoch schedule sysvar
    #[error("Missing epoch schedule sysvar")]
    MissingEpochScheduleSysvar,

    /// Missing slot hashes sysvar
    #[error("Missing slot hashes sysvar")]
    MissingSlotHashesSysvar,

    /// Replay bank hash mismatch
    #[error("Replay bank hash mismatch")]
    ReplayBankHashMismatch,

    /// Skip slot is present on this fork
    #[error("Skipped slot is present on this fork")]
    SkipSlotPresent,

    /// Skip slot exceeds clock slot
    #[error("Skip slot exceeds clock slot")]
    SkipSlotExceedsCurrentSlot,

    /// Slot hashes is missing the replayed slot key
    #[error("Slot hashes is missing the replayed slot key")]
    SlotHashesMissingKey,

    /// Version mismatch
    #[error("Version mismatch")]
    VersionMismatch,
}

impl From<VoteError> for ProgramError {
    fn from(e: VoteError) -> Self {
        ProgramError::Custom(e as u32)
    }
}

impl<T> DecodeError<T> for VoteError {
    fn type_of() -> &'static str {
        "Vote Error"
    }
}
