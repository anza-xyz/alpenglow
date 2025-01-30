//! Vote program errors

use {
    core::fmt,
    num_derive::{FromPrimitive, ToPrimitive},
    solana_decode_error::DecodeError,
};

/// Reasons the vote might have had an error
#[derive(Debug, Clone, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum VoteError {
    VoteTooOld,
    SlotsMismatch,
    SlotHashMismatch,
    TimestampTooOld,
    TooSoonToReauthorize,
    ActiveVoteAccountClose,
    CommissionUpdateTooLate,
    AssertionFailed,
}

impl std::error::Error for VoteError {}

impl fmt::Display for VoteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::VoteTooOld => "vote already recorded or not in slot hashes history",
            Self::SlotsMismatch => "vote slots do not match bank history",
            Self::SlotHashMismatch => "vote hash does not match bank hash",
            Self::TimestampTooOld => "vote timestamp not recent",
            Self::TooSoonToReauthorize => "authorized voter has already been changed this epoch",
            Self::ActiveVoteAccountClose => {
                "Cannot close vote account unless it stopped voting at least one full epoch ago"
            }
            Self::CommissionUpdateTooLate => "Cannot update commission at this point in the epoch",
            Self::AssertionFailed => "Assertion failed",
        })
    }
}

impl<E> DecodeError<E> for VoteError {
    fn type_of() -> &'static str {
        "VoteError"
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_instruction::error::InstructionError};

    #[test]
    fn test_custom_error_decode() {
        use num_traits::FromPrimitive;
        fn pretty_err<T>(err: InstructionError) -> String
        where
            T: 'static + std::error::Error + DecodeError<T> + FromPrimitive,
        {
            if let InstructionError::Custom(code) = err {
                let specific_error: T = T::decode_custom_error_to_enum(code).unwrap();
                format!(
                    "{:?}: {}::{:?} - {}",
                    err,
                    T::type_of(),
                    specific_error,
                    specific_error,
                )
            } else {
                "".to_string()
            }
        }
        assert_eq!(
            "Custom(0): VoteError::VoteTooOld - vote already recorded or not in slot hashes history",
            pretty_err::<VoteError>(VoteError::VoteTooOld.into())
        )
    }
}
