use super::*;
#[cfg(test)]
use arbitrary::{Arbitrary, Unstructured};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum VoteStateVersions {
    Current(Box<VoteState>),
}

impl VoteStateVersions {
    pub fn new_current(vote_state: VoteState) -> Self {
        Self::Current(Box::new(vote_state))
    }

    pub fn convert_to_current(self) -> VoteState {
        match self {
            VoteStateVersions::Current(state) => *state,
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        match self {
            VoteStateVersions::Current(vote_state) => vote_state.authorized_voters.is_empty(),
        }
    }

    pub fn vote_state_size_of() -> usize {
        VoteState::size_of()
    }

    pub fn is_correct_size_and_initialized(data: &[u8]) -> bool {
        VoteState::is_correct_size_and_initialized(data)
    }
}

#[cfg(test)]
impl Arbitrary<'_> for VoteStateVersions {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let variant = u.choose_index(1)?;
        match variant {
            0 => Ok(Self::Current(Box::new(VoteState::arbitrary(u)?))),
            _ => unreachable!(),
        }
    }
}
