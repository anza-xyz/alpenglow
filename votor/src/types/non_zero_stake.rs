use {crate::common::Stake, std::num::NonZeroU64};

/// A struct to represent stake that is guaranteed to not be 0.
#[derive(Clone, Copy)]
pub(crate) struct NonZeroStake(NonZeroU64);

impl NonZeroStake {
    /// Constructs a new instance of [`NonZeroStake`].
    pub(crate) fn try_new(stake: Stake) -> Option<Self> {
        NonZeroU64::new(stake).map(Self)
    }

    /// Returns the [`Stake`].
    pub(crate) fn get(&self) -> Stake {
        self.0.get()
    }
}

impl From<NonZeroStake> for NonZeroU64 {
    fn from(stake: NonZeroStake) -> Self {
        stake.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_constructor() {
        assert!(NonZeroStake::try_new(0).is_none());
        assert_eq!(NonZeroStake::try_new(1).unwrap().get(), 1);
    }
}
