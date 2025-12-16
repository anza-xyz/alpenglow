use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fraction {
    numerator: u64,
    denominator: NonZeroU64,
}

impl Fraction {
    #[inline]
    pub const fn new(numerator: u64, denominator: NonZeroU64) -> Self {
        Self {
            numerator,
            denominator,
        }
    }

    #[inline]
    pub const fn from_percentage(pct: u64) -> Self {
        // SAFETY: 100 != 0
        Self::new(pct, unsafe { NonZeroU64::new_unchecked(100) })
    }
}

impl PartialOrd for Fraction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Fraction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Cross-multiply to compare
        let lhs = (self.numerator as u128)
            .checked_mul(other.denominator.get() as u128)
            .unwrap();
        let rhs = (other.numerator as u128)
            .checked_mul(self.denominator.get() as u128)
            .unwrap();
        lhs.cmp(&rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frac(n: u64, d: u64) -> Fraction {
        Fraction::new(n, NonZeroU64::new(d).unwrap())
    }

    #[test]
    fn test_cmp() {
        assert!(frac(1, 3) < frac(1, 2));
        assert!(frac(2, 4) <= frac(1, 2));
        assert!(frac(2, 4) >= frac(1, 2));
        assert!(frac(3, 4) > frac(2, 3));
    }

    #[test]
    fn test_f64_precision_loss() {
        let total_stake = NonZeroU64::new(100_000_000_000_000_000).unwrap();
        let stake = 60_000_000_000_000_001u64; // 60% + 1

        let f64_ratio = stake as f64 / total_stake.get() as f64;
        assert!(f64_ratio <= 0.6); // wrong!
        assert!(Fraction::new(stake, total_stake) > Fraction::from_percentage(60));
    }
}
