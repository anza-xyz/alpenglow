/// Numerator / denominator, with denominator != 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fraction {
    numerator: u64,
    denominator: u64,
}

impl Fraction {
    #[inline]
    pub const fn new(numerator: u64, denominator: u64) -> Self {
        if denominator == 0 {
            panic!("Fraction with zero denominoator found.");
        }

        Self {
            numerator,
            denominator,
        }
    }

    #[inline]
    pub const fn from_tuple((numerator, denominator): (u64, u64)) -> Self {
        Self::new(numerator, denominator)
    }

    #[inline]
    pub const fn from_percentage(pct: u64) -> Self {
        Self::new(pct, 100)
    }
}

impl PartialOrd for Fraction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Fraction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Bowtie method to avoid division
        let lhs = (self.numerator as u128)
            .checked_mul(other.denominator as u128)
            .unwrap();
        let rhs = (other.numerator as u128)
            .checked_mul(self.denominator as u128)
            .unwrap();
        lhs.cmp(&rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmp() {
        assert!(Fraction::new(1, 3) < Fraction::new(1, 2));
        assert!(Fraction::new(2, 4) <= Fraction::new(1, 2));
        assert!(Fraction::new(2, 4) >= Fraction::new(1, 2));
        assert!(Fraction::new(3, 4) > Fraction::new(2, 3));
    }
}
