use super::{Stake, SUPERMAJORITY};

pub fn super_majority_threshold(total_stake: Stake) -> f64 {
    SUPERMAJORITY * total_stake as f64
}

pub fn stake_reached_super_majority(stake: Stake, total_stake: Stake) -> bool {
    stake as f64 > super_majority_threshold(total_stake)
}
