use {
    crate::{bank::Bank, epoch_stakes::VersionedEpochStakes},
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    thiserror::Error,
};

/// Different types of errors that can happen when calculating and paying voting reward.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum Error {
    #[error("missing epoch stakes")]
    MissingEpochStakes,
    #[error("missing validator")]
    MissingValidator,
    #[error(
        "calculating reward for validator {validator} with stake {validator_stake_lamports} \
         failed with {error}"
    )]
    RewardCalculation {
        validator: Pubkey,
        validator_stake_lamports: u64,
        error: FloatError,
    },
}

/// Calculates and pays voting reward.
///
/// This is a NOP if [`reward_slot_and_validators`] is [`None`].
///
/// TODO: currently this function is just calculating rewards and not actually paying them.
pub(super) fn calculate_and_pay_voting_reward(
    bank: &Bank,
    reward_slot_and_validators: Option<(Slot, Vec<Pubkey>)>,
) -> Result<(), Error> {
    let Some((reward_slot, validators)) = reward_slot_and_validators else {
        return Ok(());
    };
    let epoch_stakes = bank
        .epoch_stakes_from_slot(reward_slot)
        .ok_or(Error::MissingEpochStakes)?;
    let vote_accounts = epoch_stakes.stakes().vote_accounts().as_ref();
    let mut total_leader_reward_lamports = 0u64;
    for validator in validators {
        let (validator_stake_lamports, _) = vote_accounts
            .get(&validator)
            .ok_or(Error::MissingValidator)?;
        let reward_lamports =
            calculate_voting_reward(bank, epoch_stakes, *validator_stake_lamports).map_err(
                |e| Error::RewardCalculation {
                    validator,
                    validator_stake_lamports: *validator_stake_lamports,
                    error: e,
                },
            )?;
        let _validator_reward_lamports = reward_lamports / 2;
        let leader_reward_lamports = reward_lamports / 2;
        total_leader_reward_lamports =
            total_leader_reward_lamports.saturating_add(leader_reward_lamports);
    }
    Ok(())
}

/// Different types of errors possible when converting a [`f64`] to a [`u64`].
#[derive(Debug, PartialEq, Eq, Error)]
pub enum FloatError {
    #[error("input {0} is inifinite")]
    Infinite(String),
    #[error("input {0} overflowed")]
    Overflow(String),
}

/// Converts a [`f64`] into a [`u64`].
fn round(input: f64) -> Result<u64, FloatError> {
    if !input.is_finite() {
        return Err(FloatError::Infinite(input.to_string()));
    }
    if input < 0.0 {
        return Err(FloatError::Overflow(input.to_string()));
    }
    if input > u64::MAX as f64 {
        return Err(FloatError::Overflow(input.to_string()));
    }
    Ok(input.round() as u64)
}

/// Computes the voting reward in Lamports.
fn calculate_voting_reward(
    bank: &Bank,
    epoch_stakes: &VersionedEpochStakes,
    validator_stake_lamports: u64,
) -> Result<u64, FloatError> {
    let total_supply_lamports = bank.capitalization();
    let total_stake_lamports = epoch_stakes.total_stake();
    let yearly_inflation = bank.inflation().total(bank.slot_in_year_for_inflation());
    let slots_per_epoch = bank.epoch_schedule().slots_per_epoch;
    let slots_per_year = bank.slots_per_year();

    debug_assert!(validator_stake_lamports <= total_stake_lamports);
    debug_assert!(total_stake_lamports <= total_supply_lamports);
    debug_assert!(slots_per_epoch as f64 <= slots_per_year);

    let epoch_inflation = (1f64 + yearly_inflation).powf(slots_per_year) - 1f64;
    let per_slot_inflation =
        total_supply_lamports as f64 * epoch_inflation / slots_per_epoch as f64;
    let fractional_stake = validator_stake_lamports as f64 / total_stake_lamports as f64;
    round(fractional_stake * per_slot_inflation)
}
