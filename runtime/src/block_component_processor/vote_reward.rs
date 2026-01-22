use {
    crate::{
        bank::{Bank, PrevEpochInflationRewards},
        epoch_stakes::VersionedEpochStakes,
    },
    solana_account::AccountSharedData,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_rent::Rent,
    solana_system_interface::program as system_program,
    std::sync::LazyLock,
    thiserror::Error,
};

/// The account address for the off curve account used to store metadata for calculating and paying voting rewards.
static VOTE_REWARD_ACCOUNT_ADDR: LazyLock<Pubkey> = LazyLock::new(|| {
    let (pubkey, _) = Pubkey::find_program_address(
        &[b"vote_reward_account"],
        &agave_feature_set::alpenglow::id(),
    );
    pubkey
});

/// The state stored in the off curve account used to store metadata for calculating and paying voting rewards.
#[derive(Deserialize, Serialize)]
pub(crate) struct VoteRewardAccountState {
    /// The capitalization at the end of the previous epoch.
    prev_epoch_capitalization: u64,
    /// The epoch number of the previous epoch.
    prev_epoch: Epoch,
    /// The additional validator rewards according to PER.
    per_validator_rewards: u64,
}

impl VoteRewardAccountState {
    fn get_state(bank: &Bank) -> Self {
        bank.get_account(&VOTE_REWARD_ACCOUNT_ADDR)
            .map(|acct| acct.deserialize_data().unwrap())
            .unwrap()
    }

    fn set_state(&self, bank: &Bank) {
        let account_size = bincode::serialized_size(&self).unwrap();
        let lamports = Rent::default().minimum_balance(account_size as usize);
        let account = AccountSharedData::new_data(lamports, &self, &system_program::ID).unwrap();

        bank.store_account_and_update_capitalization(&VOTE_REWARD_ACCOUNT_ADDR, &account);
    }

    /// Returns the rewards (in lamports) that would be paid to the validator whose stake is equal to the capitalization and it voted in every slot in the epoch.
    fn calculate_total_validator_rewards(bank: &Bank) -> u64 {
        let Self {
            prev_epoch_capitalization,
            prev_epoch,
            per_validator_rewards,
        } = Self::get_state(bank);
        let PrevEpochInflationRewards {
            validator_rewards, ..
        } = bank.calculate_previous_epoch_inflation_rewards(prev_epoch_capitalization, prev_epoch);
        validator_rewards + per_validator_rewards
    }

    /// Updates the state when a new epoch starts.
    pub(crate) fn epoch_update_account(
        bank: &Bank,
        prev_epoch: Epoch,
        prev_epoch_capitalization: u64,
        per_validator_rewards: u64,
    ) {
        let state = Self {
            prev_epoch_capitalization,
            prev_epoch,
            per_validator_rewards,
        };
        state.set_state(bank);
    }
}

/// Different types of errors that can happen when calculating and paying voting reward.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum PayVoteRewardError {
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
) -> Result<(), PayVoteRewardError> {
    let Some((reward_slot, validators)) = reward_slot_and_validators else {
        return Ok(());
    };
    let epoch_stakes = bank
        .epoch_stakes_from_slot(reward_slot)
        .ok_or(PayVoteRewardError::MissingEpochStakes)?;
    let vote_accounts = epoch_stakes.stakes().vote_accounts().as_ref();
    let mut total_leader_reward_lamports = 0u64;
    let total_validator_rewards_lamports =
        VoteRewardAccountState::calculate_total_validator_rewards(bank);
    for validator in validators {
        let (validator_stake_lamports, _) = vote_accounts
            .get(&validator)
            .ok_or(PayVoteRewardError::MissingValidator)?;
        let reward_lamports = calculate_voting_reward(
            bank,
            total_validator_rewards_lamports,
            epoch_stakes,
            *validator_stake_lamports,
        )
        .map_err(|e| PayVoteRewardError::RewardCalculation {
            validator,
            validator_stake_lamports: *validator_stake_lamports,
            error: e,
        })?;
        let validator_reward_lamports = reward_lamports / 2;
        let leader_reward_lamports = reward_lamports - validator_reward_lamports;
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
    total_validator_rewards_lamports: u64,
    epoch_stakes: &VersionedEpochStakes,
    validator_stake_lamports: u64,
) -> Result<u64, FloatError> {
    let slots_per_epoch = bank.epoch_schedule().slots_per_epoch;
    let per_slot_inflation_lamports =
        total_validator_rewards_lamports as f64 / slots_per_epoch as f64;
    let total_stake_lamports = epoch_stakes.total_stake();
    let fractional_stake_lamports = validator_stake_lamports as f64 / total_stake_lamports as f64;
    round(fractional_stake_lamports * per_slot_inflation_lamports)
}
