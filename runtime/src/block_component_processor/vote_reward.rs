use {
    crate::bank::{Bank, PrevEpochInflationRewards},
    solana_account::AccountSharedData,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_rent::Rent,
    solana_system_interface::program as system_program,
    std::sync::LazyLock,
    thiserror::Error,
};
#[cfg(feature = "dev-context-only-utils")]
use {solana_account::Account, solana_genesis_config::GenesisConfig};

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
    /// The rewards (in lamports) that would be paid to a validator whose stake is equal to the capitalization and it voted in every slot in the epoch.
    /// This is also the epoch inflation.
    epoch_validator_rewards_lamports: u64,
}

impl VoteRewardAccountState {
    /// Returns the deserialized [`Self`] from the accounts in the [`Bank`].
    fn get_state(bank: &Bank) -> Self {
        bank.get_account(&VOTE_REWARD_ACCOUNT_ADDR)
            // unwrap assumes that the account exists which should be the case if alpenglow is enabled.
            .unwrap()
            .deserialize_data()
            // unwrap should be safe as the data being deserialized was serialized by us in [`Self::set_state`].
            .unwrap()
    }

    /// Serializes and updates [`Self`] into the accounts in the [`Bank`].
    fn set_state(&self, bank: &Bank) {
        let account_size = bincode::serialized_size(&self).unwrap();
        let lamports = Rent::default().minimum_balance(account_size as usize);
        let account = AccountSharedData::new_data(lamports, &self, &system_program::ID).unwrap();
        bank.store_account_and_update_capitalization(&VOTE_REWARD_ACCOUNT_ADDR, &account);
    }

    /// Calculates and serializes a new version of [`Self`] into the accounts in the [`Bank`] when a new epoch starts.
    ///
    /// At the start of a new epoch, over several slots we pay the inflation rewards from the previous epoch.
    /// This is called Partitioned Epoch Rewards (PER).
    /// As such, the capitalization keeps increasing in the first slots of the epoch.
    /// Vote rewards are calculated as a function of the capitalization and we do not want voting in the initial slots to earn less rewards than voting in the later rewards.
    /// As such this function is called with [`additional_validator_rewards`] which should be the total rewards that will be paid by PER and we use the capitalization from the previous epoch plus this value to compute the vote rewards.
    pub(crate) fn new_epoch_update_account(
        bank: &Bank,
        prev_epoch: Epoch,
        prev_epoch_capitalization: u64,
        additional_validator_rewards: u64,
    ) {
        let PrevEpochInflationRewards {
            validator_rewards,
            prev_epoch_duration_in_years: _,
            validator_rate: _,
            foundation_rate: _,
        } = bank.calculate_previous_epoch_inflation_rewards(prev_epoch_capitalization, prev_epoch);
        let epoch_validator_rewards_lamports = validator_rewards + additional_validator_rewards;
        let state = Self {
            epoch_validator_rewards_lamports,
        };
        state.set_state(bank);
    }

    #[cfg(feature = "dev-context-only-utils")]
    /// Test only function to ensure the state and the account exists.
    pub(crate) fn genesis_insert_account(
        genesis_config: &mut GenesisConfig,
        epoch_validator_rewards_lamports: u64,
    ) {
        let state = Self {
            epoch_validator_rewards_lamports,
        };
        let state_size = bincode::serialized_size(&state).unwrap();
        let lamports = Rent::default().minimum_balance(state_size as usize);
        let account = Account::new_data(lamports, &state, &system_program::ID).unwrap();
        genesis_config
            .accounts
            .insert(*VOTE_REWARD_ACCOUNT_ADDR, account);
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

    let (vote_accounts, total_stake_lamports) = {
        let epoch_stakes = bank
            .epoch_stakes_from_slot(reward_slot)
            .ok_or(PayVoteRewardError::MissingEpochStakes)?;
        (
            epoch_stakes.stakes().vote_accounts().as_ref(),
            epoch_stakes.total_stake(),
        )
    };
    let epoch_validator_rewards_lamports =
        VoteRewardAccountState::get_state(bank).epoch_validator_rewards_lamports;

    let mut total_leader_reward_lamports = 0u64;
    for validator in validators {
        let (validator_stake_lamports, _) = vote_accounts
            .get(&validator)
            .ok_or(PayVoteRewardError::MissingValidator)?;
        let reward_lamports = calculate_voting_reward(
            bank.epoch_schedule().slots_per_epoch,
            epoch_validator_rewards_lamports,
            total_stake_lamports,
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
    slots_per_epoch: u64,
    epoch_validator_rewards_lamports: u64,
    total_stake_lamports: u64,
    validator_stake_lamports: u64,
) -> Result<u64, FloatError> {
    let per_slot_inflation_lamports =
        epoch_validator_rewards_lamports as f64 / slots_per_epoch as f64;
    let fractional_stake_lamports = validator_stake_lamports as f64 / total_stake_lamports as f64;
    round(fractional_stake_lamports * per_slot_inflation_lamports)
}
