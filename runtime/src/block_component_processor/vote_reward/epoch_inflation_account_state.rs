use {
    crate::bank::{Bank, EpochInflationRewards},
    solana_account::AccountSharedData,
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    solana_system_interface::program as system_program,
    std::sync::LazyLock,
};

/// The account address for the off curve account used to store metadata for calculating and
/// paying voting rewards.
static VOTE_REWARD_ACCOUNT_ADDR: LazyLock<Pubkey> = LazyLock::new(|| {
    let (pubkey, _) = Pubkey::find_program_address(
        &[b"vote_reward_account"],
        &agave_feature_set::alpenglow::id(),
    );
    pubkey
});

/// The per epoch info stored in the off curve account.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(super) struct EpochInflationState {
    /// The rewards (in lamports) that would be paid to a validator whose stake is equal to the
    /// capitalization and it voted in every slot in the epoch.  This is also the
    /// epoch inflation.
    pub(super) max_possible_validator_reward: u64,
    /// Number of slots in the epoch.
    pub(super) slots_per_epoch: u64,
    /// The epoch number for this state.
    pub(super) epoch: Epoch,
}

/// The state stored in the off curve account used to store metadata for calculating and paying
/// voting rewards.
///
/// Info for the previous and the current epoch is stored.
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct EpochInflationAccountState {
    pub(super) current: EpochInflationState,
    pub(super) prev: EpochInflationState,
}

impl EpochInflationAccountState {
    fn get_initial_state(current_epoch: Epoch, slots_per_epoch: u64) -> Self {
        let current = EpochInflationState {
            max_possible_validator_reward: 0,
            slots_per_epoch,
            epoch: current_epoch,
        };
        let prev = EpochInflationState {
            max_possible_validator_reward: 0,
            slots_per_epoch,
            epoch: current_epoch.saturating_sub(1),
        };
        Self { current, prev }
    }

    /// Returns the deserialized [`Self`] from the accounts in the [`Bank`].
    pub(super) fn new_from_bank(bank: &Bank) -> Self {
        match bank.get_account(&VOTE_REWARD_ACCOUNT_ADDR) {
            None => {
                // this can happen in the first epoch when the account has not been created yet.
                // we create a dummy state to handle this case with the assumption that this code
                // will become active in an epoch before the epoch in which Alpenglow is activated.
                let state =
                    Self::get_initial_state(bank.epoch(), bank.epoch_schedule.slots_per_epoch);
                state.set_state(bank);
                state
            }
            Some(acct) => {
                // unwrap should be safe as the data being deserialized was serialized by us in
                // [`Self::set_state`].
                acct.deserialize_data().unwrap()
            }
        }
    }

    /// Serializes and updates [`Self`] into the accounts in the [`Bank`].
    pub(super) fn set_state(&self, bank: &Bank) {
        // TODO: use wincode instead.
        let account_size = bincode::serialized_size(&self).unwrap();
        let lamports = bank
            .rent_collector()
            .rent
            .minimum_balance(account_size as usize);
        let account = AccountSharedData::new_data(lamports, &self, &system_program::ID).unwrap();
        bank.store_account_and_update_capitalization(&VOTE_REWARD_ACCOUNT_ADDR, &account);
    }

    /// Calculates and serializes a new version of [`Self`] into the accounts in the [`Bank`]
    /// when  a new epoch starts.   At the start of a new epoch, over several slots we pay the
    /// inflation rewards from the previous epoch.  This is called Partitioned Epoch Rewards
    /// (PER).  As such, the capitalization keeps increasing in the first slots of the epoch.
    /// Vote rewards are calculated as a function of the capitalization and we do not want
    /// voting in the initial slots  to earn less rewards than voting in the later rewards.  As
    /// such this function is called with [`additional_validator_rewards`] which should be the
    /// total rewards that will be paid by PER and we use the capitalization from the previous
    /// epoch plus this value to compute the vote rewards.
    pub(crate) fn new_epoch_update_account(
        bank: &Bank,
        prev_epoch: Epoch,
        prev_epoch_capitalization: u64,
        additional_validator_rewards: u64,
    ) {
        let prev_state = Self::new_from_bank(bank);
        let EpochInflationRewards {
            validator_rewards_lamports,
            epoch_duration_in_years: _,
            validator_rate: _,
            foundation_rate: _,
        } = bank.calculate_epoch_inflation_rewards(
            prev_epoch_capitalization + additional_validator_rewards,
            prev_epoch,
        );
        let new_state = Self {
            prev: prev_state.current,
            current: EpochInflationState {
                max_possible_validator_reward: validator_rewards_lamports,
                slots_per_epoch: bank.epoch_schedule.slots_per_epoch,
                epoch: bank.epoch(),
            },
        };
        new_state.set_state(bank);
    }

    /// Returns the [`EpochState`] corresponding to the given `epoch`.
    pub(super) fn get_epoch_state(self, epoch: Epoch) -> Option<EpochInflationState> {
        if self.current.epoch == epoch {
            Some(self.current)
        } else if self.prev.epoch == epoch {
            Some(self.prev)
        } else {
            None
        }
    }

    /// Returns the amount of lamports needed to store this account.
    #[cfg(test)]
    pub(crate) fn rent_needed_for_account(bank: &Bank) -> u64 {
        let epoch_state = EpochInflationState {
            max_possible_validator_reward: 0,
            slots_per_epoch: 0,
            epoch: 0,
        };
        let state = Self {
            current: epoch_state.clone(),
            prev: epoch_state,
        };
        let account_size = bincode::serialized_size(&state).unwrap();
        bank.rent_collector()
            .rent
            .minimum_balance(account_size as usize)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::bank::EpochInflationRewards, rand::Rng,
        solana_genesis_config::GenesisConfig, std::sync::Arc,
    };

    fn get_rand_state() -> EpochInflationAccountState {
        let mut rng = rand::thread_rng();
        EpochInflationAccountState {
            prev: EpochInflationState {
                max_possible_validator_reward: rng.gen(),
                slots_per_epoch: rng.gen(),
                epoch: rng.gen(),
            },
            current: EpochInflationState {
                max_possible_validator_reward: rng.gen(),
                slots_per_epoch: rng.gen(),
                epoch: rng.gen(),
            },
        }
    }

    #[test]
    fn new_from_bank_works() {
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let expected = EpochInflationAccountState::get_initial_state(
            bank.epoch(),
            bank.epoch_schedule.slots_per_epoch,
        );
        let state = EpochInflationAccountState::new_from_bank(&bank);
        assert_eq!(state, expected);
    }

    #[test]
    fn set_state_works() {
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let state = get_rand_state();
        state.set_state(&bank);
        let deserialized = EpochInflationAccountState::new_from_bank(&bank);
        assert_eq!(state, deserialized);
    }

    #[test]
    fn new_epoch_update_account_works() {
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let first_slot_in_epoch_1 = bank.epoch_schedule().get_first_slot_in_epoch(1);
        let bank =
            Bank::new_from_parent(Arc::new(bank), &Pubkey::new_unique(), first_slot_in_epoch_1);
        let prev_epoch = 1;
        let prev_epoch_capitalization = 12345;
        let additional_validator_rewards = 6789;
        EpochInflationAccountState::new_epoch_update_account(
            &bank,
            prev_epoch,
            prev_epoch_capitalization,
            additional_validator_rewards,
        );
        let EpochInflationAccountState { current, .. } =
            EpochInflationAccountState::new_from_bank(&bank);
        let EpochInflationRewards {
            validator_rewards_lamports,
            ..
        } = bank.calculate_epoch_inflation_rewards(
            prev_epoch_capitalization + additional_validator_rewards,
            prev_epoch,
        );
        assert_eq!(
            current.max_possible_validator_reward,
            validator_rewards_lamports
        );
    }
}
