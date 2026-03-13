use {
    crate::bank::{Bank, EpochInflationRewards},
    solana_account::{Account, AccountSharedData},
    solana_clock::Epoch,
    solana_genesis_config::GenesisConfig,
    solana_pubkey::Pubkey,
    solana_rent::Rent,
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

impl EpochInflationState {
    fn new_from_bank(
        bank: &Bank,
        prev_epoch: Epoch,
        prev_epoch_capitalization: u64,
        additional_validator_rewards: u64,
    ) -> Self {
        let EpochInflationRewards {
            validator_rewards_lamports,
            epoch_duration_in_years: _,
            validator_rate: _,
            foundation_rate: _,
        } = bank.calculate_epoch_inflation_rewards(
            prev_epoch_capitalization + additional_validator_rewards,
            prev_epoch,
        );
        EpochInflationState {
            max_possible_validator_reward: validator_rewards_lamports,
            slots_per_epoch: bank.epoch_schedule.slots_per_epoch,
            epoch: bank.epoch(),
        }
    }
}

/// The state stored in the off curve account used to store metadata for calculating and paying
/// voting rewards.
///
/// Info for the previous and the current epoch is stored.
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct EpochInflationAccountState {
    /// [`EpochInflationState`] for the current epoch.
    pub(super) current: EpochInflationState,
    /// [`EpochInflationState`] for the previous epoch.  This is needed for scenarios when the
    /// reward slot is in a previous epoch relative to the current slot.
    ///
    /// Stored in an `Option` because in the first epoch when Alpenglow is enabled, we will not
    /// have any state for the previous epoch.
    pub(super) prev: Option<EpochInflationState>,
}

impl EpochInflationAccountState {
    /// Returns the deserialized [`Self`] from the accounts in the [`Bank`].
    ///
    /// Returns `None` if the `bank` does not contain the account.
    pub(super) fn new_from_bank(bank: &Bank) -> Option<Self> {
        bank.get_account(&VOTE_REWARD_ACCOUNT_ADDR).map(|acct| {
            acct.deserialize_data()
                .expect("should deserialize as we serialized the data in Self::set_state()")
        })
    }

    /// Serializes and updates [`Self`] into the accounts in the [`Bank`].
    pub(super) fn set_state(&self, bank: &Bank) {
        // TODO: use wincode instead.
        let account_size = bincode::serialized_size(&self).unwrap();
        let lamports = bank
            .rent_collector()
            .rent
            .minimum_balance(account_size.try_into().unwrap());
        let account = AccountSharedData::new_data(lamports, &self, &system_program::ID).unwrap();
        bank.store_account_and_update_capitalization(&VOTE_REWARD_ACCOUNT_ADDR, &account);
    }

    /// Inserts a dummy account for [`Self`] into the `genesis_config` to aid local cluster testing.
    pub(crate) fn insert_into_genesis_config(genesis_config: &mut GenesisConfig) {
        let current = EpochInflationState {
            max_possible_validator_reward: 0,
            slots_per_epoch: genesis_config.epoch_schedule.slots_per_epoch,
            epoch: 0,
        };
        let account = Self {
            current,
            prev: None,
        };
        let account_size = bincode::serialized_size(&account).unwrap();
        let lamports = Rent::default().minimum_balance(account_size.try_into().unwrap());
        let account = Account::new_data(lamports, &account, &system_program::ID).unwrap();
        genesis_config
            .accounts
            .insert(*VOTE_REWARD_ACCOUNT_ADDR, account);
    }

    pub fn remove_from_genesis_config(genesis_config: &mut GenesisConfig) {
        genesis_config.accounts.remove(&VOTE_REWARD_ACCOUNT_ADDR);
    }

    /// Computes a new version of `Self` for `bank.epoch` and serializes it into accounts in the `bank`.
    ///
    /// At the start of a new epoch, over several slots we pay the inflation rewards from the
    /// previous epoch.  This is called Partitioned Epoch Rewards (PER).  As such, the
    /// capitalization keeps increasing in the first slots of the epoch.  Vote rewards are
    /// calculated as a function of the capitalization and we do not want voting in the initial
    /// slots to earn less rewards than voting in the later rewards.  As such this function is
    /// called with [`additional_validator_rewards`] which should be the total rewards that will
    /// be paid by PER and we use the capitalization from the previous epoch plus this value to
    /// compute the vote rewards.
    pub(crate) fn new_epoch_update_account(
        bank: &Bank,
        prev_epoch: Epoch,
        prev_epoch_capitalization: u64,
        additional_validator_rewards: u64,
    ) {
        let prev = Self::new_from_bank(bank).map(|s| s.current);
        let current = EpochInflationState::new_from_bank(
            bank,
            prev_epoch,
            prev_epoch_capitalization,
            additional_validator_rewards,
        );
        let state = Self { prev, current };
        state.set_state(bank);
    }

    /// Returns the [`EpochState`] corresponding to the given `epoch`.
    pub(super) fn get_epoch_state(self, epoch: Epoch) -> Option<EpochInflationState> {
        if self.current.epoch == epoch {
            return Some(self.current);
        }
        if let Some(prev) = self.prev {
            if prev.epoch == epoch {
                return Some(prev);
            }
        }
        None
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
            prev: None,
        };
        let account_size = bincode::serialized_size(&state).unwrap();
        bank.rent_collector()
            .rent
            .minimum_balance(account_size as usize)
            .max(1)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, rand::Rng, solana_genesis_config::GenesisConfig, std::sync::Arc};

    fn get_rand_state() -> EpochInflationAccountState {
        let mut rng = rand::thread_rng();
        EpochInflationAccountState {
            prev: None,
            current: EpochInflationState {
                max_possible_validator_reward: rng.gen(),
                slots_per_epoch: rng.gen(),
                epoch: rng.gen(),
            },
        }
    }

    #[test]
    fn set_state_works() {
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let state = get_rand_state();
        state.set_state(&bank);
        let deserialized = EpochInflationAccountState::new_from_bank(&bank).unwrap();
        assert_eq!(state, deserialized);
    }

    #[test]
    fn new_epoch_update_account_works() {
        let (bank_epoch_0, bank_epoch_1, bank_epoch_2) = {
            let bank_epoch_0 = Arc::new(Bank::new_for_tests(&GenesisConfig::default()));
            let first_slot_in_epoch_1 = bank_epoch_0.epoch_schedule().get_first_slot_in_epoch(1);
            let bank_epoch_1 = Arc::new(Bank::new_from_parent(
                bank_epoch_0.clone(),
                &Pubkey::new_unique(),
                first_slot_in_epoch_1,
            ));
            assert_eq!(bank_epoch_1.epoch(), 1);
            let first_slot_in_epoch_2 = bank_epoch_1.epoch_schedule().get_slots_in_epoch(2);
            let bank_epoch_2 = Arc::new(Bank::new_from_parent(
                bank_epoch_1.clone(),
                &Pubkey::new_unique(),
                first_slot_in_epoch_2,
            ));
            (bank_epoch_0, bank_epoch_1, bank_epoch_2)
        };
        assert_eq!(bank_epoch_0.epoch(), 0);
        assert_eq!(bank_epoch_1.epoch(), 1);
        assert_eq!(bank_epoch_2.epoch(), 2);

        let expected_prev = EpochInflationState::new_from_bank(
            &bank_epoch_1,
            bank_epoch_0.epoch(),
            bank_epoch_0.capitalization(),
            0,
        );
        let expected_current = EpochInflationState::new_from_bank(
            &bank_epoch_2,
            bank_epoch_1.epoch(),
            bank_epoch_1.capitalization(),
            0,
        );
        let EpochInflationAccountState { current, prev } =
            EpochInflationAccountState::new_from_bank(&bank_epoch_2).unwrap();
        assert_eq!(current, expected_current);
        assert_eq!(prev.unwrap(), expected_prev);
    }
}
