use {
    crate::bank::{Bank, EpochInflationRewards},
    log::{error, info},
    solana_account::AccountSharedData,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_system_interface::program as system_program,
    solana_vote::vote_account::VoteAccount,
    solana_vote_interface::state::{VoteStateV4, MAX_EPOCH_CREDITS_HISTORY},
    std::sync::LazyLock,
    thiserror::Error,
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
struct EpochState {
    /// The rewards (in lamports) that would be paid to a validator whose stake is equal to the
    /// capitalization and it voted in every slot in the epoch.  This is also the
    /// epoch inflation.
    epoch_validator_rewards: u64,
    /// Number of slots in the epoch.
    slots_per_epoch: u64,
}

/// The state stored in the off curve account used to store metadata for calculating and paying
/// voting rewards.
///
/// Info for the previous and the current epoch is stored.
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct VoteRewardAccountState {
    current_epoch: Epoch,
    current_epoch_state: EpochState,
    prev_epoch: Epoch,
    prev_epoch_state: EpochState,
}

impl VoteRewardAccountState {
    /// Returns the deserialized [`Self`] from the accounts in the [`Bank`].
    fn new_from_bank(bank: &Bank) -> Self {
        match bank.get_account(&VOTE_REWARD_ACCOUNT_ADDR) {
            None => {
                // this can happen in the first epoch when the account has not been created yet.
                // we create a dummy state to handle this case with the assumption that this code
                // will become active in an epoch before the epoch in which Alpenglow is activated.
                let epoch_state = EpochState {
                    epoch_validator_rewards: 0,
                    slots_per_epoch: bank.epoch_schedule.slots_per_epoch,
                };
                let state = Self {
                    current_epoch: bank.epoch(),
                    current_epoch_state: epoch_state.clone(),
                    prev_epoch: bank.epoch().checked_sub(1).unwrap(),
                    prev_epoch_state: epoch_state,
                };
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
    fn set_state(&self, bank: &Bank) {
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
    /// inflation rewards from the  previous epoch.  This is called Partitioned Epoch Rewards
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
            prev_epoch: prev_state.current_epoch,
            prev_epoch_state: prev_state.current_epoch_state,
            current_epoch: bank.epoch(),
            current_epoch_state: EpochState {
                epoch_validator_rewards: validator_rewards_lamports,
                slots_per_epoch: bank.epoch_schedule.slots_per_epoch,
            },
        };
        new_state.set_state(bank);
    }

    /// Returns the [`EpochState`] corresponding to the given `epoch`.
    fn get_epoch_state(self, epoch: Epoch) -> Option<EpochState> {
        if self.current_epoch == epoch {
            Some(self.current_epoch_state)
        } else if self.prev_epoch == epoch {
            Some(self.prev_epoch_state)
        } else {
            None
        }
    }

    /// Returns the amount of lamports needed to store this account.
    #[cfg(test)]
    pub(crate) fn rent_needed_for_account(bank: &Bank) -> u64 {
        let epoch_state = EpochState {
            epoch_validator_rewards: 0,
            slots_per_epoch: 0,
        };
        let state = Self {
            prev_epoch: 0,
            prev_epoch_state: epoch_state.clone(),
            current_epoch: 0,
            current_epoch_state: epoch_state,
        };
        let account_size = bincode::serialized_size(&state).unwrap();
        bank.rent_collector()
            .rent
            .minimum_balance(account_size as usize)
    }
}

/// Different types of errors that can happen when calculating and paying voting reward.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum PayVoteRewardError {
    #[error("missing epoch stakes for reward_slot {reward_slot} in current_slot {current_slot}")]
    MissingEpochStakes {
        reward_slot: Slot,
        current_slot: Slot,
    },
    #[error("missing reward slot validator")]
    MissingRewardSlotValidator {
        pubkey: Pubkey,
        reward_slot: Slot,
        current_slot: Slot,
    },
    #[error(
        "missing validator stake info for reward epoch {reward_epoch} in current_slot \
         {current_slot}"
    )]
    NoEpochValidatorStake {
        reward_epoch: Epoch,
        current_slot: Slot,
    },
}

/// Calculates and pays voting reward.
///
/// This is a NOP if [`reward_slot_and_validators`] is [`None`].
///
/// The reward slot is in the past relative to the current slot and hence might be in a different
/// epoch than the current epoch and may have different validator sets and stakes, etc.
/// This function must compute rewards using the stakes in the reward slot and pay them using the
/// stakes in the current slot.
pub(super) fn calculate_and_pay_voting_reward(
    bank: &Bank,
    reward_slot_and_validators: Option<(Slot, Vec<Pubkey>)>,
) -> Result<(), PayVoteRewardError> {
    let Some((reward_slot, validators_to_reward)) = reward_slot_and_validators else {
        return Ok(());
    };

    let current_slot = bank.slot();
    let (reward_slot_accounts, reward_slot_total_stake) = {
        let epoch_stakes = bank.epoch_stakes_from_slot(reward_slot).ok_or(
            PayVoteRewardError::MissingEpochStakes {
                reward_slot,
                current_slot,
            },
        )?;
        (
            epoch_stakes.stakes().vote_accounts().as_ref(),
            epoch_stakes.total_stake(),
        )
    };

    // This assumes that if the epoch_schedule ever changes, the new schedule will maintain correct
    // info about older slots as well.
    let reward_epoch = bank.epoch_schedule.get_epoch(reward_slot);
    let epoch_state = VoteRewardAccountState::new_from_bank(bank)
        .get_epoch_state(reward_epoch)
        .ok_or(PayVoteRewardError::NoEpochValidatorStake {
            reward_epoch,
            current_slot,
        })?;

    let current_slot_accounts = bank.vote_accounts();
    let mut accounts_to_store = Vec::with_capacity(validators_to_reward.len());
    let mut total_leader_reward = 0u64;
    let current_slot_leader = *bank.collector_id();
    let current_epoch = bank.epoch();
    for validator_to_reward in validators_to_reward {
        let (reward_slot_validator_stake, _) = reward_slot_accounts
            .get(&validator_to_reward)
            .ok_or(PayVoteRewardError::MissingRewardSlotValidator {
                pubkey: validator_to_reward,
                reward_slot,
                current_slot,
            })?;
        let (validator_reward, add_leader_reward) = calculate_reward(
            &epoch_state,
            reward_slot_total_stake,
            *reward_slot_validator_stake,
        );
        total_leader_reward = total_leader_reward.saturating_add(add_leader_reward);

        if validator_to_reward == current_slot_leader {
            // current slot's leader.  We haven't finished calculating its reward yet.
            // Will be paid at the end.
            total_leader_reward = total_leader_reward.saturating_add(validator_reward);
        } else {
            let Some((_, current_slot_account)) = current_slot_accounts.get(&validator_to_reward)
            else {
                info!(
                    "validator {validator_to_reward} was present for reward_slot {reward_slot} \
                     but is absent for current_slot {current_slot}"
                );
                continue;
            };
            pay_reward(
                current_epoch,
                validator_to_reward,
                current_slot_account,
                validator_reward,
                &mut accounts_to_store,
            );
        }
    }
    match current_slot_accounts.get(&current_slot_leader) {
        Some((_, leader_account)) => {
            pay_reward(
                current_epoch,
                current_slot_leader,
                leader_account,
                total_leader_reward,
                &mut accounts_to_store,
            );
        }
        None => {
            info!("Current slot {current_slot}'s leader's account {current_slot_leader} not found")
        }
    }
    bank.store_accounts((current_slot, accounts_to_store.as_slice()));
    Ok(())
}

/// Computes the voting reward in Lamports.
fn calculate_reward(
    epoch_state: &EpochState,
    total_stake_lamports: u64,
    validator_stake_lamports: u64,
) -> (u64, u64) {
    // Rewards are computed as following:
    // per_slot_inflation = epoch_validator_rewards_lamports / slots_per_epoch
    // fractional_stake = validator_stake / total_stake_lamports
    // rewards = fractional_stake * per_slot_inflation
    //
    // The code below is equivalent but changes the order of operations to maintain precision

    let numerator = epoch_state.epoch_validator_rewards as u128 * validator_stake_lamports as u128;
    let denominator = epoch_state.slots_per_epoch as u128 * total_stake_lamports as u128;

    // SAFETY: the result should fit in u64 because we do not expect the inflation in a single
    // epoch to exceed u64::MAX.
    let reward_lamports: u64 = (numerator / denominator).try_into().unwrap();
    // As per the Alpenglow SIMD, the rewards are split equally between the validators and the leader.
    let validator_reward_lamports = reward_lamports / 2;
    let leader_reward_lamports = reward_lamports - validator_reward_lamports;
    (validator_reward_lamports, leader_reward_lamports)
}

fn pay_reward(
    epoch: Epoch,
    pubkey: Pubkey,
    account: &VoteAccount,
    reward: u64,
    accounts_to_store: &mut Vec<(Pubkey, AccountSharedData)>,
) {
    let data = account.account().data_clone();
    // TODO (akhi): this is a stop gap till we upstream.  We want to use the `VoteStateHandler` API.
    let mut vote_state: VoteStateV4 = bincode::deserialize(&data).unwrap();
    increment_credits(&mut vote_state, epoch, reward);
    accounts_to_store.push((
        pubkey,
        AccountSharedData::new_data(account.lamports(), &vote_state, account.owner()).unwrap(),
    ));
}

// TODO (akhi): this is a stop gap till we upstream.  We want to use the `VoteStateHandler` API.
fn increment_credits(vote_state: &mut VoteStateV4, epoch: Epoch, credits: u64) {
    // never seen a credit
    if vote_state.epoch_credits.is_empty() {
        vote_state.epoch_credits.push((epoch, 0, 0));
    } else if epoch != vote_state.epoch_credits.last().unwrap().0 {
        let (_, credits, prev_credits) = *vote_state.epoch_credits.last().unwrap();

        if credits != prev_credits {
            // if credits were earned previous epoch
            // append entry at end of list for the new epoch
            vote_state.epoch_credits.push((epoch, credits, credits));
        } else {
            // else just move the current epoch
            vote_state.epoch_credits.last_mut().unwrap().0 = epoch;
        }

        // Remove too old epoch_credits
        if vote_state.epoch_credits.len() > MAX_EPOCH_CREDITS_HISTORY {
            vote_state.epoch_credits.remove(0);
        }
    }

    vote_state.epoch_credits.last_mut().unwrap().1 = vote_state
        .epoch_credits
        .last()
        .unwrap()
        .1
        .saturating_add(credits);
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_genesis_config::GenesisConfig, solana_native_token::LAMPORTS_PER_SOL,
        std::sync::Arc,
    };

    #[test]
    fn calculate_voting_reward_does_not_panic() {
        // the current circulating supply is about 566M.  The most extreme numbers are when all of
        // it is staked by a single validator.
        let circulating_supply = 566_000_000 * LAMPORTS_PER_SOL;

        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let EpochInflationRewards {
            validator_rewards_lamports,
            ..
        } = bank.calculate_epoch_inflation_rewards(circulating_supply, 1);

        let epoch_state = EpochState {
            slots_per_epoch: bank.epoch_schedule.slots_per_epoch,
            epoch_validator_rewards: validator_rewards_lamports,
        };

        calculate_reward(&epoch_state, circulating_supply, circulating_supply);
    }

    #[test]
    fn serialization_works() {
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let state = VoteRewardAccountState {
            prev_epoch: 321,
            prev_epoch_state: EpochState {
                epoch_validator_rewards: 23432,
                slots_per_epoch: 2532,
            },
            current_epoch: 3645,
            current_epoch_state: EpochState {
                epoch_validator_rewards: 76463,
                slots_per_epoch: 2346,
            },
        };
        state.set_state(&bank);
        let deserialized = VoteRewardAccountState::new_from_bank(&bank);
        assert_eq!(state, deserialized);
    }

    #[test]
    fn epoch_update_works() {
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let first_slot_in_epoch_1 = bank.epoch_schedule().get_first_slot_in_epoch(1);
        let bank =
            Bank::new_from_parent(Arc::new(bank), &Pubkey::new_unique(), first_slot_in_epoch_1);
        let prev_epoch = 1;
        let prev_epoch_capitalization = 12345;
        let additional_validator_rewards = 6789;
        VoteRewardAccountState::new_epoch_update_account(
            &bank,
            prev_epoch,
            prev_epoch_capitalization,
            additional_validator_rewards,
        );
        let VoteRewardAccountState {
            current_epoch_state,
            ..
        } = VoteRewardAccountState::new_from_bank(&bank);
        let EpochInflationRewards {
            validator_rewards_lamports,
            ..
        } = bank.calculate_epoch_inflation_rewards(
            prev_epoch_capitalization + additional_validator_rewards,
            prev_epoch,
        );
        assert_eq!(
            current_epoch_state.epoch_validator_rewards,
            validator_rewards_lamports
        );
    }
}
