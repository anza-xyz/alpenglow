use {
    crate::{bank::Bank, epoch_stakes::VersionedEpochStakes},
    epoch_inflation_account_state::{EpochInflationAccountState, EpochInflationState},
    log::{error, info},
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_vote::vote_account::VoteAccount,
    solana_vote_interface::state::{VoteStateV4, VoteStateVersions, MAX_EPOCH_CREDITS_HISTORY},
    thiserror::Error,
};

pub mod epoch_inflation_account_state;

/// Different types of errors that can happen when calculating and paying voting reward.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum PayVoteRewardError {
    #[error("missing EpochInflationAccountState for current slot {current_slot}")]
    MissingEpochInflationAccountState { current_slot: Slot },
    #[error("missing epoch stakes for reward_slot {reward_slot} in current_slot {current_slot}")]
    MissingEpochStakes {
        reward_slot: Slot,
        current_slot: Slot,
    },
    #[error(
        "validator {pubkey} missing in current slot {current_slot} for reward slot {reward_slot}"
    )]
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
    let (reward_slot_accounts, reward_slot_total_stake, current_slot_leader_vote_pubkey) = {
        let epoch_stakes = bank.epoch_stakes_from_slot(reward_slot).ok_or(
            PayVoteRewardError::MissingEpochStakes {
                reward_slot,
                current_slot,
            },
        )?;
        let current_slot_leader_vote_pubkey =
            convert_node_pubkey_to_vote_pubkey(epoch_stakes, *bank.collector_id())
                .inspect_err(|e| {
                    info!(
                        "Converting current slot leader's node pubkey to vote pubkey failed with \
                         {e}.  It will not be paid"
                    );
                })
                .ok();
        (
            epoch_stakes.stakes().vote_accounts().as_ref(),
            epoch_stakes.total_stake(),
            current_slot_leader_vote_pubkey,
        )
    };

    // This assumes that if the epoch_schedule ever changes, the new schedule will maintain correct
    // info about older slots as well.
    let reward_epoch = bank.epoch_schedule.get_epoch(reward_slot);
    let epoch_state = {
        let epoch_inflation_account_state = EpochInflationAccountState::new_from_bank(bank);
        // This function should only be called after alpenglow is active and the slot in the the epoch
        // that activated Alpenglow should have created the account.
        debug_assert!(epoch_inflation_account_state.is_some());
        epoch_inflation_account_state
            .ok_or(PayVoteRewardError::MissingEpochInflationAccountState { current_slot })?
            .get_epoch_state(reward_epoch)
            .ok_or(PayVoteRewardError::NoEpochValidatorStake {
                reward_epoch,
                current_slot,
            })?
    };

    let current_vote_accounts = bank.vote_accounts();
    // Adding 1 to capacity in case the current leader was not in the aggregate and paying it triggers a reallocation.
    let mut paid_vote_accounts = Vec::with_capacity(validators_to_reward.len().saturating_add(1));
    let mut total_leader_reward = 0u64;
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

        if Some(validator_to_reward) == current_slot_leader_vote_pubkey {
            // current slot's leader.  We haven't finished calculating its reward yet.
            // Will be paid at the end.
            total_leader_reward = total_leader_reward.saturating_add(validator_reward);
        } else {
            let Some((_, current_slot_account)) = current_vote_accounts.get(&validator_to_reward)
            else {
                info!(
                    "validator {validator_to_reward} was present for reward_slot {reward_slot} \
                     but is absent for current_slot {current_slot}"
                );
                continue;
            };
            if let Some(account_data) =
                pay_reward(current_epoch, current_slot_account, validator_reward)
            {
                paid_vote_accounts.push((validator_to_reward, account_data));
            }
        }
    }
    if let Some(pubkey) = current_slot_leader_vote_pubkey {
        match current_vote_accounts.get(&pubkey) {
            Some((_, leader_account)) => {
                if let Some(account_data) =
                    pay_reward(current_epoch, leader_account, total_leader_reward)
                {
                    paid_vote_accounts.push((pubkey, account_data));
                }
            }
            None => {
                info!(
                    "Current slot {current_slot}'s leader's account {pubkey} not found.  It will \
                     not be paid."
                )
            }
        }
    }

    bank.store_accounts((current_slot, paid_vote_accounts.as_slice()));
    Ok(())
}

/// Computes the voting reward in Lamports.
///
/// Returns `(validator rewards, leader rewards)`.
fn calculate_reward(
    epoch_state: &EpochInflationState,
    total_stake_lamports: u64,
    validator_stake_lamports: u64,
) -> (u64, u64) {
    // Rewards are computed as following:
    // per_slot_inflation = epoch_validator_rewards_lamports / slots_per_epoch
    // fractional_stake = validator_stake / total_stake_lamports
    // rewards = fractional_stake * per_slot_inflation
    //
    // The code below is equivalent but changes the order of operations to maintain precision

    let numerator =
        epoch_state.max_possible_validator_reward as u128 * validator_stake_lamports as u128;
    let denominator = epoch_state.slots_per_epoch as u128 * total_stake_lamports as u128;

    // SAFETY: the result should fit in u64 because we do not expect the inflation in a single
    // epoch to exceed u64::MAX.
    let reward_lamports: u64 = (numerator / denominator).try_into().unwrap();
    // As per the Alpenglow SIMD, the rewards are split equally between the validators and the leader.
    let validator_reward_lamports = reward_lamports / 2;
    let leader_reward_lamports = reward_lamports - validator_reward_lamports;
    (validator_reward_lamports, leader_reward_lamports)
}

/// Pays `reward` to `account` in `current_epoch`.
///
/// TODO: this is using VoteStateV4 explicitly.  When we upstream, we will use VoteStateHandle API.
fn pay_reward(
    current_epoch: Epoch,
    account: &VoteAccount,
    reward: u64,
) -> Option<AccountSharedData> {
    let data = account.account().data();
    let Ok(vote_state_versions) = bincode::deserialize(data) else {
        return None;
    };
    match vote_state_versions {
        VoteStateVersions::V4(mut vote_state) => {
            increment_credits(&mut vote_state, current_epoch, reward);
            let mut paid_account = AccountSharedData::new(
                account.lamports(),
                account.account().data().len(),
                account.owner(),
            );
            paid_account
                .serialize_data(&VoteStateVersions::V4(vote_state))
                .ok()?;
            Some(paid_account)
        }
        _ => None,
    }
}

/// We store rewards as credits in the current vote state.
///
/// TODO: this is using VoteStateV4 explicitly.  When we upstream, we will use VoteStateHandle API.
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

#[derive(Debug, Error)]
enum ConvertError {
    #[error("failed to find node_vote_accounts for {node_pubkey}")]
    NodeVoteAccountsNotFound { node_pubkey: Pubkey },
    #[error(
        "node pubkey should map to exactly {expected} vote account but maps to {got} accounts"
    )]
    VoteAccountsLenMismatch { expected: usize, got: usize },
}

/// Converts a `node_pubkey` to a `vote_pubkey`.
fn convert_node_pubkey_to_vote_pubkey(
    epoch_stakes: &VersionedEpochStakes,
    node_pubkey: Pubkey,
) -> Result<Pubkey, ConvertError> {
    let map = epoch_stakes.node_id_to_vote_accounts();
    let Some(node_vote_accounts) = map.get(&node_pubkey) else {
        return Err(ConvertError::NodeVoteAccountsNotFound { node_pubkey });
    };
    if node_vote_accounts.vote_accounts.len() != 1 {
        return Err(ConvertError::VoteAccountsLenMismatch {
            expected: 1,
            got: node_vote_accounts.vote_accounts.len(),
        });
    }
    Ok(node_vote_accounts.vote_accounts[0])
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            bank::EpochInflationRewards,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts, ValidatorVoteKeypairs,
            },
        },
        agave_votor_messages::reward_certificate::NUM_SLOTS_FOR_REWARD,
        rand::seq::SliceRandom,
        solana_account::ReadableAccount,
        solana_epoch_schedule::EpochSchedule,
        solana_genesis_config::GenesisConfig,
        solana_native_token::LAMPORTS_PER_SOL,
        solana_rent::Rent,
        solana_signer::Signer,
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

        let epoch_state = EpochInflationState {
            slots_per_epoch: bank.epoch_schedule.slots_per_epoch,
            max_possible_validator_reward: validator_rewards_lamports,
            epoch: 1234,
        };

        calculate_reward(&epoch_state, circulating_supply, circulating_supply);
    }

    #[test]
    fn increment_credits_works() {
        let mut vote_state = VoteStateV4::default();
        let epoch = 1234;
        let credits = 543432;
        increment_credits(&mut vote_state, epoch, credits);
        assert_eq!(credits, vote_state.epoch_credits.last().unwrap().1);
    }

    #[test]
    fn pay_reward_works() {
        let account = VoteAccount::new_random_alpenglow();
        let epoch = 1234;
        let reward = 3453423;
        let account_shared_data = pay_reward(epoch, &account, reward).unwrap();
        let vote_state_versions: VoteStateVersions =
            bincode::deserialize(&account_shared_data.data_clone()).unwrap();
        let VoteStateVersions::V4(vote_state) = vote_state_versions else {
            panic!("unexpected state version: {vote_state_versions:?}");
        };
        assert_eq!(reward, vote_state.epoch_credits.last().unwrap().1);
    }

    fn calc_reward_for_test(
        prev_bank: &Bank,
        bank: &Bank,
        total_stake: u64,
        stake_voted: u64,
    ) -> u64 {
        let EpochInflationRewards {
            validator_rewards_lamports: epoch_inflation,
            epoch_duration_in_years: _,
            validator_rate: _,
            foundation_rate: _,
        } = bank.calculate_epoch_inflation_rewards(prev_bank.capitalization(), prev_bank.epoch());

        let numerator = epoch_inflation as u128 * stake_voted as u128;
        let denominator = bank.epoch_schedule.slots_per_epoch as u128 * total_stake as u128;
        let reward: u64 = (numerator / denominator).try_into().unwrap();
        reward / 2
    }

    #[test]
    fn calculate_and_pay_works() {
        let num_validators = 100;
        let per_validator_stake = LAMPORTS_PER_SOL * 100;
        let num_validators_to_reward = 10;

        let validator_keypairs = (0..num_validators)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let mut genesis_config = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![per_validator_stake; validator_keypairs.len()],
        )
        .genesis_config;
        genesis_config.epoch_schedule = EpochSchedule::without_warmup();
        genesis_config.rent = Rent::default();

        let validator_keypairs_to_reward = validator_keypairs
            .choose_multiple(&mut rand::thread_rng(), num_validators_to_reward as usize)
            .collect::<Vec<_>>();

        let validator_pubkeys_to_reward = validator_keypairs_to_reward
            .iter()
            .map(|v| v.vote_keypair.pubkey())
            .collect::<Vec<_>>();
        let leader_vote_pubkey = validator_keypairs_to_reward[0].vote_keypair.pubkey();
        let leader_node_pubkey = validator_keypairs_to_reward[0].node_keypair.pubkey();

        let prev_bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let current_slot = prev_bank
            .epoch_schedule
            .get_first_slot_in_epoch(prev_bank.epoch() + 1)
            + NUM_SLOTS_FOR_REWARD;
        let bank = Bank::new_from_parent(prev_bank.clone(), &leader_node_pubkey, current_slot);
        let reward_slot = current_slot - NUM_SLOTS_FOR_REWARD;

        calculate_and_pay_voting_reward(
            &bank,
            Some((reward_slot, validator_pubkeys_to_reward.clone())),
        )
        .unwrap();

        let vote_accounts = bank.vote_accounts();
        let rewards = validator_pubkeys_to_reward
            .iter()
            .map(|validator| {
                let (_, vote_account) = vote_accounts.get(validator).unwrap();
                let data = vote_account.account().data();
                let vote_state_versions = bincode::deserialize(data).unwrap();
                let VoteStateVersions::V4(vote_state) = vote_state_versions else {
                    panic!();
                };
                assert_eq!(vote_state.epoch_credits.len(), 1);
                let got_reward = vote_state.epoch_credits[0].1;
                let total_stake = bank
                    .epoch_stakes_from_slot(reward_slot)
                    .unwrap()
                    .total_stake();
                let expected_validator_reward =
                    calc_reward_for_test(&prev_bank, &bank, total_stake, per_validator_stake);
                if *validator != leader_vote_pubkey {
                    assert_eq!(got_reward, expected_validator_reward);
                }
                got_reward
            })
            .collect::<Vec<_>>();
        let expected_leader_reward = rewards.last().unwrap()
            * validator_pubkeys_to_reward.len() as u64
            + rewards.last().unwrap();
        assert_eq!(expected_leader_reward, rewards[0]);
    }
}
