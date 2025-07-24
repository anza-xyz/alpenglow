//! Accounting related operations on the Vote Account

use bytemuck::{Pod, PodInOption, Zeroable, ZeroableInOption};
use solana_program::account_info::AccountInfo;
use solana_program::clock::Clock;
use solana_program::clock::Epoch;
use solana_program::clock::Slot;
use solana_program::epoch_schedule::EpochSchedule;
use solana_program::program_error::ProgramError;
use solana_program::pubkey::Pubkey;
use solana_program::rent::Rent;
use spl_pod::bytemuck::pod_from_bytes_mut;
use spl_pod::primitives::PodU64;

use crate::alpenglow::error::VoteError;
use crate::alpenglow::instruction::AuthorityType;
use crate::alpenglow::state::{PodEpoch, VoteState};

/// Authorized Signer for vote instructions
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable, Default, PartialEq)]
pub struct AuthorizedVoter {
    pub(crate) epoch: PodEpoch,
    pub(crate) voter: Pubkey,
}

impl AuthorizedVoter {
    /// Create a new authorized voter for `epoch`
    pub fn new(epoch: Epoch, voter: Pubkey) -> Self {
        Self {
            epoch: PodEpoch::from(epoch),
            voter,
        }
    }

    /// Get the authorization epoch
    pub fn epoch(&self) -> Epoch {
        Epoch::from(self.epoch)
    }

    /// Get the voter that is authorized
    pub fn voter(&self) -> &Pubkey {
        &self.voter
    }

    /// Set the authorization epoch
    pub fn set_epoch(&mut self, epoch: Epoch) {
        self.epoch = PodEpoch::from(epoch);
    }

    /// Set the voter that is authorized
    pub fn set_voter(&mut self, voter: Pubkey) {
        self.voter = voter;
    }
}

// UNSAFE: we require that `epoch > 0` so this is safe
unsafe impl ZeroableInOption for AuthorizedVoter {}
unsafe impl PodInOption for AuthorizedVoter {}

/// The credits information for an epoch
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable, Default, PartialEq)]
pub struct EpochCredit {
    pub(crate) epoch: PodEpoch,
    pub(crate) credits: PodU64,
    pub(crate) prev_credits: PodU64,
}

impl EpochCredit {
    /// Create a new epoch credit
    pub fn new(epoch: Epoch, credits: u64, prev_credits: u64) -> Self {
        Self {
            epoch: PodEpoch::from(epoch),
            credits: PodU64::from(credits),
            prev_credits: PodU64::from(prev_credits),
        }
    }

    /// Get epoch in which credits were earned
    pub fn epoch(&self) -> u64 {
        u64::from(self.epoch)
    }

    /// Get the credits earned
    pub fn credits(&self) -> u64 {
        u64::from(self.credits)
    }

    /// Get the credits earned in the previous epoch
    pub fn prev_credits(&self) -> u64 {
        u64::from(self.prev_credits)
    }

    /// Set epoch in which credits were earned
    pub fn set_epoch(&mut self, epoch: Epoch) {
        self.epoch = PodEpoch::from(epoch);
    }

    /// Set the credits earned
    pub fn set_credits(&mut self, credits: u64) {
        self.credits = PodU64::from(credits);
    }

    /// Set the credits earned in the previous epoch
    pub fn set_prev_credits(&mut self, prev_credits: u64) {
        self.prev_credits = PodU64::from(prev_credits);
    }
}

/// Authorize the given pubkey to withdraw or sign votes. This may be called multiple times,
/// but will implicitly withdraw authorization from the previously authorized key
pub(crate) fn authorize(
    vote_account: &AccountInfo,
    new_authority: &Pubkey,
    vote_authorize: AuthorityType,
    authority: &Pubkey,
    clock: &Clock,
) -> Result<(), ProgramError> {
    let mut buffer = vote_account.try_borrow_mut_data()?;
    let vote_state = pod_from_bytes_mut::<VoteState>(&mut buffer)?;

    match vote_authorize {
        AuthorityType::Voter => {
            // Current authorized withdrawer or voter must match
            if vote_state.authorized_withdrawer != *authority
                && vote_state.authorized_voter.voter != *authority
            {
                return Err(ProgramError::MissingRequiredSignature);
            }

            let epoch_in_effect = clock
                .leader_schedule_epoch
                .checked_add(1)
                .ok_or(ProgramError::InvalidInstructionData)?;
            // Overwrite the next authorized voter
            vote_state.next_authorized_voter = Some(AuthorizedVoter {
                epoch: PodU64::from(epoch_in_effect),
                voter: *new_authority,
            });
        }
        AuthorityType::Withdrawer => {
            // Current authorized withdrawer must match
            if vote_state.authorized_withdrawer != *authority {
                return Err(ProgramError::MissingRequiredSignature);
            }
            vote_state.authorized_withdrawer = *new_authority;
        }
    }
    Ok(())
}

pub(crate) fn withdraw(
    vote_account: &AccountInfo,
    recipient: &AccountInfo,
    lamports: u64,
    withdraw_pubkey: &Pubkey,
    rent_sysvar: &Rent,
    clock: &Clock,
) -> Result<(), ProgramError> {
    let vote_state = vote_account.data.borrow();
    let vote_state = bytemuck::from_bytes::<VoteState>(&vote_state);

    if vote_state.authorized_withdrawer != *withdraw_pubkey {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let remaining_balance = vote_account
        .try_lamports()?
        .checked_sub(lamports)
        .ok_or(ProgramError::InsufficientFunds)?;

    if remaining_balance == 0 {
        let last_epoch_with_credits = u64::from(vote_state.epoch_credits.epoch);
        let current_epoch = clock.epoch;
        // if current_epoch - last_epoch_with_credits < 2 then the validator has received credits
        // either in the current epoch or the previous epoch. If it's >= 2 then it has been at least
        // one full epoch since the validator has received credits.
        let reject_active_vote_account_close =
            current_epoch.saturating_sub(last_epoch_with_credits) < 2;

        if reject_active_vote_account_close {
            return Err(VoteError::ActiveVoteAccountClose.into());
        } else {
            // Deinitialize upon zero-balance
            VoteState::set_vote_account_state(vote_account, &VoteState::default())?;
        }
    } else {
        let min_rent_exempt_balance = rent_sysvar.minimum_balance(vote_account.data_len());
        if remaining_balance < min_rent_exempt_balance {
            return Err(ProgramError::InsufficientFunds);
        }
    }

    let mut vote_account_lamports = vote_account.try_borrow_mut_lamports()?;

    **vote_account_lamports = vote_account_lamports
        .checked_sub(lamports)
        .ok_or(ProgramError::InsufficientFunds)?;

    let mut recipient_lamports = recipient.try_borrow_mut_lamports()?;

    **recipient_lamports = recipient_lamports
        .checked_add(lamports)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    Ok(())
}

pub(crate) fn update_validator_identity(
    vote_account: &AccountInfo,
    new_node_pubkey: &Pubkey,
    withdraw_pubkey: &Pubkey,
) -> Result<(), ProgramError> {
    let mut buffer = vote_account.try_borrow_mut_data()?;
    let vote_state = pod_from_bytes_mut::<VoteState>(&mut buffer)?;

    if vote_state.authorized_withdrawer != *withdraw_pubkey {
        return Err(ProgramError::MissingRequiredSignature);
    }

    vote_state.node_pubkey = *new_node_pubkey;
    Ok(())
}

pub(crate) fn update_commission(
    vote_account: &AccountInfo,
    commission: u8,
    withdraw_pubkey: &Pubkey,
    epoch_schedule: &EpochSchedule,
    clock: &Clock,
) -> Result<(), ProgramError> {
    let mut buffer = vote_account.try_borrow_mut_data()?;
    let vote_state = pod_from_bytes_mut::<VoteState>(&mut buffer)?;

    if vote_state.authorized_withdrawer != *withdraw_pubkey {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let is_commission_increase = commission > vote_state.commission;
    if !is_commission_increase && !is_commission_update_allowed(clock.slot, epoch_schedule) {
        return Err(VoteError::CommissionUpdateTooLate.into());
    }

    vote_state.commission = commission;

    Ok(())
}

/// Given the current slot and epoch schedule, determine if a commission change
/// is allowed
fn is_commission_update_allowed(slot: Slot, epoch_schedule: &EpochSchedule) -> bool {
    // always allowed during warmup epochs
    if let Some(relative_slot) = slot
        .saturating_sub(epoch_schedule.first_normal_slot)
        .checked_rem(epoch_schedule.slots_per_epoch)
    {
        // allowed up to the midpoint of the epoch
        relative_slot.saturating_mul(2) <= epoch_schedule.slots_per_epoch
    } else {
        // no slots per epoch, just allow it, even though this should never happen
        true
    }
}
