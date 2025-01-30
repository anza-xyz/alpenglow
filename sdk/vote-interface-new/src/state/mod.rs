//! Vote state
#[cfg(all(not(target_os = "solana"), feature = "bincode"))]
use bincode::deserialize;
#[cfg(feature = "bincode")]
use bincode::{serialize_into, ErrorKind};
#[cfg(feature = "serde")]
use serde_derive::{Deserialize, Serialize};
#[cfg(feature = "frozen-abi")]
use solana_frozen_abi_macro::{frozen_abi, AbiExample};
use {
    crate::{authorized_voters::AuthorizedVoters, error::VoteError},
    solana_clock::{Clock, Epoch, Slot, UnixTimestamp},
    solana_hash::Hash,
    solana_instruction::error::InstructionError,
    solana_pubkey::Pubkey,
    solana_rent::Rent,
    std::{collections::VecDeque, fmt::Debug},
};
#[cfg(test)]
use {
    arbitrary::{self, Arbitrary, Unstructured},
    solana_epoch_schedule::MAX_LEADER_SCHEDULE_EPOCH_OFFSET,
};

#[cfg(any(target_os = "solana", feature = "bincode"))]
mod vote_state_deserialize;
#[cfg(any(target_os = "solana", feature = "bincode"))]
use vote_state_deserialize::deserialize_vote_state_into;
pub mod vote_state_versions;
pub use vote_state_versions::*;

// Maximum number of credits history to keep around
pub const MAX_EPOCH_CREDITS_HISTORY: usize = 64;

// Offset of VoteState::prior_voters, for determining initialization status without deserialization
const DEFAULT_PRIOR_VOTERS_OFFSET: usize = 114;

// Number of slots of grace period for which maximum vote credits are awarded - votes landing within this number of slots of the slot that is being voted on are awarded full credits.
pub const VOTE_CREDITS_GRACE_SLOTS: u8 = 3;

// Maximum number of credits to award for a vote; this number of credits is awarded to votes on slots that land within the grace period. After that grace period, vote credits are reduced.
pub const VOTE_CREDITS_MAXIMUM_PER_SLOT: u8 = 16;

/// Representation of a vote in the VoteState after it has successfully been
/// processed by the vote program
/// #[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[cfg_attr(test, derive(Arbitrary))]
pub enum LandedVote {
    Notarize(Slot),
    Finalize(Slot),
    Skip(Slot),
}

/// Update this when the number of LandedVote variants are updated. Could
/// use a proc macro to do this dynamically, but annoying to set up.
pub const NUM_LANDED_VOTE_ENUM: u8 = 3;

impl Default for LandedVote {
    fn default() -> Self {
        LandedVote::Notarize(Slot::default())
    }
}

impl LandedVote {
    fn is_notarize(self) -> bool {
        matches!(self, LandedVote::Notarize(_))
    }
}

/// The essential core of the vote submitted by the validator for
/// processing by the vote program
/// #[cfg_attr(
#[cfg_attr(
    feature = "frozen-abi",
    frozen_abi(digest = "GvUzgtcxhKVVxPAjSntXGPqjLZK5ovgZzCiUP1tDpB9q"),
    derive(AbiExample)
)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum InnerVote {
    Notarize(Slot, Hash),
    Finalize(Slot, Hash),
    Skip(Slot),
}

// Manually implement Default
impl Default for InnerVote {
    fn default() -> Self {
        InnerVote::Notarize(Slot::default(), Hash::default())
    }
}

impl InnerVote {
    fn new_notarize(slot: Slot, hash: Hash) -> Self {
        Self::Notarize(slot, hash)
    }

    fn new_finalize(slot: Slot, hash: Hash) -> Self {
        Self::Finalize(slot, hash)
    }

    fn new_skip(slot: Slot) -> Self {
        Self::Skip(slot)
    }

    fn slot(&self) -> Slot {
        match self {
            InnerVote::Notarize(slot, _hash) => *slot,
            InnerVote::Finalize(slot, _hash) => *slot,
            InnerVote::Skip(slot) => *slot,
        }
    }

    fn hash(&self) -> Option<Hash> {
        match self {
            InnerVote::Notarize(_slot, hash) => Some(*hash),
            InnerVote::Finalize(_slot, hash) => Some(*hash),
            InnerVote::Skip(_slot) => None,
        }
    }
}

/// The essential core of the vote submitted by the validator for
/// processing by the vote program
#[cfg_attr(
    feature = "frozen-abi",
    frozen_abi(digest = "GvUzgtcxhKVVxPAjSntXGPqjLZK5ovgZzCiUP1tDpB9q"),
    derive(AbiExample)
)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct Vote {
    // TODO: Should this be `VOTE_CREDITS_GRACE_SLOTS`
    // multple inner votes to allow catch up?
    inner_vote: InnerVote,
    /// processing timestamp of last slot
    pub timestamp: Option<UnixTimestamp>,
}

impl Vote {
    pub fn new(inner_vote: InnerVote) -> Self {
        Self {
            inner_vote,
            timestamp: None,
        }
    }

    pub fn slot(&self) -> Slot {
        self.inner_vote.slot()
    }

    pub fn hash(&self) -> Option<Hash> {
        self.inner_vote.hash()
    }
}

impl LandedVote {
    pub fn slot(&self) -> Slot {
        match self {
            LandedVote::Notarize(slot) => *slot,
            LandedVote::Finalize(slot) => *slot,
            LandedVote::Skip(slot) => *slot,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct VoteInit {
    pub node_pubkey: Pubkey,
    pub authorized_voter: Pubkey,
    pub authorized_withdrawer: Pubkey,
    pub commission: u8,
}

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum VoteAuthorize {
    Voter,
    Withdrawer,
}

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct VoteAuthorizeWithSeedArgs {
    pub authorization_type: VoteAuthorize,
    pub current_authority_derived_key_owner: Pubkey,
    pub current_authority_derived_key_seed: String,
    pub new_authority: Pubkey,
}

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct VoteAuthorizeCheckedWithSeedArgs {
    pub authorization_type: VoteAuthorize,
    pub current_authority_derived_key_owner: Pubkey,
    pub current_authority_derived_key_seed: String,
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, Default, PartialEq, Eq, Clone)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct BlockTimestamp {
    pub slot: Slot,
    pub timestamp: UnixTimestamp,
}

// this is how many epochs a voter can be remembered for slashing
const MAX_ITEMS: usize = 32;

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct CircBuf<I> {
    buf: [I; MAX_ITEMS],
    /// next pointer
    idx: usize,
    is_empty: bool,
}

impl<I: Default + Copy> Default for CircBuf<I> {
    fn default() -> Self {
        Self {
            buf: [I::default(); MAX_ITEMS],
            idx: MAX_ITEMS
                .checked_sub(1)
                .expect("`MAX_ITEMS` should be positive"),
            is_empty: true,
        }
    }
}

impl<I> CircBuf<I> {
    pub fn append(&mut self, item: I) {
        // remember prior delegate and when we switched, to support later slashing
        self.idx = self
            .idx
            .checked_add(1)
            .and_then(|idx| idx.checked_rem(MAX_ITEMS))
            .expect("`self.idx` should be < `MAX_ITEMS` which should be non-zero");

        self.buf[self.idx] = item;
        self.is_empty = false;
    }

    pub fn buf(&self) -> &[I; MAX_ITEMS] {
        &self.buf
    }

    pub fn last(&self) -> Option<&I> {
        if !self.is_empty {
            self.buf.get(self.idx)
        } else {
            None
        }
    }
}

#[cfg_attr(
    feature = "frozen-abi",
    frozen_abi(digest = "BRwozbypfYXsHqFVj9w3iH5x1ak2NWHqCCn6pr3gHBkG"),
    derive(AbiExample)
)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, Default, PartialEq, Eq, Clone)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct VoteState {
    /// the node that votes in this account
    pub node_pubkey: Pubkey,

    /// the signer for withdrawals
    pub authorized_withdrawer: Pubkey,
    /// percentage (0-100) that represents what part of a rewards
    ///  payout should be given to this VoteAccount
    pub commission: u8,

    /// The latest VOTE_CREDITS_GRACE_SLOTS votes landed
    pub votes: VecDeque<LandedVote>,

    /// the signer for vote transactions
    authorized_voters: AuthorizedVoters,

    /// history of prior authorized voters and the epochs for which
    /// they were set, the bottom end of the range is inclusive,
    /// the top of the range is exclusive
    prior_voters: CircBuf<(Pubkey, Epoch, Epoch)>,

    /// history of how many credits earned by the end of each epoch
    ///  each tuple is (Epoch, credits, prev_credits)
    pub epoch_credits: Vec<(Epoch, u64, u64)>,

    /// most recent timestamp submitted with a vote
    pub last_timestamp: BlockTimestamp,
}

impl VoteState {
    pub fn new(vote_init: &VoteInit, clock: &Clock) -> Self {
        Self {
            node_pubkey: vote_init.node_pubkey,
            authorized_voters: AuthorizedVoters::new(clock.epoch, vote_init.authorized_voter),
            authorized_withdrawer: vote_init.authorized_withdrawer,
            commission: vote_init.commission,
            ..VoteState::default()
        }
    }

    pub fn new_rand_for_tests(node_pubkey: Pubkey, root_slot: Slot) -> Self {
        let votes = (1..32)
            .map(|x: Slot| LandedVote::Notarize(x.saturating_add(root_slot)))
            .collect();
        Self {
            node_pubkey,
            votes,
            ..VoteState::default()
        }
    }

    pub fn get_authorized_voter(&self, epoch: Epoch) -> Option<Pubkey> {
        self.authorized_voters.get_authorized_voter(epoch)
    }

    pub fn authorized_voters(&self) -> &AuthorizedVoters {
        &self.authorized_voters
    }

    pub fn prior_voters(&mut self) -> &CircBuf<(Pubkey, Epoch, Epoch)> {
        &self.prior_voters
    }

    pub fn get_rent_exempt_reserve(rent: &Rent) -> u64 {
        rent.minimum_balance(VoteState::size_of())
    }

    /// Upper limit on the size of the Vote State
    /// when votes.len() is MAX_LOCKOUT_HISTORY.
    pub const fn size_of() -> usize {
        3386 // see test_vote_state_size_of.
    }

    #[cfg(any(target_os = "solana", feature = "bincode"))]
    pub fn deserialize(input: &[u8]) -> Result<Self, InstructionError> {
        #[cfg(not(target_os = "solana"))]
        {
            deserialize::<VoteStateVersions>(input)
                .map(|versioned| versioned.convert_to_current())
                .map_err(|_| InstructionError::InvalidAccountData)
        }
        #[cfg(target_os = "solana")]
        {
            let mut vote_state = Self::default();
            Self::deserialize_into(input, &mut vote_state)?;
            Ok(vote_state)
        }
    }

    /// Deserializes the input `VoteStateVersions` buffer directly into the provided `VoteState`.
    ///
    /// In a SBPF context, V0_23_5 is not supported, but in non-SBPF, all versions are supported for
    /// compatibility with `bincode::deserialize`.
    ///
    /// On success, `vote_state` reflects the state of the input data. On failure, `vote_state` is
    /// reset to `VoteState::default()`.
    #[cfg(any(target_os = "solana", feature = "bincode"))]
    pub fn deserialize_into(
        input: &[u8],
        vote_state: &mut VoteState,
    ) -> Result<(), InstructionError> {
        // Rebind vote_state to *mut VoteState so that the &mut binding isn't
        // accessible anymore, preventing accidental use after this point.
        //
        // NOTE: switch to ptr::from_mut() once platform-tools moves to rustc >= 1.76
        let vote_state = vote_state as *mut VoteState;

        // Safety: vote_state is valid to_drop (see drop_in_place() docs). After
        // dropping, the pointer is treated as uninitialized and only accessed
        // through ptr::write, which is safe as per drop_in_place docs.
        unsafe {
            std::ptr::drop_in_place(vote_state);
        }

        // This is to reset vote_state to VoteState::default() if deserialize fails or panics.
        struct DropGuard {
            vote_state: *mut VoteState,
        }

        impl Drop for DropGuard {
            fn drop(&mut self) {
                // Safety:
                //
                // Deserialize failed or panicked so at this point vote_state is uninitialized. We
                // must write a new _valid_ value into it or after returning (or unwinding) from
                // this function the caller is left with an uninitialized `&mut VoteState`, which is
                // UB (references must always be valid).
                //
                // This is always safe and doesn't leak memory because deserialize_into_ptr() writes
                // into the fields that heap alloc only when it returns Ok().
                unsafe {
                    self.vote_state.write(VoteState::default());
                }
            }
        }

        let guard = DropGuard { vote_state };

        let res = VoteState::deserialize_into_ptr(input, vote_state);
        if res.is_ok() {
            std::mem::forget(guard);
        }

        res
    }

    /// Deserializes the input `VoteStateVersions` buffer directly into the provided
    /// `MaybeUninit<VoteState>`.
    #[cfg(any(target_os = "solana", feature = "bincode"))]
    pub fn deserialize_into_uninit(
        input: &[u8],
        vote_state: &mut std::mem::MaybeUninit<VoteState>,
    ) -> Result<(), InstructionError> {
        VoteState::deserialize_into_ptr(input, vote_state.as_mut_ptr())
    }

    #[cfg(any(target_os = "solana", feature = "bincode"))]
    fn deserialize_into_ptr(
        input: &[u8],
        vote_state: *mut VoteState,
    ) -> Result<(), InstructionError> {
        let mut cursor = std::io::Cursor::new(input);
        let variant = solana_serialize_utils::cursor::read_u32(&mut cursor)?;
        match variant {
            0 => deserialize_vote_state_into(&mut cursor, vote_state),
            _ => Err(InstructionError::InvalidAccountData),
        }?;

        Ok(())
    }

    #[cfg(feature = "bincode")]
    pub fn serialize(
        versioned: &VoteStateVersions,
        output: &mut [u8],
    ) -> Result<(), InstructionError> {
        serialize_into(output, versioned).map_err(|err| match *err {
            ErrorKind::SizeLimit => InstructionError::AccountDataTooSmall,
            _ => InstructionError::GenericError,
        })
    }

    /// returns commission split as (voter_portion, staker_portion, was_split) tuple
    ///
    ///  if commission calculation is 100% one way or other,
    ///   indicate with false for was_split
    pub fn commission_split(&self, on: u64) -> (u64, u64, bool) {
        match self.commission.min(100) {
            0 => (0, on, false),
            100 => (on, 0, false),
            split => {
                let on = u128::from(on);
                // Calculate mine and theirs independently and symmetrically instead of
                // using the remainder of the other to treat them strictly equally.
                // This is also to cancel the rewarding if either of the parties
                // should receive only fractional lamports, resulting in not being rewarded at all.
                // Thus, note that we intentionally discard any residual fractional lamports.
                let mine = on
                    .checked_mul(u128::from(split))
                    .expect("multiplication of a u64 and u8 should not overflow")
                    / 100u128;
                let theirs = on
                    .checked_mul(u128::from(
                        100u8
                            .checked_sub(split)
                            .expect("commission cannot be greater than 100"),
                    ))
                    .expect("multiplication of a u64 and u8 should not overflow")
                    / 100u128;

                (mine as u64, theirs as u64, true)
            }
        }
    }

    #[cfg(test)]
    fn get_max_sized_vote_state() -> VoteState {
        let mut authorized_voters = AuthorizedVoters::default();
        for i in 0..=MAX_LEADER_SCHEDULE_EPOCH_OFFSET {
            authorized_voters.insert(i, Pubkey::new_unique());
        }

        VoteState {
            votes: VecDeque::from(vec![
                LandedVote::default();
                VOTE_CREDITS_GRACE_SLOTS as usize
            ]),
            epoch_credits: vec![(0, 0, 0); MAX_EPOCH_CREDITS_HISTORY],
            authorized_voters,
            ..Self::default()
        }
    }

    pub fn process_next_vote_slot(&mut self, landed_vote: LandedVote, epoch: Epoch) {
        // Try replacing an existing Notarize vote for the same slot because
        // skip votes can replace Notarizes.
        if let Some(existing_vote) = self
            .votes
            .iter_mut()
            .find(|v| v.slot() == landed_vote.slot() && v.is_notarize())
        {
            *existing_vote = landed_vote;
            return;
        }

        // Find the last position where landed_vote is greater than an existing vote
        let insert_pos = self
            .votes
            .iter()
            .rposition(|v| landed_vote.slot() > v.slot());

        if let Some(index) = insert_pos {
            // Insert at the correct position to maintain order
            self.votes.insert(index + 1, landed_vote);
            // If deque is full, pop earliest vote and increment credits
            if self.votes.len() > VOTE_CREDITS_GRACE_SLOTS as usize {
                self.votes.pop_front();
                self.increment_credits(epoch, 1);
            }
        } else {
            // If landed_vote is the smallest, insert at the front if there's space
            if self.votes.len() < VOTE_CREDITS_GRACE_SLOTS as usize {
                self.votes.push_front(landed_vote);
            }
        }
    }

    /// increment credits, record credits for last epoch if new epoch
    pub fn increment_credits(&mut self, epoch: Epoch, credits: u64) {
        // increment credits, record by epoch

        // never seen a credit
        if self.epoch_credits.is_empty() {
            self.epoch_credits.push((epoch, 0, 0));
        } else if epoch != self.epoch_credits.last().unwrap().0 {
            let (_, credits, prev_credits) = *self.epoch_credits.last().unwrap();

            if credits != prev_credits {
                // if credits were earned previous epoch
                // append entry at end of list for the new epoch
                self.epoch_credits.push((epoch, credits, credits));
            } else {
                // else just move the current epoch
                self.epoch_credits.last_mut().unwrap().0 = epoch;
            }

            // Remove too old epoch_credits
            if self.epoch_credits.len() > MAX_EPOCH_CREDITS_HISTORY {
                self.epoch_credits.remove(0);
            }
        }

        self.epoch_credits.last_mut().unwrap().1 =
            self.epoch_credits.last().unwrap().1.saturating_add(credits);
    }

    // Computes the vote latency for vote on voted_for_slot where the vote itself landed in current_slot
    pub fn compute_vote_latency(voted_for_slot: Slot, current_slot: Slot) -> u8 {
        std::cmp::min(current_slot.saturating_sub(voted_for_slot), u8::MAX as u64) as u8
    }

    pub fn last_voted_slot(&self) -> Option<Slot> {
        self.votes.back().map(|vote| vote.slot())
    }

    pub fn current_epoch(&self) -> Epoch {
        if self.epoch_credits.is_empty() {
            0
        } else {
            self.epoch_credits.last().unwrap().0
        }
    }

    /// Number of "credits" owed to this account from the mining pool. Submit this
    /// VoteState to the Rewards program to trade credits for lamports.
    pub fn credits(&self) -> u64 {
        if self.epoch_credits.is_empty() {
            0
        } else {
            self.epoch_credits.last().unwrap().1
        }
    }

    /// Number of "credits" owed to this account from the mining pool on a per-epoch basis,
    ///  starting from credits observed.
    /// Each tuple of (Epoch, u64, u64) is read as (epoch, credits, prev_credits), where
    ///   credits for each epoch is credits - prev_credits; while redundant this makes
    ///   calculating rewards over partial epochs nice and simple
    pub fn epoch_credits(&self) -> &Vec<(Epoch, u64, u64)> {
        &self.epoch_credits
    }

    pub fn set_new_authorized_voter<F>(
        &mut self,
        authorized_pubkey: &Pubkey,
        current_epoch: Epoch,
        target_epoch: Epoch,
        verify: F,
    ) -> Result<(), InstructionError>
    where
        F: Fn(Pubkey) -> Result<(), InstructionError>,
    {
        let epoch_authorized_voter = self.get_and_update_authorized_voter(current_epoch)?;
        verify(epoch_authorized_voter)?;

        // The offset in slots `n` on which the target_epoch
        // (default value `DEFAULT_LEADER_SCHEDULE_SLOT_OFFSET`) is
        // calculated is the number of slots available from the
        // first slot `S` of an epoch in which to set a new voter for
        // the epoch at `S` + `n`
        if self.authorized_voters.contains(target_epoch) {
            return Err(VoteError::TooSoonToReauthorize.into());
        }

        // Get the latest authorized_voter
        let (latest_epoch, latest_authorized_pubkey) = self
            .authorized_voters
            .last()
            .ok_or(InstructionError::InvalidAccountData)?;

        // If we're not setting the same pubkey as authorized pubkey again,
        // then update the list of prior voters to mark the expiration
        // of the old authorized pubkey
        if latest_authorized_pubkey != authorized_pubkey {
            // Update the epoch ranges of authorized pubkeys that will be expired
            let epoch_of_last_authorized_switch =
                self.prior_voters.last().map(|range| range.2).unwrap_or(0);

            // target_epoch must:
            // 1) Be monotonically increasing due to the clock always
            //    moving forward
            // 2) not be equal to latest epoch otherwise this
            //    function would have returned TooSoonToReauthorize error
            //    above
            if target_epoch <= *latest_epoch {
                return Err(InstructionError::InvalidAccountData);
            }

            // Commit the new state
            self.prior_voters.append((
                *latest_authorized_pubkey,
                epoch_of_last_authorized_switch,
                target_epoch,
            ));
        }

        self.authorized_voters
            .insert(target_epoch, *authorized_pubkey);

        Ok(())
    }

    pub fn get_and_update_authorized_voter(
        &mut self,
        current_epoch: Epoch,
    ) -> Result<Pubkey, InstructionError> {
        let pubkey = self
            .authorized_voters
            .get_and_cache_authorized_voter_for_epoch(current_epoch)
            .ok_or(InstructionError::InvalidAccountData)?;
        self.authorized_voters
            .purge_authorized_voters(current_epoch);
        Ok(pubkey)
    }

    pub fn process_timestamp(
        &mut self,
        slot: Slot,
        timestamp: UnixTimestamp,
    ) -> Result<(), VoteError> {
        if (slot < self.last_timestamp.slot || timestamp < self.last_timestamp.timestamp)
            || (slot == self.last_timestamp.slot
                && BlockTimestamp { slot, timestamp } != self.last_timestamp
                && self.last_timestamp.slot != 0)
        {
            return Err(VoteError::TimestampTooOld);
        }
        self.last_timestamp = BlockTimestamp { slot, timestamp };
        Ok(())
    }

    pub fn is_correct_size_and_initialized(data: &[u8]) -> bool {
        const VERSION_OFFSET: usize = 4;
        const DEFAULT_PRIOR_VOTERS_END: usize = VERSION_OFFSET + DEFAULT_PRIOR_VOTERS_OFFSET;
        data.len() == VoteState::size_of()
            && data[VERSION_OFFSET..DEFAULT_PRIOR_VOTERS_END] != [0; DEFAULT_PRIOR_VOTERS_OFFSET]
    }
}

#[cfg(test)]
mod tests {
    use {super::*, bincode::serialized_size, core::mem::MaybeUninit, rand::Rng};

    #[test]
    fn test_vote_serialize() {
        let mut buffer: Vec<u8> = vec![0; VoteState::size_of()];
        let mut vote_state = VoteState::default();
        vote_state
            .votes
            .resize(VOTE_CREDITS_GRACE_SLOTS as usize, LandedVote::default());
        let versioned = VoteStateVersions::new_current(vote_state);
        assert!(VoteState::serialize(&versioned, &mut buffer[0..4]).is_err());
        VoteState::serialize(&versioned, &mut buffer).unwrap();
        assert_eq!(
            VoteState::deserialize(&buffer).unwrap(),
            versioned.convert_to_current()
        );
    }

    #[test]
    fn test_vote_deserialize_into() {
        // base case
        let target_vote_state = VoteState::default();
        let vote_state_buf =
            bincode::serialize(&VoteStateVersions::new_current(target_vote_state.clone())).unwrap();

        let mut test_vote_state = VoteState::default();
        VoteState::deserialize_into(&vote_state_buf, &mut test_vote_state).unwrap();

        assert_eq!(target_vote_state, test_vote_state);

        // variant
        // provide 4x the minimum struct size in bytes to ensure we typically touch every field
        let struct_bytes_x4 = std::mem::size_of::<VoteState>() * 4;
        for _ in 0..1000 {
            let raw_data: Vec<u8> = (0..struct_bytes_x4).map(|_| rand::random::<u8>()).collect();
            let mut unstructured = Unstructured::new(&raw_data);

            let target_vote_state_versions =
                VoteStateVersions::arbitrary(&mut unstructured).unwrap();
            let vote_state_buf = bincode::serialize(&target_vote_state_versions).unwrap();
            let target_vote_state = target_vote_state_versions.convert_to_current();

            let mut test_vote_state = VoteState::default();
            VoteState::deserialize_into(&vote_state_buf, &mut test_vote_state).unwrap();

            assert_eq!(target_vote_state, test_vote_state);
        }
    }

    #[test]
    fn test_vote_deserialize_into_error() {
        let target_vote_state = VoteState::new_rand_for_tests(Pubkey::new_unique(), 42);
        let mut vote_state_buf =
            bincode::serialize(&VoteStateVersions::new_current(target_vote_state.clone())).unwrap();
        let len = vote_state_buf.len();
        vote_state_buf.truncate(len - 1);

        let mut test_vote_state = VoteState::default();
        VoteState::deserialize_into(&vote_state_buf, &mut test_vote_state).unwrap_err();
        assert_eq!(test_vote_state, VoteState::default());
    }

    #[test]
    fn test_vote_deserialize_into_uninit() {
        // base case
        let target_vote_state = VoteState::default();
        let vote_state_buf =
            bincode::serialize(&VoteStateVersions::new_current(target_vote_state.clone())).unwrap();

        let mut test_vote_state = MaybeUninit::uninit();
        VoteState::deserialize_into_uninit(&vote_state_buf, &mut test_vote_state).unwrap();
        let test_vote_state = unsafe { test_vote_state.assume_init() };

        assert_eq!(target_vote_state, test_vote_state);

        // variant
        // provide 4x the minimum struct size in bytes to ensure we typically touch every field
        let struct_bytes_x4 = std::mem::size_of::<VoteState>() * 4;
        for _ in 0..1000 {
            let raw_data: Vec<u8> = (0..struct_bytes_x4).map(|_| rand::random::<u8>()).collect();
            let mut unstructured = Unstructured::new(&raw_data);

            let target_vote_state_versions =
                VoteStateVersions::arbitrary(&mut unstructured).unwrap();
            let vote_state_buf = bincode::serialize(&target_vote_state_versions).unwrap();
            let target_vote_state = target_vote_state_versions.convert_to_current();

            let mut test_vote_state = MaybeUninit::uninit();
            VoteState::deserialize_into_uninit(&vote_state_buf, &mut test_vote_state).unwrap();
            let test_vote_state = unsafe { test_vote_state.assume_init() };

            assert_eq!(target_vote_state, test_vote_state);
        }
    }

    #[test]
    fn test_vote_deserialize_into_uninit_nopanic() {
        // base case
        let mut test_vote_state = MaybeUninit::uninit();
        let e = VoteState::deserialize_into_uninit(&[], &mut test_vote_state).unwrap_err();
        assert_eq!(e, InstructionError::InvalidAccountData);

        // variant
        let serialized_len_x4 = serialized_size(&VoteState::default()).unwrap() * 4;
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let raw_data_length = rng.gen_range(1..serialized_len_x4);
            let mut raw_data: Vec<u8> = (0..raw_data_length).map(|_| rng.gen::<u8>()).collect();

            // pure random data will ~never have a valid enum tag, so lets help it out
            if raw_data_length >= 4 && rng.gen::<bool>() {
                let tag = rng.gen::<u8>() % 3;
                raw_data[0] = tag;
                raw_data[1] = 0;
                raw_data[2] = 0;
                raw_data[3] = 0;
            }

            // it is extremely improbable, though theoretically possible, for random bytes to be syntactically valid
            // so we only check that the parser does not panic and that it succeeds or fails exactly in line with bincode
            let mut test_vote_state = MaybeUninit::uninit();
            let test_res = VoteState::deserialize_into_uninit(&raw_data, &mut test_vote_state);
            let bincode_res = bincode::deserialize::<VoteStateVersions>(&raw_data)
                .map(|versioned| versioned.convert_to_current());

            if test_res.is_err() {
                assert!(bincode_res.is_err());
            } else {
                let test_vote_state = unsafe { test_vote_state.assume_init() };
                assert_eq!(test_vote_state, bincode_res.unwrap());
            }
        }
    }

    #[test]
    fn test_vote_deserialize_into_uninit_ill_sized() {
        // provide 4x the minimum struct size in bytes to ensure we typically touch every field
        let struct_bytes_x4 = std::mem::size_of::<VoteState>() * 4;
        for _ in 0..1000 {
            let raw_data: Vec<u8> = (0..struct_bytes_x4).map(|_| rand::random::<u8>()).collect();
            let mut unstructured = Unstructured::new(&raw_data);

            let original_vote_state_versions =
                VoteStateVersions::arbitrary(&mut unstructured).unwrap();
            let original_buf = bincode::serialize(&original_vote_state_versions).unwrap();

            let mut truncated_buf = original_buf.clone();
            let mut expanded_buf = original_buf.clone();

            truncated_buf.resize(original_buf.len() - 8, 0);
            expanded_buf.resize(original_buf.len() + 8, 0);

            // truncated fails
            let mut test_vote_state = MaybeUninit::uninit();
            let test_res = VoteState::deserialize_into_uninit(&truncated_buf, &mut test_vote_state);
            let bincode_res = bincode::deserialize::<VoteStateVersions>(&truncated_buf)
                .map(|versioned| versioned.convert_to_current());

            assert!(test_res.is_err());
            assert!(bincode_res.is_err());

            // expanded succeeds
            let mut test_vote_state = MaybeUninit::uninit();
            VoteState::deserialize_into_uninit(&expanded_buf, &mut test_vote_state).unwrap();
            let bincode_res = bincode::deserialize::<VoteStateVersions>(&expanded_buf)
                .map(|versioned| versioned.convert_to_current());

            let test_vote_state = unsafe { test_vote_state.assume_init() };
            assert_eq!(test_vote_state, bincode_res.unwrap());
        }
    }

    #[test]
    fn test_vote_state_commission_split() {
        let vote_state = VoteState::default();

        assert_eq!(vote_state.commission_split(1), (0, 1, false));

        let mut vote_state = VoteState {
            commission: u8::MAX,
            ..VoteState::default()
        };
        assert_eq!(vote_state.commission_split(1), (1, 0, false));

        vote_state.commission = 99;
        assert_eq!(vote_state.commission_split(10), (9, 0, true));

        vote_state.commission = 1;
        assert_eq!(vote_state.commission_split(10), (0, 9, true));

        vote_state.commission = 50;
        let (voter_portion, staker_portion, was_split) = vote_state.commission_split(10);

        assert_eq!((voter_portion, staker_portion, was_split), (5, 5, true));
    }

    #[test]
    fn test_vote_state_epoch_credits() {
        let mut vote_state = VoteState::default();

        assert_eq!(vote_state.credits(), 0);
        assert_eq!(vote_state.epoch_credits().clone(), vec![]);

        let mut expected = vec![];
        let mut credits = 0;
        let epochs = (MAX_EPOCH_CREDITS_HISTORY + 2) as u64;
        for epoch in 0..epochs {
            for _j in 0..epoch {
                vote_state.increment_credits(epoch, 1);
                credits += 1;
            }
            expected.push((epoch, credits, credits - epoch));
        }

        while expected.len() > MAX_EPOCH_CREDITS_HISTORY {
            expected.remove(0);
        }

        assert_eq!(vote_state.credits(), credits);
        assert_eq!(vote_state.epoch_credits().clone(), expected);
    }

    #[test]
    fn test_vote_state_epoch0_no_credits() {
        let mut vote_state = VoteState::default();

        assert_eq!(vote_state.epoch_credits().len(), 0);
        vote_state.increment_credits(1, 1);
        assert_eq!(vote_state.epoch_credits().len(), 1);

        vote_state.increment_credits(2, 1);
        assert_eq!(vote_state.epoch_credits().len(), 2);
    }

    #[test]
    fn test_vote_state_increment_credits() {
        let mut vote_state = VoteState::default();

        let credits = (MAX_EPOCH_CREDITS_HISTORY + 2) as u64;
        for i in 0..credits {
            vote_state.increment_credits(i, 1);
        }
        assert_eq!(vote_state.credits(), credits);
        assert!(vote_state.epoch_credits().len() <= MAX_EPOCH_CREDITS_HISTORY);
    }

    #[test]
    fn test_vote_process_timestamp() {
        let (slot, timestamp) = (15, 1_575_412_285);
        let mut vote_state = VoteState {
            last_timestamp: BlockTimestamp { slot, timestamp },
            ..VoteState::default()
        };

        assert_eq!(
            vote_state.process_timestamp(slot - 1, timestamp + 1),
            Err(VoteError::TimestampTooOld)
        );
        assert_eq!(
            vote_state.last_timestamp,
            BlockTimestamp { slot, timestamp }
        );
        assert_eq!(
            vote_state.process_timestamp(slot + 1, timestamp - 1),
            Err(VoteError::TimestampTooOld)
        );
        assert_eq!(
            vote_state.process_timestamp(slot, timestamp + 1),
            Err(VoteError::TimestampTooOld)
        );
        assert_eq!(vote_state.process_timestamp(slot, timestamp), Ok(()));
        assert_eq!(
            vote_state.last_timestamp,
            BlockTimestamp { slot, timestamp }
        );
        assert_eq!(vote_state.process_timestamp(slot + 1, timestamp), Ok(()));
        assert_eq!(
            vote_state.last_timestamp,
            BlockTimestamp {
                slot: slot + 1,
                timestamp
            }
        );
        assert_eq!(
            vote_state.process_timestamp(slot + 2, timestamp + 1),
            Ok(())
        );
        assert_eq!(
            vote_state.last_timestamp,
            BlockTimestamp {
                slot: slot + 2,
                timestamp: timestamp + 1
            }
        );

        // Test initial vote
        vote_state.last_timestamp = BlockTimestamp::default();
        assert_eq!(vote_state.process_timestamp(0, timestamp), Ok(()));
    }

    #[test]
    fn test_get_and_update_authorized_voter() {
        let original_voter = Pubkey::new_unique();
        let mut vote_state = VoteState::new(
            &VoteInit {
                node_pubkey: original_voter,
                authorized_voter: original_voter,
                authorized_withdrawer: original_voter,
                commission: 0,
            },
            &Clock::default(),
        );

        assert_eq!(vote_state.authorized_voters.len(), 1);
        assert_eq!(
            *vote_state.authorized_voters.first().unwrap().1,
            original_voter
        );

        // If no new authorized voter was set, the same authorized voter
        // is locked into the next epoch
        assert_eq!(
            vote_state.get_and_update_authorized_voter(1).unwrap(),
            original_voter
        );

        // Try to get the authorized voter for epoch 5, implies
        // the authorized voter for epochs 1-4 were unchanged
        assert_eq!(
            vote_state.get_and_update_authorized_voter(5).unwrap(),
            original_voter
        );

        // Authorized voter for expired epoch 0..5 should have been
        // purged and no longer queryable
        assert_eq!(vote_state.authorized_voters.len(), 1);
        for i in 0..5 {
            assert!(vote_state
                .authorized_voters
                .get_authorized_voter(i)
                .is_none());
        }

        // Set an authorized voter change at slot 7
        let new_authorized_voter = Pubkey::new_unique();
        vote_state
            .set_new_authorized_voter(&new_authorized_voter, 5, 7, |_| Ok(()))
            .unwrap();

        // Try to get the authorized voter for epoch 6, unchanged
        assert_eq!(
            vote_state.get_and_update_authorized_voter(6).unwrap(),
            original_voter
        );

        // Try to get the authorized voter for epoch 7 and onwards, should
        // be the new authorized voter
        for i in 7..10 {
            assert_eq!(
                vote_state.get_and_update_authorized_voter(i).unwrap(),
                new_authorized_voter
            );
        }
        assert_eq!(vote_state.authorized_voters.len(), 1);
    }

    #[test]
    fn test_set_new_authorized_voter() {
        let original_voter = Pubkey::new_unique();
        let epoch_offset = 15;
        let mut vote_state = VoteState::new(
            &VoteInit {
                node_pubkey: original_voter,
                authorized_voter: original_voter,
                authorized_withdrawer: original_voter,
                commission: 0,
            },
            &Clock::default(),
        );

        assert!(vote_state.prior_voters.last().is_none());

        let new_voter = Pubkey::new_unique();
        // Set a new authorized voter
        vote_state
            .set_new_authorized_voter(&new_voter, 0, epoch_offset, |_| Ok(()))
            .unwrap();

        assert_eq!(vote_state.prior_voters.idx, 0);
        assert_eq!(
            vote_state.prior_voters.last(),
            Some(&(original_voter, 0, epoch_offset))
        );

        // Trying to set authorized voter for same epoch again should fail
        assert_eq!(
            vote_state.set_new_authorized_voter(&new_voter, 0, epoch_offset, |_| Ok(())),
            Err(VoteError::TooSoonToReauthorize.into())
        );

        // Setting the same authorized voter again should succeed
        vote_state
            .set_new_authorized_voter(&new_voter, 2, 2 + epoch_offset, |_| Ok(()))
            .unwrap();

        // Set a third and fourth authorized voter
        let new_voter2 = Pubkey::new_unique();
        vote_state
            .set_new_authorized_voter(&new_voter2, 3, 3 + epoch_offset, |_| Ok(()))
            .unwrap();
        assert_eq!(vote_state.prior_voters.idx, 1);
        assert_eq!(
            vote_state.prior_voters.last(),
            Some(&(new_voter, epoch_offset, 3 + epoch_offset))
        );

        let new_voter3 = Pubkey::new_unique();
        vote_state
            .set_new_authorized_voter(&new_voter3, 6, 6 + epoch_offset, |_| Ok(()))
            .unwrap();
        assert_eq!(vote_state.prior_voters.idx, 2);
        assert_eq!(
            vote_state.prior_voters.last(),
            Some(&(new_voter2, 3 + epoch_offset, 6 + epoch_offset))
        );

        // Check can set back to original voter
        vote_state
            .set_new_authorized_voter(&original_voter, 9, 9 + epoch_offset, |_| Ok(()))
            .unwrap();

        // Run with these voters for a while, check the ranges of authorized
        // voters is correct
        for i in 9..epoch_offset {
            assert_eq!(
                vote_state.get_and_update_authorized_voter(i).unwrap(),
                original_voter
            );
        }
        for i in epoch_offset..3 + epoch_offset {
            assert_eq!(
                vote_state.get_and_update_authorized_voter(i).unwrap(),
                new_voter
            );
        }
        for i in 3 + epoch_offset..6 + epoch_offset {
            assert_eq!(
                vote_state.get_and_update_authorized_voter(i).unwrap(),
                new_voter2
            );
        }
        for i in 6 + epoch_offset..9 + epoch_offset {
            assert_eq!(
                vote_state.get_and_update_authorized_voter(i).unwrap(),
                new_voter3
            );
        }
        for i in 9 + epoch_offset..=10 + epoch_offset {
            assert_eq!(
                vote_state.get_and_update_authorized_voter(i).unwrap(),
                original_voter
            );
        }
    }

    #[test]
    fn test_authorized_voter_is_locked_within_epoch() {
        let original_voter = Pubkey::new_unique();
        let mut vote_state = VoteState::new(
            &VoteInit {
                node_pubkey: original_voter,
                authorized_voter: original_voter,
                authorized_withdrawer: original_voter,
                commission: 0,
            },
            &Clock::default(),
        );

        // Test that it's not possible to set a new authorized
        // voter within the same epoch, even if none has been
        // explicitly set before
        let new_voter = Pubkey::new_unique();
        assert_eq!(
            vote_state.set_new_authorized_voter(&new_voter, 1, 1, |_| Ok(())),
            Err(VoteError::TooSoonToReauthorize.into())
        );

        assert_eq!(vote_state.get_authorized_voter(1), Some(original_voter));

        // Set a new authorized voter for a future epoch
        assert_eq!(
            vote_state.set_new_authorized_voter(&new_voter, 1, 2, |_| Ok(())),
            Ok(())
        );

        // Test that it's not possible to set a new authorized
        // voter within the same epoch, even if none has been
        // explicitly set before
        assert_eq!(
            vote_state.set_new_authorized_voter(&original_voter, 3, 3, |_| Ok(())),
            Err(VoteError::TooSoonToReauthorize.into())
        );

        assert_eq!(vote_state.get_authorized_voter(3), Some(new_voter));
    }

    #[test]
    fn test_vote_state_size_of() {
        let vote_state = VoteState::get_max_sized_vote_state();
        let vote_state = VoteStateVersions::new_current(vote_state);
        let size = serialized_size(&vote_state).unwrap();
        assert_eq!(VoteState::size_of() as u64, size);
    }

    #[test]
    fn test_vote_state_max_size() {
        let mut max_sized_data = vec![0; VoteState::size_of()];
        let vote_state = VoteState::get_max_sized_vote_state();
        let (start_leader_schedule_epoch, _) = vote_state.authorized_voters.last().unwrap();
        let start_current_epoch =
            start_leader_schedule_epoch - MAX_LEADER_SCHEDULE_EPOCH_OFFSET + 1;

        let mut vote_state = Some(vote_state);
        for i in start_current_epoch..start_current_epoch + 2 * MAX_LEADER_SCHEDULE_EPOCH_OFFSET {
            vote_state.as_mut().map(|vote_state| {
                vote_state.set_new_authorized_voter(
                    &Pubkey::new_unique(),
                    i,
                    i + MAX_LEADER_SCHEDULE_EPOCH_OFFSET,
                    |_| Ok(()),
                )
            });

            let versioned = VoteStateVersions::new_current(vote_state.take().unwrap());
            VoteState::serialize(&versioned, &mut max_sized_data).unwrap();
            vote_state = Some(versioned.convert_to_current());
        }
    }

    #[test]
    fn test_default_vote_state_is_uninitialized() {
        // The default `VoteState` is stored to de-initialize a zero-balance vote account,
        // so must remain such that `VoteStateVersions::is_uninitialized()` returns true
        // when called on a `VoteStateVersions` that stores it
        assert!(VoteStateVersions::new_current(VoteState::default()).is_uninitialized());
    }

    #[test]
    fn test_is_correct_size_and_initialized() {
        // Check all zeroes
        let mut vote_account_data = vec![0; VoteStateVersions::vote_state_size_of()];
        assert!(!VoteStateVersions::is_correct_size_and_initialized(
            &vote_account_data
        ));

        // Check default VoteState
        let default_account_state = VoteStateVersions::new_current(VoteState::default());
        VoteState::serialize(&default_account_state, &mut vote_account_data).unwrap();
        assert!(!VoteStateVersions::is_correct_size_and_initialized(
            &vote_account_data
        ));

        // Check non-zero data shorter than offset index used
        let short_data = vec![1; DEFAULT_PRIOR_VOTERS_OFFSET];
        assert!(!VoteStateVersions::is_correct_size_and_initialized(
            &short_data
        ));

        // Check non-zero large account
        let mut large_vote_data = vec![1; 2 * VoteStateVersions::vote_state_size_of()];
        let default_account_state = VoteStateVersions::new_current(VoteState::default());
        VoteState::serialize(&default_account_state, &mut large_vote_data).unwrap();
        assert!(!VoteStateVersions::is_correct_size_and_initialized(
            &vote_account_data
        ));

        // Check populated VoteState
        let vote_state = VoteState::new(
            &VoteInit {
                node_pubkey: Pubkey::new_unique(),
                authorized_voter: Pubkey::new_unique(),
                authorized_withdrawer: Pubkey::new_unique(),
                commission: 0,
            },
            &Clock::default(),
        );
        let account_state = VoteStateVersions::new_current(vote_state.clone());
        VoteState::serialize(&account_state, &mut vote_account_data).unwrap();
        assert!(VoteStateVersions::is_correct_size_and_initialized(
            &vote_account_data
        ));
    }

    #[test]
    fn test_minimum_balance() {
        let rent = solana_rent::Rent::default();
        let minimum_balance = rent.minimum_balance(VoteState::size_of());
        // golden, may need updating when vote_state grows
        assert!(minimum_balance as f64 / 10f64.powf(9.0) < 0.04)
    }

    #[test]
    fn test_circbuf_oob() {
        // Craft an invalid CircBuf with out-of-bounds index
        let data: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00];
        let circ_buf: CircBuf<()> = bincode::deserialize(data).unwrap();
        assert_eq!(circ_buf.last(), None);
    }
}
