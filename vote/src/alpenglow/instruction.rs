//! Program instructions

use {
    crate::alpenglow::{
        error::VoteError,
        id,
        state::{PodSlot, VoteState},
        vote::{
            FinalizationVote, NotarizationFallbackVote, NotarizationVote, SkipFallbackVote,
            SkipVote,
        },
        vote_processor::{NotarizationVoteInstructionData, CURRENT_NOTARIZE_VOTE_VERSION},
    },
    bytemuck::{Pod, Zeroable},
    num_enum::{IntoPrimitive, TryFromPrimitive},
    solana_bls_signatures::Pubkey as BlsPubkey,
    solana_program::{
        instruction::{AccountMeta, Instruction},
        program_error::ProgramError,
        pubkey::Pubkey,
        rent::Rent,
        system_instruction,
    },
    spl_pod::{
        bytemuck::{pod_bytes_of, pod_from_bytes, pod_get_packed_len},
        primitives::{PodU32, PodU64},
        slice::PodSlice,
    },
};

/// Instructions supported by the program
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive, IntoPrimitive)]
pub enum VoteInstruction {
    /// Initialize a vote account
    ///
    /// # Account references
    ///   0. `[WRITE]` Uninitialized vote account
    ///   1. `[SIGNER]` New validator identity (node_pubkey)
    ///
    ///   Data expected by this instruction:
    ///     `InitializeAccountInstructionData`
    InitializeAccount,

    /// Authorize a key to send votes or issue a withdrawal
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated with the Pubkey for authorization
    ///   1. `[SIGNER]` Vote or withdraw authority
    ///
    ///   Data expected by this instruction:
    ///     `AuthorizeInstructionData`
    Authorize,

    /// Authorize a key to send votes or issue a withdrawal
    ///
    /// This instruction behaves like `Authorize` with the additional requirement that the new vote
    /// or withdraw authority must also be a signer.
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated with the Pubkey for authorization
    ///   1. `[SIGNER]` Vote or withdraw authority
    ///   2. `[SIGNER]` New vote or withdraw authority
    ///
    ///   Data expected by this instruction:
    ///     `VoteAuthorize`
    AuthorizeChecked,

    /// Given that the current Voter or Withdrawer authority is a derived key,
    /// this instruction allows someone who can sign for that derived key's
    /// base key to authorize a new Voter or Withdrawer for a vote account.
    ///
    /// # Account references
    ///   0. `[Write]` Vote account to be updated
    ///   1. `[SIGNER]` Base key of current Voter or Withdrawer authority's derived key
    ///
    ///   Data expected by this instruction:
    ///     `AuthorizeWithSeedInstructionData`
    AuthorizeWithSeed,

    /// Given that the current Voter or Withdrawer authority is a derived key,
    /// this instruction allows someone who can sign for that derived key's
    /// base key to authorize a new Voter or Withdrawer for a vote account.
    ///
    /// This instruction behaves like `AuthorizeWithSeed` with the additional requirement
    /// that the new vote or withdraw authority must also be a signer.
    ///
    /// # Account references
    ///   0. `[Write]` Vote account to be updated
    ///   1. `[SIGNER]` Base key of current Voter or Withdrawer authority's derived key
    ///   2. `[SIGNER]` New vote or withdraw authority
    ///
    ///   Data expected by this instruction:
    ///     `AuthorizeCheckedWithSeedInstructionData`
    AuthorizeCheckedWithSeed,

    /// Withdraw some amount of funds
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to withdraw from
    ///   1. `[WRITE]` Recipient account
    ///   2. `[SIGNER]` Withdraw authority
    ///
    ///   Data expected by this instruction:
    ///     `lamports` : `u64`
    Withdraw,

    /// Update the vote account's validator identity (node_pubkey)
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated with the given authority public key
    ///   1. `[SIGNER]` New validator identity (node_pubkey)
    ///   2. `[SIGNER]` Withdraw authority
    UpdateValidatorIdentity,

    /// Update the commission for the vote account
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated
    ///   1. `[SIGNER]` Withdraw authority
    ///
    ///   Data expected by this instruction:
    ///     `commission` : `u8`
    UpdateCommission,

    /// A notarization vote
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated
    ///   1. `[SIGNER]` Vote authority
    ///
    ///   Data expected by this instruction:
    ///     `NotarizationVoteInstructionData`
    Notarize,

    /// A finalization vote
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated
    ///   1. `[SIGNER]` Vote authority
    ///
    ///   Data expected by this instruction:
    ///     `slot` : `u64`
    Finalize,

    /// A skip vote
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated
    ///   1. `[SIGNER]` Vote authority
    ///
    ///   Data expected by this instruction:
    ///     `slot` : `u64`
    Skip,

    /// A notarization fallback vote
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated
    ///   1. `[SIGNER]` Vote authority
    ///
    ///   Data expected by this instruction:
    ///     `NotarizationVoteInstructionData`
    NotarizeFallback,

    /// A skip fallback vote
    ///
    /// # Account references
    ///   0. `[WRITE]` Vote account to be updated
    ///   1. `[SIGNER]` Vote authority
    ///
    ///   Data expected by this instruction:
    ///     `slot` : `u64`
    SkipFallback,
}

/// Instruction builder to create a notarization vote
pub fn notarize(
    vote_pubkey: Pubkey,
    vote_authority: Pubkey,
    vote: &NotarizationVote,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(vote_authority, true),
    ];

    encode_instruction(
        accounts,
        VoteInstruction::Notarize,
        &NotarizationVoteInstructionData {
            version: CURRENT_NOTARIZE_VOTE_VERSION,
            slot: PodSlot::from(vote.slot()),
            block_id: *vote.block_id(),
            _replayed_slot: PodSlot::from(0),
            replayed_bank_hash: *vote.replayed_bank_hash(),
        },
    )
}

/// Instruction builder to create a finalization vote
pub fn finalize(
    vote_pubkey: Pubkey,
    vote_authority: Pubkey,
    vote: &FinalizationVote,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(vote_authority, true),
    ];

    encode_instruction(
        accounts,
        VoteInstruction::Finalize,
        &PodSlot::from(vote.slot()),
    )
}

/// Instruction builder to create a skip vote
pub fn skip(vote_pubkey: Pubkey, vote_authority: Pubkey, vote: &SkipVote) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(vote_authority, true),
    ];

    encode_instruction(accounts, VoteInstruction::Skip, &PodSlot::from(vote.slot()))
}

/// Instruction builder to create a notarization fallback vote
pub fn notarize_fallback(
    vote_pubkey: Pubkey,
    vote_authority: Pubkey,
    vote: &NotarizationFallbackVote,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(vote_authority, true),
    ];

    encode_instruction(
        accounts,
        VoteInstruction::NotarizeFallback,
        &NotarizationVoteInstructionData {
            version: CURRENT_NOTARIZE_VOTE_VERSION,
            slot: PodSlot::from(vote.slot()),
            block_id: *vote.block_id(),
            _replayed_slot: PodSlot::from(0),
            replayed_bank_hash: *vote.replayed_bank_hash(),
        },
    )
}

/// Instruction builder to create a skip fallback vote
pub fn skip_fallback(
    vote_pubkey: Pubkey,
    vote_authority: Pubkey,
    vote: &SkipFallbackVote,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(vote_authority, true),
    ];

    encode_instruction(
        accounts,
        VoteInstruction::SkipFallback,
        &PodSlot::from(vote.slot()),
    )
}

/// Data expected by
/// `VoteInstruction::InitializeAccount`
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct InitializeAccountInstructionData {
    /// The node that votes in this account
    pub node_pubkey: Pubkey,
    /// The signer for vote transactions
    pub authorized_voter: Pubkey,
    /// The signer for withdrawals
    pub authorized_withdrawer: Pubkey,
    /// The commission percentage for this vote account
    pub commission: u8,
    /// BLS public key
    pub bls_pubkey: BlsPubkey,
}

/// Instruction builder to initialize a new vote account with a valid VoteState:
/// - `vote_pubkey` the vote account
/// - `instruction_data` the vote account's account creation metadata
pub fn initialize_account(
    vote_pubkey: Pubkey,
    instruction_data: &InitializeAccountInstructionData,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(instruction_data.node_pubkey, true),
    ];

    encode_instruction(
        accounts,
        VoteInstruction::InitializeAccount,
        instruction_data,
    )
}

/// Instruction builder to create and initialize a new vote account with a valid VoteState:
/// - `from_pubkey` the account that funds the rent exemption
/// - `vote_pubkey` the vote account
/// - `rent` network Rent details
/// - `instruction_data` the vote account's account creation metadata
/// - `excess_lamports` if set to `Some(val)`, funds `val` extra lamports for rent
pub fn create_account_with_config_excess_lamports(
    from_pubkey: &Pubkey,
    vote_pubkey: &Pubkey,
    rent: &Rent,
    instruction_data: InitializeAccountInstructionData,
    excess_lamports: Option<u64>,
) -> Vec<Instruction> {
    let create_ix = system_instruction::create_account(
        from_pubkey,
        vote_pubkey,
        rent.minimum_balance(VoteState::size())
            .saturating_add(excess_lamports.unwrap_or(0)),
        VoteState::size() as u64,
        &id(),
    );

    let init_ix = initialize_account(*vote_pubkey, &instruction_data);

    vec![create_ix, init_ix]
}

/// Instruction builder to create and initialize a new vote account with a valid VoteState:
/// - `from_pubkey` the account that funds the rent exemption
/// - `vote_pubkey` the vote account
/// - `rent` network Rent details
/// - `instruction_data` the vote account's account creation metadata
pub fn create_account_with_config(
    from_pubkey: &Pubkey,
    vote_pubkey: &Pubkey,
    rent: &Rent,
    instruction_data: InitializeAccountInstructionData,
) -> Vec<Instruction> {
    create_account_with_config_excess_lamports(
        from_pubkey,
        vote_pubkey,
        rent,
        instruction_data,
        None,
    )
}

/// The type of authority on the account
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
pub enum AuthorityType {
    /// Voting authority
    Voter,
    /// Withdrawal authority
    Withdrawer,
}

/// Data expected by
/// `VoteInstruction::Authorize`
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct AuthorizeInstructionData {
    /// New authority pubkey for the vote account
    pub new_authorized_pubkey: Pubkey,
    /// The type of authority
    pub authority_type: u8,
}

/// Instruction builder to update the authority of a vote account
/// - `vote_pubkey` the vote account
/// - `authorized_pubkey` the current authority
/// - `new_authorized_pubkey` the new authority
/// - `authority_type` the type of the authorities
pub fn authorize(
    vote_pubkey: Pubkey,
    authorized_pubkey: Pubkey, // currently authorized
    new_authorized_pubkey: Pubkey,
    authority_type: AuthorityType,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(authorized_pubkey, true),
    ];

    encode_instruction(
        accounts,
        VoteInstruction::Authorize,
        &AuthorizeInstructionData {
            new_authorized_pubkey,
            authority_type: u8::from(authority_type),
        },
    )
}

/// Instruction builder to update the authority of a vote account
/// This checked variant requires `new_authorized_pubkey` to be a signer
/// - `vote_pubkey` the vote account
/// - `authorized_pubkey` the current authority
/// - `new_authorized_pubkey` the new authority
/// - `vote_authorize` the type of the authorities
pub fn authorize_checked(
    vote_pubkey: Pubkey,
    authorized_pubkey: Pubkey, // currently authorized
    new_authorized_pubkey: Pubkey,
    authority_type: AuthorityType,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(authorized_pubkey, true),
        AccountMeta::new_readonly(new_authorized_pubkey, true),
    ];

    encode_instruction(
        accounts,
        VoteInstruction::AuthorizeChecked,
        &u8::from(authority_type),
    )
}

/// Data expected by
/// `VoteInstruction::AuthorizeWithSeed`
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct AuthorizeWithSeedInstructionData {
    /// The authority type
    pub authority_type: u8,
    /// The current authority owner key
    pub current_authority_derived_key_owner: Pubkey,
    /// The new authority pubkey for the vote account
    pub new_authority: Pubkey,
}

/// Instruction builder to update the authority of a vote account
/// using a seed based schema
/// - `vote_pubkey` the vote account
/// - `current_authority_base_key` the base key of the current authority
/// - `current_authority_derived_key_owner` current authority owner
/// - `current_authority_derived_key_seed` current authority seed
/// - `new_authorized_pubkey` the new authority
/// - `authority_type` the type of the authorities
pub fn authorize_with_seed(
    vote_pubkey: Pubkey,
    current_authority_base_key: Pubkey,
    current_authority_derived_key_owner: Pubkey,
    current_authority_derived_key_seed: &str,
    new_authority: Pubkey,
    authority_type: AuthorityType,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(current_authority_base_key, true),
    ];

    encode_instruction_with_seed(
        accounts,
        VoteInstruction::AuthorizeWithSeed,
        &AuthorizeWithSeedInstructionData {
            authority_type: u8::from(authority_type),
            current_authority_derived_key_owner,
            new_authority,
        },
        Some(current_authority_derived_key_seed),
    )
}

/// Data expected by
/// `VoteInstruction::AuthorizeCheckedWithSeed`
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct AuthorizeCheckedWithSeedInstructionData {
    /// The authority type
    pub authority_type: u8,
    /// The current authority owner key
    pub current_authority_derived_key_owner: Pubkey,
}

/// Instruction builder to update the authority of a vote account
/// using a seed based schema
/// This checked variant requires `new_authorized_pubkey` to be a signer
/// - `vote_pubkey` the vote account
/// - `current_authority_base_key` the base key of the current authority
/// - `current_authority_derived_key_owner` current authority owner
/// - `current_authority_derived_key_seed` current authority seed
/// - `new_authorized_pubkey` the new authority
/// - `authority_type` the type of the authorities
pub fn authorize_checked_with_seed(
    vote_pubkey: Pubkey,
    current_authority_base_key: Pubkey,
    current_authority_derived_key_owner: Pubkey,
    current_authority_derived_key_seed: &str,
    new_authority: Pubkey,
    authorization_type: AuthorityType,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(current_authority_base_key, true),
        AccountMeta::new_readonly(new_authority, true),
    ];

    encode_instruction_with_seed(
        accounts,
        VoteInstruction::AuthorizeCheckedWithSeed,
        &AuthorizeCheckedWithSeedInstructionData {
            authority_type: u8::from(authorization_type),
            current_authority_derived_key_owner,
        },
        Some(current_authority_derived_key_seed),
    )
}

/// Instruction builder to withdraw from the vote account
/// - `vote_pubkey` the vote account
/// - `authorized_withdrawer_pubkey` the withdraw authority of the vote account
/// - `lamports` amount to withdraw
/// - `recipient` the account to withdraw to
pub fn withdraw(
    vote_pubkey: Pubkey,
    authorized_withdrawer_pubkey: Pubkey,
    lamports: u64,
    recipient_pubkey: Pubkey,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new(recipient_pubkey, false),
        AccountMeta::new_readonly(authorized_withdrawer_pubkey, true),
    ];

    encode_instruction(accounts, VoteInstruction::Withdraw, &PodU64::from(lamports))
}

/// Instruction builder to update the node pubkey on the vote account
/// - `vote_pubkey` the vote account
/// - `authorized_withdrawer_pubkey` the withdraw authority of the vote account
/// - `node_pubkey` the new node pubkey to write to the vote account
pub fn update_validator_identity(
    vote_pubkey: Pubkey,
    authorized_withdrawer_pubkey: Pubkey,
    new_node_pubkey: Pubkey,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(new_node_pubkey, true),
        AccountMeta::new_readonly(authorized_withdrawer_pubkey, true),
    ];

    let data = vec![u8::from(VoteInstruction::UpdateValidatorIdentity)];

    Instruction {
        program_id: id(),
        accounts,
        data,
    }
}

/// Instruction builder to update the commission on the vote account
/// - `vote_pubkey` the vote account
/// - `authorized_withdrawer_pubkey` the withdraw authority of the vote account
/// - `commission`  the new commission to write to the vote account
pub fn update_commission(
    vote_pubkey: Pubkey,
    authorized_withdrawer_pubkey: Pubkey,
    new_commission: u8,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(vote_pubkey, false),
        AccountMeta::new_readonly(authorized_withdrawer_pubkey, true),
    ];

    encode_instruction(accounts, VoteInstruction::UpdateCommission, &new_commission)
}

/// Utility function for encoding instruction data
pub(crate) fn encode_instruction<D: Pod>(
    accounts: Vec<AccountMeta>,
    instruction: VoteInstruction,
    instruction_data: &D,
) -> Instruction {
    encode_instruction_with_seed(accounts, instruction, instruction_data, None)
}

/// Utility function for encoding instruction data
/// with a seed.
///
/// Some accounting instructions have a variable length
/// `seed`, we serialize this as a pod slice at the end
/// of the instruction data
pub(crate) fn encode_instruction_with_seed<D: Pod>(
    accounts: Vec<AccountMeta>,
    instruction: VoteInstruction,
    instruction_data: &D,
    seed: Option<&str>,
) -> Instruction {
    let mut data = vec![u8::from(instruction)];
    data.extend_from_slice(bytemuck::bytes_of(instruction_data));
    if let Some(seed) = seed {
        let seed_len = PodU32::from(seed.len() as u32);
        data.extend_from_slice(&[pod_bytes_of(&seed_len), seed.as_bytes()].concat());
    }
    Instruction {
        program_id: id(),
        accounts,
        data,
    }
}

/// Utility function for decoding just the instruction type
pub(crate) fn decode_instruction_type(input: &[u8]) -> Result<VoteInstruction, ProgramError> {
    if input.is_empty() {
        Err(ProgramError::InvalidInstructionData)
    } else {
        VoteInstruction::try_from(input[0]).map_err(|_| VoteError::InvalidInstruction.into())
    }
}

/// Utility function for decoding instruction data
pub(crate) fn decode_instruction_data<T: Pod>(input_with_type: &[u8]) -> Result<&T, ProgramError> {
    if input_with_type.len() != pod_get_packed_len::<T>().saturating_add(1) {
        Err(ProgramError::InvalidInstructionData)
    } else {
        pod_from_bytes(&input_with_type[1..])
    }
}

/// Utility function for decoding instruction data with a variable length seed
pub(crate) fn decode_instruction_data_with_seed<T: Pod>(
    input_with_type: &[u8],
) -> Result<(&T, PodSlice<'_, u8>), ProgramError> {
    if input_with_type.len() < pod_get_packed_len::<T>().saturating_add(1) {
        return Err(ProgramError::InvalidInstructionData);
    }

    let data_offset = std::mem::size_of::<T>()
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let instruction_data = pod_from_bytes(&input_with_type[1..data_offset])?;
    let seed = PodSlice::unpack(&input_with_type[data_offset..])?;
    Ok((instruction_data, seed))
}
