use {
    log::*,
    solana_bls_signatures::{keypair::Keypair as BLSKeypair, Pubkey as BLSPubkey},
    solana_feature_set::{self, FeatureSet, FEATURE_NAMES},
    solana_loader_v3_interface::state::UpgradeableLoaderState,
    solana_sdk::{
        account::{Account, AccountSharedData},
        feature::{self, Feature},
        fee_calculator::FeeRateGovernor,
        genesis_config::{ClusterType, GenesisConfig},
        native_token::sol_to_lamports,
        pubkey::Pubkey,
        rent::Rent,
        signature::{Keypair, Signer},
        signer::SeedDerivable,
        stake::state::StakeStateV2,
        system_program,
    },
    solana_stake_program::stake_state,
    solana_vote_program::vote_state,
    solana_votor_messages::{
        self, bls_message::BLS_KEYPAIR_DERIVE_SEED, state::VoteState as AlpenglowVoteState,
    },
    std::{borrow::Borrow, fs::File, io::Read},
};

// Default amount received by the validator
const VALIDATOR_LAMPORTS: u64 = 42;

// fun fact: rustc is very close to make this const fn.
pub fn bootstrap_validator_stake_lamports() -> u64 {
    Rent::default().minimum_balance(StakeStateV2::size_of())
}

// Number of lamports automatically used for genesis accounts
pub const fn genesis_sysvar_and_builtin_program_lamports() -> u64 {
    const NUM_BUILTIN_PROGRAMS: u64 = 9;
    const NUM_PRECOMPILES: u64 = 2;
    const STAKE_HISTORY_MIN_BALANCE: u64 = 114_979_200;
    const CLOCK_SYSVAR_MIN_BALANCE: u64 = 1_169_280;
    const RENT_SYSVAR_MIN_BALANCE: u64 = 1_009_200;
    const EPOCH_SCHEDULE_SYSVAR_MIN_BALANCE: u64 = 1_120_560;
    const RECENT_BLOCKHASHES_SYSVAR_MIN_BALANCE: u64 = 42_706_560;

    STAKE_HISTORY_MIN_BALANCE
        + CLOCK_SYSVAR_MIN_BALANCE
        + RENT_SYSVAR_MIN_BALANCE
        + EPOCH_SCHEDULE_SYSVAR_MIN_BALANCE
        + RECENT_BLOCKHASHES_SYSVAR_MIN_BALANCE
        + NUM_BUILTIN_PROGRAMS
        + NUM_PRECOMPILES
}

pub struct ValidatorVoteKeypairs {
    pub node_keypair: Keypair,
    pub vote_keypair: Keypair,
    pub stake_keypair: Keypair,
    pub bls_keypair: BLSKeypair,
}

impl ValidatorVoteKeypairs {
    pub fn new(node_keypair: Keypair, vote_keypair: Keypair, stake_keypair: Keypair) -> Self {
        let bls_keypair =
            BLSKeypair::derive_from_signer(&vote_keypair, BLS_KEYPAIR_DERIVE_SEED).unwrap();
        Self {
            node_keypair,
            vote_keypair,
            stake_keypair,
            bls_keypair,
        }
    }

    pub fn new_rand() -> Self {
        Self {
            node_keypair: Keypair::new(),
            vote_keypair: Keypair::new(),
            stake_keypair: Keypair::new(),
            bls_keypair: BLSKeypair::new(),
        }
    }
}

pub struct GenesisConfigInfo {
    pub genesis_config: GenesisConfig,
    pub mint_keypair: Keypair,
    pub voting_keypair: Keypair,
    pub validator_pubkey: Pubkey,
}

pub fn create_genesis_config(mint_lamports: u64) -> GenesisConfigInfo {
    // Note that zero lamports for validator stake will result in stake account
    // not being stored in accounts-db but still cached in bank stakes. This
    // causes discrepancy between cached stakes accounts in bank and
    // accounts-db which in particular will break snapshots test.
    create_genesis_config_with_leader(
        mint_lamports,
        &solana_pubkey::new_rand(), // validator_pubkey
        0,                          // validator_stake_lamports
    )
}

pub fn create_genesis_config_with_vote_accounts(
    mint_lamports: u64,
    voting_keypairs: &[impl Borrow<ValidatorVoteKeypairs>],
    stakes: Vec<u64>,
) -> GenesisConfigInfo {
    create_genesis_config_with_vote_accounts_and_cluster_type(
        mint_lamports,
        voting_keypairs,
        stakes,
        ClusterType::Development,
        None,
    )
}

pub fn create_genesis_config_with_alpenglow_vote_accounts(
    mint_lamports: u64,
    voting_keypairs: &[impl Borrow<ValidatorVoteKeypairs>],
    stakes: Vec<u64>,
    alpenglow_so_path: &str,
) -> GenesisConfigInfo {
    create_genesis_config_with_vote_accounts_and_cluster_type(
        mint_lamports,
        voting_keypairs,
        stakes,
        ClusterType::Development,
        Some(alpenglow_so_path),
    )
}

pub fn create_genesis_config_with_vote_accounts_and_cluster_type(
    mint_lamports: u64,
    voting_keypairs: &[impl Borrow<ValidatorVoteKeypairs>],
    stakes: Vec<u64>,
    cluster_type: ClusterType,
    alpenglow_so_path: Option<&str>,
) -> GenesisConfigInfo {
    assert!(!voting_keypairs.is_empty());
    assert_eq!(voting_keypairs.len(), stakes.len());

    let mint_keypair = Keypair::new();
    let voting_keypair = voting_keypairs[0].borrow().vote_keypair.insecure_clone();

    let validator_pubkey = voting_keypairs[0].borrow().node_keypair.pubkey();
    let genesis_config = create_genesis_config_with_leader_ex(
        mint_lamports,
        &mint_keypair.pubkey(),
        &validator_pubkey,
        &voting_keypairs[0].borrow().vote_keypair.pubkey(),
        &voting_keypairs[0].borrow().stake_keypair.pubkey(),
        Some(&voting_keypairs[0].borrow().bls_keypair.public.into()),
        stakes[0],
        VALIDATOR_LAMPORTS,
        FeeRateGovernor::new(0, 0), // most tests can't handle transaction fees
        Rent::free(),               // most tests don't expect rent
        cluster_type,
        vec![],
        alpenglow_so_path,
    );

    let mut genesis_config_info = GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        voting_keypair,
        validator_pubkey,
    };

    for (validator_voting_keypairs, stake) in voting_keypairs[1..].iter().zip(&stakes[1..]) {
        let node_pubkey = validator_voting_keypairs.borrow().node_keypair.pubkey();
        let vote_pubkey = validator_voting_keypairs.borrow().vote_keypair.pubkey();
        let stake_pubkey = validator_voting_keypairs.borrow().stake_keypair.pubkey();
        let bls_pubkey = validator_voting_keypairs.borrow().bls_keypair.public.into();

        // Create accounts
        let node_account = Account::new(VALIDATOR_LAMPORTS, 0, &system_program::id());
        let vote_account = if alpenglow_so_path.is_some() {
            AlpenglowVoteState::create_account_with_authorized(
                &node_pubkey,
                &vote_pubkey,
                &vote_pubkey,
                0,
                *stake,
                bls_pubkey,
            )
        } else {
            vote_state::create_account(&vote_pubkey, &node_pubkey, 0, *stake)
        };
        let stake_account = Account::from(stake_state::create_account(
            &stake_pubkey,
            &vote_pubkey,
            &vote_account,
            &genesis_config_info.genesis_config.rent,
            *stake,
        ));

        let vote_account = Account::from(vote_account);

        // Put newly created accounts into genesis
        genesis_config_info.genesis_config.accounts.extend(vec![
            (node_pubkey, node_account),
            (vote_pubkey, vote_account),
            (stake_pubkey, stake_account),
        ]);
    }

    genesis_config_info
}

pub fn create_genesis_config_with_leader(
    mint_lamports: u64,
    validator_pubkey: &Pubkey,
    validator_stake_lamports: u64,
) -> GenesisConfigInfo {
    // Use deterministic keypair so we don't get confused by randomness in tests
    let mint_keypair = Keypair::from_seed(&[
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ])
    .unwrap();

    create_genesis_config_with_leader_with_mint_keypair(
        mint_keypair,
        mint_lamports,
        validator_pubkey,
        validator_stake_lamports,
        None,
    )
}

#[cfg(feature = "dev-context-only-utils")]
pub fn create_genesis_config_with_alpenglow_vote_accounts_no_program(
    mint_lamports: u64,
    voting_keypairs: &[impl Borrow<ValidatorVoteKeypairs>],
    stakes: Vec<u64>,
) -> GenesisConfigInfo {
    create_genesis_config_with_alpenglow_vote_accounts(
        mint_lamports,
        voting_keypairs,
        stakes,
        build_alpenglow_vote::ALPENGLOW_VOTE_SO_PATH,
    )
}

#[cfg(feature = "dev-context-only-utils")]
pub fn create_genesis_config_with_leader_enable_alpenglow(
    mint_lamports: u64,
    validator_pubkey: &Pubkey,
    validator_stake_lamports: u64,
    alpenglow_so_path: Option<&str>,
) -> GenesisConfigInfo {
    // Use deterministic keypair so we don't get confused by randomness in tests
    let mint_keypair = Keypair::from_seed(&[
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ])
    .unwrap();

    create_genesis_config_with_leader_with_mint_keypair(
        mint_keypair,
        mint_lamports,
        validator_pubkey,
        validator_stake_lamports,
        alpenglow_so_path,
    )
}

pub fn create_genesis_config_with_leader_with_mint_keypair(
    mint_keypair: Keypair,
    mint_lamports: u64,
    validator_pubkey: &Pubkey,
    validator_stake_lamports: u64,
    alpenglow_so_path: Option<&str>,
) -> GenesisConfigInfo {
    // Use deterministic keypair so we don't get confused by randomness in tests
    let voting_keypair = Keypair::from_seed(&[
        32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
        55, 56, 57, 58, 59, 60, 61, 62, 63,
    ])
    .unwrap();

    let bls_keypair =
        BLSKeypair::derive_from_signer(&voting_keypair, BLS_KEYPAIR_DERIVE_SEED).unwrap();
    let bls_pubkey: BLSPubkey = bls_keypair.public.into();
    let genesis_config = create_genesis_config_with_leader_ex(
        mint_lamports,
        &mint_keypair.pubkey(),
        validator_pubkey,
        &voting_keypair.pubkey(),
        &Pubkey::new_unique(),
        Some(&bls_pubkey),
        validator_stake_lamports,
        VALIDATOR_LAMPORTS,
        FeeRateGovernor::new(0, 0), // most tests can't handle transaction fees
        Rent::free(),               // most tests don't expect rent
        ClusterType::Development,
        vec![],
        alpenglow_so_path,
    );

    GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        voting_keypair,
        validator_pubkey: *validator_pubkey,
    }
}

pub fn activate_all_features_alpenglow(genesis_config: &mut GenesisConfig) {
    do_activate_all_features::<true>(genesis_config);
}

pub fn activate_all_features(genesis_config: &mut GenesisConfig) {
    do_activate_all_features::<false>(genesis_config);
}

pub fn do_activate_all_features<const IS_ALPENGLOW: bool>(genesis_config: &mut GenesisConfig) {
    // Activate all features at genesis in development mode
    for feature_id in FeatureSet::default().inactive {
        if IS_ALPENGLOW || feature_id != solana_feature_set::secp256k1_program_enabled::id() {
            activate_feature(genesis_config, feature_id);
        }
    }
}

pub fn deactivate_features(
    genesis_config: &mut GenesisConfig,
    features_to_deactivate: &Vec<Pubkey>,
) {
    // Remove all features in `features_to_skip` from genesis
    for deactivate_feature_pk in features_to_deactivate {
        if FEATURE_NAMES.contains_key(deactivate_feature_pk) {
            genesis_config.accounts.remove(deactivate_feature_pk);
        } else {
            warn!(
                "Feature {:?} set for deactivation is not a known Feature public key",
                deactivate_feature_pk
            );
        }
    }
}

pub fn activate_feature(genesis_config: &mut GenesisConfig, feature_id: Pubkey) {
    genesis_config.accounts.insert(
        feature_id,
        Account::from(feature::create_account(
            &Feature {
                activated_at: Some(0),
            },
            std::cmp::max(genesis_config.rent.minimum_balance(Feature::size_of()), 1),
        )),
    );
}

pub fn include_alpenglow_bpf_program(genesis_config: &mut GenesisConfig, alpenglow_so_path: &str) {
    // Parse out the elf
    let mut program_data_elf: Vec<u8> = vec![];
    File::open(alpenglow_so_path)
        .and_then(|mut file| file.read_to_end(&mut program_data_elf))
        .unwrap_or_else(|err| {
            panic!(
                "Error: failed to read alpenglow-vote program from path {}: {}",
                alpenglow_so_path, err
            )
        });

    // Derive the address for the program data account
    let address = solana_votor_messages::id();
    let loader = solana_program::bpf_loader_upgradeable::id();
    let programdata_address =
        solana_program::bpf_loader_upgradeable::get_program_data_address(&address);

    // Generate the data for the program data account
    let upgrade_authority_address = system_program::id();
    let mut program_data = bincode::serialize(&UpgradeableLoaderState::ProgramData {
        slot: 0,
        upgrade_authority_address: Some(upgrade_authority_address),
    })
    .unwrap();
    program_data.extend_from_slice(&program_data_elf);

    // Store the program data account into genesis
    genesis_config.add_account(
        programdata_address,
        AccountSharedData::from(Account {
            lamports: genesis_config
                .rent
                .minimum_balance(program_data.len())
                .max(1u64),
            data: program_data,
            owner: loader,
            executable: false,
            rent_epoch: 0,
        }),
    );

    // Add the program acccount to genesis
    let program_data = bincode::serialize(&UpgradeableLoaderState::Program {
        programdata_address,
    })
    .unwrap();
    genesis_config.add_account(
        address,
        AccountSharedData::from(Account {
            lamports: genesis_config
                .rent
                .minimum_balance(program_data.len())
                .max(1u64),
            data: program_data,
            owner: loader,
            executable: true,
            rent_epoch: 0,
        }),
    );
}

#[allow(clippy::too_many_arguments)]
pub fn create_genesis_config_with_leader_ex_no_features(
    mint_lamports: u64,
    mint_pubkey: &Pubkey,
    validator_pubkey: &Pubkey,
    validator_vote_account_pubkey: &Pubkey,
    validator_stake_account_pubkey: &Pubkey,
    validator_bls_pubkey: Option<&BLSPubkey>,
    validator_stake_lamports: u64,
    validator_lamports: u64,
    fee_rate_governor: FeeRateGovernor,
    rent: Rent,
    cluster_type: ClusterType,
    mut initial_accounts: Vec<(Pubkey, AccountSharedData)>,
    alpenglow_so_path: Option<&str>,
) -> GenesisConfig {
    let validator_vote_account = if alpenglow_so_path.is_some() {
        AlpenglowVoteState::create_account_with_authorized(
            validator_pubkey,
            validator_vote_account_pubkey,
            validator_vote_account_pubkey,
            0,
            validator_stake_lamports,
            *validator_bls_pubkey.unwrap(),
        )
    } else {
        vote_state::create_account(
            validator_vote_account_pubkey,
            validator_pubkey,
            0,
            validator_stake_lamports,
        )
    };

    let validator_stake_account = stake_state::create_account(
        validator_stake_account_pubkey,
        validator_vote_account_pubkey,
        &validator_vote_account,
        &rent,
        validator_stake_lamports,
    );

    initial_accounts.push((
        *mint_pubkey,
        AccountSharedData::new(mint_lamports, 0, &system_program::id()),
    ));
    initial_accounts.push((
        *validator_pubkey,
        AccountSharedData::new(validator_lamports, 0, &system_program::id()),
    ));
    initial_accounts.push((*validator_vote_account_pubkey, validator_vote_account));
    initial_accounts.push((*validator_stake_account_pubkey, validator_stake_account));

    let native_mint_account = solana_sdk::account::AccountSharedData::from(Account {
        owner: solana_inline_spl::token::id(),
        data: solana_inline_spl::token::native_mint::ACCOUNT_DATA.to_vec(),
        lamports: sol_to_lamports(1.),
        executable: false,
        rent_epoch: 1,
    });
    initial_accounts.push((
        solana_inline_spl::token::native_mint::id(),
        native_mint_account,
    ));

    let mut genesis_config = GenesisConfig {
        accounts: initial_accounts
            .iter()
            .cloned()
            .map(|(key, account)| (key, Account::from(account)))
            .collect(),
        fee_rate_governor,
        rent,
        cluster_type,
        ..GenesisConfig::default()
    };

    solana_stake_program::add_genesis_accounts(&mut genesis_config);

    if let Some(alpenglow_so_path) = alpenglow_so_path {
        include_alpenglow_bpf_program(&mut genesis_config, alpenglow_so_path);
    }

    genesis_config
}

#[allow(clippy::too_many_arguments)]
pub fn create_genesis_config_with_leader_ex(
    mint_lamports: u64,
    mint_pubkey: &Pubkey,
    validator_pubkey: &Pubkey,
    validator_vote_account_pubkey: &Pubkey,
    validator_stake_account_pubkey: &Pubkey,
    validator_bls_pubkey: Option<&BLSPubkey>,
    validator_stake_lamports: u64,
    validator_lamports: u64,
    fee_rate_governor: FeeRateGovernor,
    rent: Rent,
    cluster_type: ClusterType,
    initial_accounts: Vec<(Pubkey, AccountSharedData)>,
    alpenglow_so_path: Option<&str>,
) -> GenesisConfig {
    let mut genesis_config = create_genesis_config_with_leader_ex_no_features(
        mint_lamports,
        mint_pubkey,
        validator_pubkey,
        validator_vote_account_pubkey,
        validator_stake_account_pubkey,
        validator_bls_pubkey,
        validator_stake_lamports,
        validator_lamports,
        fee_rate_governor,
        rent,
        cluster_type,
        initial_accounts,
        alpenglow_so_path,
    );

    if genesis_config.cluster_type == ClusterType::Development {
        if alpenglow_so_path.is_some() {
            activate_all_features_alpenglow(&mut genesis_config);
        } else {
            activate_all_features(&mut genesis_config);
        }
    }

    genesis_config
}
