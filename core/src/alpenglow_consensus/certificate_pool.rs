use {
    super::{
        certificate_limits_and_vote_types,
        vote_certificate::{CertificateError, VoteCertificate},
        vote_pool::VotePool,
        vote_type_to_certificate_type, Stake,
    },
    crate::alpenglow_consensus::{
        CertificateType, VoteType, MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE,
        MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES, MAX_SLOT_AGE, SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP,
        SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP, SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY,
        SAFE_TO_SKIP_THRESHOLD,
    },
    alpenglow_vote::vote::Vote,
    itertools::Itertools,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, epoch_stakes::EpochStakes},
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
        hash::Hash,
    },
    std::{
        collections::{hash_map::Entry, BTreeMap, HashMap},
        sync::Arc,
    },
    thiserror::Error,
};

impl VoteType {
    pub fn get_type(vote: &Vote) -> VoteType {
        match vote {
            Vote::Notarize(_) => VoteType::Notarize,
            Vote::NotarizeFallback(_) => VoteType::NotarizeFallback,
            Vote::Skip(_) => VoteType::Skip,
            Vote::SkipFallback(_) => VoteType::SkipFallback,
            Vote::Finalize(_) => VoteType::Finalize,
        }
    }
}

pub type PoolId = (Slot, VoteType);

pub type CertificateId = (Slot, CertificateType);

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    //    #[error("Conflicting vote type: {0:?} vs existing {1:?} for slot: {2} pubkey: {3}")]
    //    ConflictingVoteType(VoteType, VoteType, Slot, Pubkey),
    #[error("Epoch stakes missing for epoch: {0}")]
    EpochStakesNotFound(Epoch),

    #[error("Zero stake")]
    ZeroStake,

    #[error("Unrooted slot")]
    UnrootedSlot,

    #[error("Slot in the future")]
    SlotInFuture,

    #[error("Certificate error: {0}")]
    Certificate(#[from] CertificateError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewHighestCertificate {
    Notarize(Slot),
    Skip(Slot),
    Finalize(Slot),
}

impl NewHighestCertificate {
    pub fn is_notarization_or_skip(&self) -> bool {
        matches!(
            self,
            NewHighestCertificate::Notarize(_) | NewHighestCertificate::Skip(_)
        )
    }

    pub fn slot(&self) -> Slot {
        match self {
            NewHighestCertificate::Notarize(slot) => *slot,
            NewHighestCertificate::Skip(slot) => *slot,
            NewHighestCertificate::Finalize(slot) => *slot,
        }
    }
}

#[derive(Default)]
pub struct CertificatePool<VC: VoteCertificate> {
    // Vote pools to do bean counting for votes.
    vote_pools: BTreeMap<PoolId, VotePool<VC>>,
    // Certificate pools to keep track of the certs.
    certificates: BTreeMap<CertificateId, VC>,
    // Lookup table for checking conflicting vote types.
    // conflicting_vote_types: HashMap<VoteType, Vec<VoteType>>,
    // Highest slot with each certificate
    highest_slot_map: HashMap<CertificateType, Slot>,
    // Cached epoch_schedule
    epoch_schedule: EpochSchedule,
    // Cached epoch_stakes_map
    epoch_stakes_map: Arc<HashMap<Epoch, EpochStakes>>,
    // The current root, no need to save anything before this slot.
    root: Slot,
    // The epoch of current root.
    root_epoch: Epoch,
}

impl<VC: VoteCertificate> CertificatePool<VC> {
    pub fn new_from_root_bank(bank: &Bank) -> Self {
        let mut pool = Self::default();

        /*    // Initialize the conflicting_vote_types map
        for (vote_type_1, vote_type_2) in CONFLICTING_VOTETYPES.iter() {
            pool.conflicting_vote_types
                .entry(*vote_type_1)
                .or_insert_with(Vec::new)
                .push(*vote_type_2);
            pool.conflicting_vote_types
                .entry(*vote_type_2)
                .or_insert_with(Vec::new)
                .push(*vote_type_1);
        }*/

        // Update the epoch_stakes_map and root
        pool.update_epoch_stakes_map(bank);
        pool.root = bank.slot();

        pool
    }

    fn update_epoch_stakes_map(&mut self, bank: &Bank) {
        let epoch = bank.epoch();
        if self.epoch_stakes_map.is_empty() || epoch > self.root_epoch {
            self.epoch_stakes_map = Arc::new(bank.epoch_stakes_map().clone());
            self.root_epoch = epoch;
            self.epoch_schedule = bank.epoch_schedule().clone();
        }
    }

    // Return true if the new slot is greater than the current highest slot.
    fn set_highest_slot(&mut self, certificate_type: CertificateType, slot: Slot) -> bool {
        match self.highest_slot_map.entry(certificate_type) {
            Entry::Occupied(mut e) if slot > *e.get() => {
                e.insert(slot);
                true
            }
            Entry::Vacant(e) => {
                e.insert(slot);
                true
            }
            _ => false,
        }
    }

    fn new_vote_pool(vote_type: VoteType) -> VotePool<VC> {
        match vote_type {
            VoteType::NotarizeFallback => VotePool::new(MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE),
            _ => VotePool::new(MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES),
        }
    }

    fn update_vote_pool(
        &mut self,
        slot: Slot,
        vote_type: VoteType,
        bank_hash: Option<Hash>,
        block_id: Option<Hash>,
        transaction: Arc<VC::VoteTransaction>,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
    ) -> bool {
        let pool = self
            .vote_pools
            .entry((slot, vote_type))
            .or_insert_with(|| Self::new_vote_pool(vote_type));
        pool.add_vote(
            validator_vote_key,
            bank_hash,
            block_id,
            transaction,
            validator_stake,
        )
    }

    /// For a new vote `slot` , `vote_type` checks if any
    /// of the related certificates are newly complete.
    /// For each newly constructed certificate
    /// - Insert it into `self.certificates`
    /// - Potentially update the new highest certificate slot
    /// - If this the new highest certificate, return the type and slot
    fn update_certificates(
        &mut self,
        slot: Slot,
        vote_type: VoteType,
        bank_hash: Option<Hash>,
        block_id: Option<Hash>,
        total_stake: Stake,
    ) -> Vec<(CertificateType, Slot)> {
        vote_type_to_certificate_type(vote_type)
            .iter()
            .filter_map(|&cert_type| {
                // If the certificate is already complete, skip it
                if self.certificates.contains_key(&(slot, cert_type)) {
                    return None;
                }
                // Otherwise check whether the certificate is complete
                let (limit, vote_types) = certificate_limits_and_vote_types(cert_type);
                let accumulated_stake = vote_types
                    .iter()
                    .filter_map(|vote_type| {
                        Some(
                            self.vote_pools
                                .get(&(slot, *vote_type))?
                                .total_stake_by_key(bank_hash, block_id),
                        )
                    })
                    .sum::<Stake>();
                if accumulated_stake as f64 / (total_stake as f64) < limit {
                    return None;
                }
                let mut transactions = Vec::new();
                for vote_type in vote_types {
                    let Some(vote_pool) = self.vote_pools.get(&(slot, *vote_type)) else {
                        continue;
                    };
                    vote_pool.copy_out_transactions(bank_hash, block_id, &mut transactions);
                }
                // TODO: use an empty hash map of pubkeys for now since it is not clear
                // where to get the sorted list of validators yet

                // TODO: remove unwrap and properly handle unwrap
                self.certificates.insert(
                    (slot, cert_type),
                    VC::new(accumulated_stake, transactions, &HashMap::default()).unwrap(),
                );
                self.set_highest_slot(cert_type, slot)
                    .then_some((cert_type, slot))
            })
            .collect()
    }

    //TODO(wen): without cert retransmit this kills our local cluster test, enable later.
    /*    fn has_conflicting_vote(
            &self,
            slot: Slot,
            vote_type: VoteType,
            validator_vote_key: &Pubkey,
        ) -> Option<VoteType> {
            let conflicting_types = self.conflicting_vote_types.get(&vote_type)?;
            for conflicting_type in conflicting_types {
                if let Some(pool) = self.vote_pools.get(&(slot, *conflicting_type)) {
                    if pool.has_prev_vote(validator_vote_key) {
                        return Some(*conflicting_type);
                    }
                }
            }
            None
        }
    */

    /// Adds the new vote the the certificate pool.
    ///
    /// If this resulted in a new highest Finalize or FastFinalize certificate,
    /// return the slot
    pub fn add_vote(
        &mut self,
        vote: &Vote,
        transaction: VC::VoteTransaction,
        validator_vote_key: &Pubkey,
    ) -> Result<Option<Slot>, AddVoteError> {
        let slot = vote.slot();
        let transaction = Arc::new(transaction);
        let epoch = self.epoch_schedule.get_epoch(slot);
        let Some(epoch_stakes) = self.epoch_stakes_map.get(&epoch) else {
            return Err(AddVoteError::EpochStakesNotFound(epoch));
        };
        let validator_stake = epoch_stakes.vote_account_stake(validator_vote_key);
        let total_stake = epoch_stakes.total_stake();

        if validator_stake == 0 {
            return Err(AddVoteError::ZeroStake);
        }

        if slot < self.root {
            return Err(AddVoteError::UnrootedSlot);
        }
        // We only allow votes
        if slot > self.root + MAX_SLOT_AGE {
            return Err(AddVoteError::SlotInFuture);
        }
        let vote_type = VoteType::get_type(vote);
        let (bank_hash, block_id) = match vote {
            Vote::Notarize(vote) => (Some(*vote.replayed_bank_hash()), Some(*vote.block_id())),
            Vote::NotarizeFallback(vote) => {
                (Some(*vote.replayed_bank_hash()), Some(*vote.block_id()))
            }
            _ => (None, None),
        };
        //TODO(wen): without cert retransmit this kills our local cluster test, enable later.
        /*        if let Some(conflicting_type) =
                    self.has_conflicting_vote(slot, vote_type, validator_vote_key)
                {
                    return Err(AddVoteError::ConflictingVoteType(
                        vote_type,
                        conflicting_type,
                        slot,
                        *validator_vote_key,
                    ));
                }
        */
        if !self.update_vote_pool(
            slot,
            vote_type,
            bank_hash,
            block_id,
            transaction.clone(),
            validator_vote_key,
            validator_stake,
        ) {
            return Ok(None);
        }
        let new_certs = self.update_certificates(slot, vote_type, bank_hash, block_id, total_stake);
        Ok(new_certs
            .iter()
            .filter_map(|(ct, s)| ct.is_finalization_variant().then_some(s))
            .max()
            .copied())
    }

    pub fn highest_not_skip_certificate_slot(&self) -> Slot {
        self.highest_slot_map
            .iter()
            .filter(|(certificate_type, _)| **certificate_type != CertificateType::Skip)
            .map(|(_, slot)| *slot)
            .max()
            .unwrap_or(0)
    }

    pub fn highest_notarized_slot(&self) -> Slot {
        // Return the max of CertificateType::Notarize and CertificateType::NotarizeFallback
        [CertificateType::Notarize, CertificateType::NotarizeFallback]
            .iter()
            .filter_map(|cert_type| self.highest_slot_map.get(cert_type))
            .copied()
            .max()
            .unwrap_or(0)
    }

    pub fn highest_skip_slot(&self) -> Slot {
        self.highest_slot_map
            .get(&CertificateType::Skip)
            .copied()
            .unwrap_or(0)
    }

    pub fn highest_finalized_slot(&self) -> Slot {
        [CertificateType::Finalize, CertificateType::FinalizeFast]
            .iter()
            .filter_map(|cert_type| self.highest_slot_map.get(cert_type))
            .copied()
            .max()
            .unwrap_or(0)
    }

    fn find_first_in_certificates(
        &self,
        slot: Slot,
        certificate_types: &[CertificateType],
    ) -> Option<usize> {
        certificate_types
            .iter()
            .filter_map(|&cert_type| {
                self.certificates
                    .get(&(slot, cert_type))
                    .map(|x| x.vote_count())
            })
            .find_or_first(|x| x.is_some())?
    }

    pub fn get_finalization_cert_size(&self, slot: Slot) -> Option<usize> {
        self.find_first_in_certificates(
            slot,
            &[CertificateType::FinalizeFast, CertificateType::Finalize],
        )
    }

    pub fn get_notarization_cert_size(&self, slot: Slot) -> Option<usize> {
        self.find_first_in_certificates(
            slot,
            &[CertificateType::Notarize, CertificateType::NotarizeFallback],
        )
    }

    pub fn skip_certified(&self, slot: Slot) -> bool {
        self.certificates
            .contains_key(&(slot, CertificateType::Skip))
    }

    fn check_self_skip_or_notarized_different(
        &self,
        my_pubkey: &Pubkey,
        slot: Slot,
        bank_hash: Option<Hash>,
        block_id: Option<Hash>,
    ) -> bool {
        if self
            .vote_pools
            .get(&(slot, VoteType::Skip))
            .is_some_and(|pool| pool.has_prev_vote(my_pubkey))
        {
            return true;
        }
        let Some(notarize_pool) = self.vote_pools.get(&(slot, VoteType::Notarize)) else {
            return false;
        };
        notarize_pool.first_prev_vote_different(my_pubkey, bank_hash, block_id)
    }

    pub fn safe_to_notar(&self, my_pubkey: &Pubkey, bank: &Bank) -> bool {
        if bank.slot() < self.root || !bank.is_frozen() {
            return false;
        }
        let bank_hash = Some(bank.hash());
        let block_id = bank.block_id();
        let slot = bank.slot();
        let Some(epoch_stakes) = self.epoch_stakes_map.get(&bank.epoch()) else {
            return false;
        };
        let total_stake = epoch_stakes.total_stake();

        // Check if I voted skip or notarize on a different hash
        if !self.check_self_skip_or_notarized_different(my_pubkey, slot, bank_hash, block_id) {
            return false;
        }

        // Check if 40% of stake holders voted notarize
        let notarized_ratio =
            self.vote_pools
                .get(&(slot, VoteType::Notarize))
                .map_or(0, |pool| pool.total_stake_by_key(bank_hash, block_id)) as f64
                / total_stake as f64;
        if notarized_ratio >= SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY {
            return true;
        }

        // Now check if 20% notarized, and 60% either notarized or skip
        if notarized_ratio < SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP {
            return false;
        }
        let skip_ratio = self
            .vote_pools
            .get(&(slot, VoteType::Skip))
            .map_or(0, |pool| pool.total_stake_by_key(None, None)) as f64
            / total_stake as f64;
        notarized_ratio + skip_ratio >= SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP
    }

    pub fn safe_to_skip(&self, my_pubkey: &Pubkey, slot: Slot) -> bool {
        if slot < self.root {
            return false;
        }
        let epoch = self.epoch_schedule.get_epoch(slot);
        let Some(epoch_stakes) = self.epoch_stakes_map.get(&epoch) else {
            return false;
        };
        let total_stake = epoch_stakes.total_stake();

        let Some(notarize_pool) = self.vote_pools.get(&(slot, VoteType::Notarize)) else {
            return false;
        };
        // Check if I voted notarize for some hash
        if !notarize_pool.first_prev_vote_different(my_pubkey, None, None) {
            return false;
        }
        let voted_stake = notarize_pool.total_stake()
            + self
                .vote_pools
                .get(&(slot, VoteType::Skip))
                .map_or(0, |pool| pool.total_stake());
        let top_notarized_stake = notarize_pool.top_entry_stake();
        (voted_stake - top_notarized_stake) as f64 / total_stake as f64 >= SAFE_TO_SKIP_THRESHOLD
    }

    /// Determines if the leader can start based on notarization and skip certificates.
    pub fn make_start_leader_decision(
        &self,
        my_leader_slot: Slot,
        parent_slot: Slot,
        first_alpenglow_slot: Slot,
    ) -> bool {
        // TODO: for GCE tests we WFSM on 1 so slot 1 is exempt
        let needs_notarization_certificate = parent_slot >= first_alpenglow_slot && parent_slot > 1;

        if needs_notarization_certificate
            && self.get_notarization_cert_size(parent_slot).is_none()
            && self.get_finalization_cert_size(parent_slot).is_none()
        {
            error!("Missing notarization certificate {parent_slot}");
            return false;
        }

        let needs_skip_certificate =
            // handles cases where we are entering the alpenglow epoch, where the first
            // slot in the epoch will pass my_leader_slot == parent_slot
            my_leader_slot != first_alpenglow_slot &&
            my_leader_slot != parent_slot + 1;

        if needs_skip_certificate {
            let begin_skip_slot = first_alpenglow_slot.max(parent_slot + 1);
            for slot in begin_skip_slot..my_leader_slot {
                if !self.skip_certified(slot) {
                    error!(
                        "Missing skip certificate for {slot}, required for skip ceritifcate \
                        from {begin_skip_slot} to build {my_leader_slot}"
                    );
                    return false;
                }
            }
        }

        true
    }

    /// Cleanup old finalized slots from the certificate pool
    pub fn handle_new_root(&mut self, bank: Arc<Bank>) {
        // `certificates`` now only contains entries >= `finalized_slot`
        self.certificates = self
            .certificates
            .split_off(&(bank.slot(), CertificateType::Finalize));
        self.vote_pools = self
            .vote_pools
            .split_off(&(bank.slot(), VoteType::Finalize));
        self.update_epoch_stakes_map(&bank);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{super::vote_certificate::LegacyVoteCertificate, *},
        solana_runtime::{
            bank::{Bank, NewBankOptions},
            bank_forks::BankForks,
            genesis_utils::{create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs},
        },
        solana_sdk::{
            clock::Slot, hash::Hash, pubkey::Pubkey, signer::Signer,
            transaction::VersionedTransaction,
        },
        std::sync::{Arc, RwLock},
    };

    fn dummy_transaction() -> VersionedTransaction {
        VersionedTransaction::default()
    }

    fn create_bank(slot: Slot, parent: Arc<Bank>, pubkey: &Pubkey) -> Bank {
        Bank::new_from_parent_with_options(parent, pubkey, slot, NewBankOptions::default())
    }

    fn create_bank_forks(validator_keypairs: &[ValidatorVoteKeypairs]) -> Arc<RwLock<BankForks>> {
        let genesis = create_genesis_config_with_vote_accounts(
            1_000_000_000,
            validator_keypairs,
            vec![100; validator_keypairs.len()],
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        BankForks::new_rw_arc(bank0)
    }

    fn create_keypairs_and_pool() -> (
        Vec<ValidatorVoteKeypairs>,
        CertificatePool<LegacyVoteCertificate>,
    ) {
        // Create 10 node validatorvotekeypairs vec
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let bank_forks = create_bank_forks(&validator_keypairs);
        let root_bank = bank_forks.read().unwrap().root_bank();
        (
            validator_keypairs,
            CertificatePool::new_from_root_bank(&root_bank.clone()),
        )
    }

    fn add_certificate(
        pool: &mut CertificatePool<LegacyVoteCertificate>,
        validator_keypairs: &[ValidatorVoteKeypairs],
        vote: Vote,
    ) {
        for keys in validator_keypairs.iter().take(6) {
            assert!(pool
                .add_vote(&vote, dummy_transaction(), &keys.vote_keypair.pubkey())
                .is_ok());
        }
        assert!(pool
            .add_vote(
                &vote,
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        match vote {
            Vote::Notarize(vote) => assert_eq!(pool.highest_notarized_slot(), vote.slot()),
            Vote::NotarizeFallback(vote) => assert_eq!(pool.highest_notarized_slot(), vote.slot()),
            Vote::Skip(vote) => assert_eq!(pool.highest_skip_slot(), vote.slot()),
            Vote::SkipFallback(vote) => assert_eq!(pool.highest_skip_slot(), vote.slot()),
            Vote::Finalize(vote) => assert_eq!(pool.highest_finalized_slot(), vote.slot()),
        }
    }

    fn add_skip_vote_range(
        pool: &mut CertificatePool<LegacyVoteCertificate>,
        start: Slot,
        end: Slot,
        pubkey: Pubkey,
    ) {
        for slot in start..=end {
            assert!(pool
                .add_vote(&Vote::new_skip_vote(slot), dummy_transaction(), &pubkey,)
                .is_ok());
        }
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_notarization_missing() {
        let (_, pool) = create_keypairs_and_pool();

        // No notarization set, pool is default
        let parent_slot = 2;
        let my_leader_slot = 3;
        let first_alpenglow_slot = 0;
        let decision =
            pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot);
        assert!(
            !decision,
            "Leader should not be allowed to start without notarization"
        );
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_1() {
        let (_, pool) = create_keypairs_and_pool();

        // If parent_slot == 0, you don't need a notarization certificate
        // Because leader_slot == parent_slot + 1, you don't need a skip certificate
        let parent_slot = 0;
        let my_leader_slot = 1;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_2() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // If parent_slot < first_alpenglow_slot, and parent_slot > 0
        // no notarization certificate is required, but a skip
        // certificate will be
        let parent_slot = 1;
        let my_leader_slot = 3;
        let first_alpenglow_slot = 2;

        assert!(!pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
        ));

        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_skip_vote(first_alpenglow_slot),
        );

        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_3() {
        let (_, pool) = create_keypairs_and_pool();
        // If parent_slot == first_alpenglow_slot, and
        // first_alpenglow_slot > 0, you need a notarization certificate
        let parent_slot = 2;
        let my_leader_slot = 3;
        let first_alpenglow_slot = 2;
        assert!(!pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
        ));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_4() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // If parent_slot < first_alpenglow_slot, and parent_slot == 0,
        // no notarization certificate is required, but a skip certificate will
        // be
        let parent_slot = 0;
        let my_leader_slot = 2;
        let first_alpenglow_slot = 1;

        assert!(!pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
        ));

        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_skip_vote(first_alpenglow_slot),
        );
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_5() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Valid skip certificate for 1-9 exists
        for slot in 1..=9 {
            add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }

        // Parent slot is equal to 0, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 0;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_6() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Valid skip certificate for 1-9 exists
        for slot in 1..=9 {
            add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }
        // Parent slot is less than first_alpenglow_slot, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 4;
        let first_alpenglow_slot = 5;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_skip_certificate_missing() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        let bank_forks = create_bank_forks(&validator_keypairs);
        let my_pubkey = validator_keypairs[0].vote_keypair.pubkey();

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        bank.freeze();
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // No skip certificate for 6-10
        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        let decision =
            pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot);
        assert!(
            !decision,
            "Leader should not be allowed to start if a skip certificate is missing"
        );
    }

    #[test]
    fn test_make_decision_leader_starts_when_no_skip_required() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Notarize slot 5
        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Leader slot is just +1 from notarized slot (no skip needed)
        let my_leader_slot = 6;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_leader_starts_if_notarized_and_skips_valid() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Notarize slot 5
        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Valid skip certificate for 6-9 exists
        for slot in 6..=9 {
            add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_leader_starts_if_skip_range_superset() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Notarize slot 5
        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Valid skip certificate for 4-9 exists
        // Should start leader block even if the beginning of the range is from
        // before your last notarized slot
        for slot in 4..=9 {
            add_certificate(
                &mut pool,
                &validator_keypairs,
                Vote::new_skip_fallback_vote(slot),
            );
        }

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_add_vote_new_finalize_certificate() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_finalized_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_finalized_slot(), 0);
        for keys in validator_keypairs.iter().take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_finalization_vote(5),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_finalized_slot(), 0);
        assert!(pool
            .add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_finalized_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_notarize_certificate() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 0);

        for keys in validator_keypairs.iter().take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_notarized_slot(), 0);
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_notarize_fallback_certificate() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 10% voted for notarize_fallback 5
        let pubkey = validator_keypairs[4].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_notarization_fallback_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 0);
        // 20% voted for notarize 5, 20% voted for notarize_fallback 5
        for keys in validator_keypairs.iter().take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        for keys in validator_keypairs.iter().skip(2).take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_notarized_slot(), 0);
        // Another 10% voted for notarize_fallback 5, we are over the threshold
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction(),
                &validator_keypairs[5].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_skip_certificate() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(&Vote::new_skip_vote(5), dummy_transaction(), &pubkey)
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(&Vote::new_skip_vote(5), dummy_transaction(), &pubkey,)
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        for keys in validator_keypairs.iter().take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(5),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_skip_slot(), 0);
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(5),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_skip_fallback_certificate() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 10% voted for skip_fallback 5
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_skip_fallback_vote(5),
                dummy_transaction(),
                &pubkey
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(
                &Vote::new_skip_fallback_vote(5),
                dummy_transaction(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        // 20% voted for skip 5, 20% voted for skip_fallback 5
        for keys in validator_keypairs.iter().take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(5),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        for keys in validator_keypairs.iter().skip(2).take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_fallback_vote(5),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_skip_slot(), 0);
        // Another 10% voted for skip_fallback 5, we are over the threshold
        assert!(pool
            .add_vote(
                &Vote::new_skip_fallback_vote(5),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 5);
    }

    #[test]
    fn test_add_vote_zero_stake() {
        let (_, mut pool) = create_keypairs_and_pool();

        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(5),
                dummy_transaction(),
                &Pubkey::new_unique()
            ),
            Err(AddVoteError::ZeroStake)
        );
    }

    fn assert_single_certificate_range(
        pool: &CertificatePool<LegacyVoteCertificate>,
        exp_range_start: Slot,
        exp_range_end: Slot,
    ) {
        for i in exp_range_start..=exp_range_end {
            assert!(pool.skip_certified(i));
        }
    }

    #[test]
    fn test_consecutive_slots() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(15));
        assert_eq!(pool.highest_skip_slot(), 15);

        for (i, keypairs) in validator_keypairs.iter().enumerate() {
            let slot = i as u64 + 16;
            // These should not extend the skip range
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(slot),
                    dummy_transaction(),
                    &keypairs.vote_keypair.pubkey()
                )
                .is_ok());
        }

        assert_single_certificate_range(&pool, 15, 15);
    }

    #[test]
    fn test_multi_skip_cert() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // We have 10 validators, 40% voted for (5, 15)
        for pubkeys in validator_keypairs.iter().take(4) {
            add_skip_vote_range(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        // 30% voted for (5, 8)
        for pubkeys in validator_keypairs.iter().skip(4).take(3) {
            add_skip_vote_range(&mut pool, 5, 8, pubkeys.vote_keypair.pubkey());
        }
        // The rest voted for (11, 15)
        for pubkeys in validator_keypairs.iter().skip(7) {
            add_skip_vote_range(&mut pool, 11, 15, pubkeys.vote_keypair.pubkey());
        }
        // Test slots from 5 to 15, (5, 8) and (11, 15) should be certified, the others aren't
        for slot in 5..=15 {
            if slot > 8 && slot < 11 {
                assert!(!pool.skip_certified(slot));
            } else {
                assert!(pool.skip_certified(slot));
            }
        }
    }

    #[test]
    fn test_add_multiple_votes() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // 10 validators, half vote for (5, 15), the other (20, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        for pubkeys in validator_keypairs.iter().skip(5) {
            add_skip_vote_range(&mut pool, 20, 30, pubkeys.vote_keypair.pubkey());
        }
        assert_eq!(pool.highest_skip_slot(), 0);

        // Now the first half vote for (5, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range(&mut pool, 5, 30, pubkeys.vote_keypair.pubkey());
        }
        assert_single_certificate_range(&pool, 20, 30);
    }

    #[test]
    fn test_add_multiple_disjoint_votes() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 50% of the validators vote for (1, 10)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range(&mut pool, 1, 10, pubkeys.vote_keypair.pubkey());
        }
        // 10% vote for (2, 2)
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(2),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 2);

        assert_single_certificate_range(&pool, 2, 2);
        // 10% vote for (4, 4)
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(4),
                dummy_transaction(),
                &validator_keypairs[7].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 4);

        assert_single_certificate_range(&pool, 2, 2);
        assert_single_certificate_range(&pool, 4, 4);
        // 10% vote for (3, 3)
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(3),
                dummy_transaction(),
                &validator_keypairs[8].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 4);
        assert_single_certificate_range(&pool, 2, 4);
        assert!(pool.skip_certified(3));
        // Let the last 10% vote for (3, 10) now
        add_skip_vote_range(
            &mut pool,
            3,
            10,
            validator_keypairs[8].vote_keypair.pubkey(),
        );
        assert_eq!(pool.highest_skip_slot(), 10);
        assert_single_certificate_range(&pool, 2, 10);
        assert!(pool.skip_certified(7));
    }

    #[test]
    fn test_update_existing_singleton_vote() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 50% voted on (1, 6)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range(&mut pool, 1, 6, pubkeys.vote_keypair.pubkey());
        }
        // Range expansion on a singleton vote should be ok
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(1),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey()
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 1);
        add_skip_vote_range(&mut pool, 1, 6, validator_keypairs[6].vote_keypair.pubkey());
        assert_eq!(pool.highest_skip_slot(), 6);
        assert_single_certificate_range(&pool, 1, 6);
    }

    #[test]
    fn test_update_existing_vote() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 50% voted for (10, 25)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range(&mut pool, 10, 25, pubkeys.vote_keypair.pubkey());
        }
        let pubkey = validator_keypairs[6].vote_keypair.pubkey();

        add_skip_vote_range(&mut pool, 10, 20, pubkey);
        assert_eq!(pool.highest_skip_slot(), 20);
        assert_single_certificate_range(&pool, 10, 20);

        // AlreadyExists, silently fail
        assert!(pool
            .add_vote(&Vote::new_skip_vote(20), dummy_transaction(), &pubkey)
            .is_ok());
    }

    #[test]
    fn test_threshold_not_reached() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // half voted (5, 15) and the other half voted (20, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        for pubkeys in validator_keypairs.iter().skip(5) {
            add_skip_vote_range(&mut pool, 20, 30, pubkeys.vote_keypair.pubkey());
        }
        for slot in 5..31 {
            assert!(!pool.skip_certified(slot));
        }
    }

    #[test]
    fn test_update_and_skip_range_certify() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // half voted (5, 15) and the other half voted (10, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        for pubkeys in validator_keypairs.iter().skip(5) {
            add_skip_vote_range(&mut pool, 10, 30, pubkeys.vote_keypair.pubkey());
        }
        for slot in 5..10 {
            assert!(!pool.skip_certified(slot));
        }
        for slot in 16..31 {
            assert!(!pool.skip_certified(slot));
        }
        assert_single_certificate_range(&pool, 10, 15);
    }

    #[test]
    fn test_safe_to_notar() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let bank_forks = create_bank_forks(&validator_keypairs);
        let my_pubkey = validator_keypairs[0].vote_keypair.pubkey();

        // Create bank 2
        let bank2 = Arc::new(create_bank(
            2,
            bank_forks.read().unwrap().get(0).unwrap(),
            &my_pubkey,
        ));
        bank2.set_block_id(Some(Hash::new_unique()));
        bank2.freeze();

        // With no votes, this should fail.
        assert!(!pool.safe_to_notar(&my_pubkey, &bank2.clone()));

        // Add a skip from myself.
        assert!(pool
            .add_vote(&Vote::new_skip_vote(2), dummy_transaction(), &my_pubkey,)
            .is_ok());
        // 40% notarized, should succeed
        for keypairs in validator_keypairs.iter().skip(1).take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(2, bank2.block_id().unwrap(), bank2.hash()),
                    dummy_transaction(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(pool.safe_to_notar(&my_pubkey, &bank2.clone()));

        // Create bank 3
        let bank3 = Arc::new(create_bank(3, bank2, &my_pubkey));
        bank3.set_block_id(Some(Hash::new_unique()));
        bank3.freeze();

        // Add 20% notarize, but no vote from myself, should fail
        for keypairs in validator_keypairs.iter().skip(1).take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(3, bank3.block_id().unwrap(), bank3.hash()),
                    dummy_transaction(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(!pool.safe_to_notar(&my_pubkey, &bank3.clone()));

        // Add a notarize from myself for some other block, but still not enough notar or skip, should fail.
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(3, Hash::new_unique(), Hash::new_unique()),
                dummy_transaction(),
                &my_pubkey,
            )
            .is_ok());
        assert!(!pool.safe_to_notar(&my_pubkey, &bank3.clone()));

        // Now add 40% skip, should succeed
        for keypairs in validator_keypairs.iter().skip(3).take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(3),
                    dummy_transaction(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(pool.safe_to_notar(&my_pubkey, &bank3.clone()));
    }

    #[test]
    fn test_safe_to_skip() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let my_pubkey = validator_keypairs[0].vote_keypair.pubkey();
        // No vote from myself, should fail.
        assert!(!pool.safe_to_skip(&my_pubkey, 2));

        // Add a notarize from myself.
        let block_id = Hash::new_unique();
        let block_hash = Hash::new_unique();
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(2, block_id, block_hash),
                dummy_transaction(),
                &my_pubkey,
            )
            .is_ok());
        // Should still fail because there are no other votes.
        assert!(!pool.safe_to_skip(&my_pubkey, 2));
        // Add 50% skip, should succeed
        for keypairs in validator_keypairs.iter().skip(1).take(5) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(2),
                    dummy_transaction(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(pool.safe_to_skip(&my_pubkey, 2));
        // Add 10% more notarize, still safe to skip any more because total voted increased.
        for keypairs in validator_keypairs.iter().skip(6).take(1) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(2, block_id, block_hash),
                    dummy_transaction(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(pool.safe_to_skip(&my_pubkey, 2));
    }

    /*
    fn create_new_vote(vote_type: VoteType, slot: Slot) -> Vote {
        match vote_type {
            VoteType::Notarize => {
                Vote::new_notarization_vote(slot, Hash::default(), Hash::default())
            }
            VoteType::NotarizeFallback => {
                Vote::new_notarization_fallback_vote(slot, Hash::default(), Hash::default())
            }
            VoteType::Skip => Vote::new_skip_vote(slot),
            VoteType::SkipFallback => Vote::new_skip_fallback_vote(slot),
            VoteType::Finalize => Vote::new_finalization_vote(slot),
        }
    }


        fn test_reject_conflicting_vote(
            pool: &mut CertificatePool,
            pubkey: &Pubkey,
            vote_type_1: VoteType,
            vote_type_2: VoteType,
            slot: Slot,
        ) {
            let vote_1 = create_new_vote(vote_type_1, slot);
            let vote_2 = create_new_vote(vote_type_2, slot);
            assert!(pool.add_vote(&vote_1, dummy_transaction(), pubkey).is_ok());
            assert!(pool.add_vote(&vote_2, dummy_transaction(), pubkey).is_err());
        }

        #[test]
        fn test_reject_conflicting_votes() {
            let (validator_keypairs, mut pool) = create_keypairs_and_pool();
            let mut slot = 2;
            for (vote_type_1, vote_type_2) in CONFLICTING_VOTETYPES.iter() {
                let pubkey = validator_keypairs[0].vote_keypair.pubkey();
                test_reject_conflicting_vote(&mut pool, &pubkey, *vote_type_1, *vote_type_2, slot);
                test_reject_conflicting_vote(&mut pool, &pubkey, *vote_type_2, *vote_type_1, slot + 1);
                slot += 2;
            }
        }
    */
}
