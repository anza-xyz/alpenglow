use {
    super::{vote_certificate::VoteCertificate, Stake},
    alpenglow_vote::vote::Vote,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, epoch_stakes::EpochStakes},
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
        transaction::VersionedTransaction,
    },
    std::{
        collections::{hash_map::Entry, BTreeMap, HashMap},
        hash::Hash,
        sync::Arc,
    },
    thiserror::Error,
};

pub type CertificateId = (Slot, CertificateType);

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    #[error("Epoch stakes missing for epoch: {0}")]
    EpochStakesNotFound(Epoch),

    #[error("Zero stake")]
    ZeroStake,

    #[error("Unrooted slot")]
    UnrootedSlot,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CertificateType {
    Notarize,
    Skip,
    Finalize,
}

impl CertificateType {
    #[inline]
    fn get_type(vote: &Vote) -> CertificateType {
        match vote {
            Vote::Notarize(_) => CertificateType::Notarize,
            Vote::Finalize(_) => CertificateType::Finalize,
            Vote::Skip(_) => CertificateType::Skip,
            _ => todo!(),
        }
    }
}

#[derive(Default)]
pub struct CertificatePool {
    // Notarization, finalization and skip vote certificates
    certificates: BTreeMap<CertificateId, VoteCertificate>,
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

impl CertificatePool {
    pub fn new_from_root_bank(bank: &Bank) -> Self {
        let mut pool = Self::default();
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

    fn update_certificate_pool(
        &mut self,
        slot: Slot,
        vote: &Vote,
        transaction: Arc<VersionedTransaction>,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
        total_stake: Stake,
    ) -> Option<Slot> {
        let certificate_type = CertificateType::get_type(vote);
        let certificate = self
            .certificates
            .entry((slot, certificate_type.clone()))
            .or_insert_with(|| VoteCertificate::new(slot));
        if !certificate.add_vote(
            validator_vote_key,
            transaction,
            validator_stake,
            total_stake,
        ) {
            return None;
        }

        if certificate.is_complete() && self.set_highest_slot(certificate_type, slot) {
            return Some(slot);
        }

        None
    }

    pub fn add_vote(
        &mut self,
        vote: &Vote,
        transaction: VersionedTransaction,
        validator_vote_key: &Pubkey,
    ) -> Result<Option<NewHighestCertificate>, AddVoteError> {
        let slot = vote.slot();
        let certificate_type = CertificateType::get_type(vote);
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

        let new_highest_slot = self.update_certificate_pool(
            slot,
            vote,
            transaction.clone(),
            validator_vote_key,
            validator_stake,
            total_stake,
        );

        Ok(new_highest_slot.map(|slot| match certificate_type {
            CertificateType::Skip => NewHighestCertificate::Skip(slot),
            CertificateType::Notarize => NewHighestCertificate::Notarize(slot),
            CertificateType::Finalize => NewHighestCertificate::Finalize(slot),
        }))
    }

    /// If complete, returns Some(size), the size of the certificate
    pub fn is_notarization_certificate_complete(&self, slot: Slot) -> Option<usize> {
        self.certificates
            .get(&(slot, CertificateType::Notarize))
            .and_then(|certificate| certificate.is_complete().then_some(certificate.size()))
    }

    fn get_certificate(
        &self,
        slot: Slot,
        certificate_type: CertificateType,
    ) -> Option<Vec<Arc<VersionedTransaction>>> {
        self.certificates
            .get(&(slot, certificate_type))
            .and_then(|certificate| {
                certificate
                    .get_certificate_iter_for_complete_cert()
                    .map(|iter| iter.map(|(_, entry)| entry.clone()).collect())
            })
    }

    pub fn get_notarization_certificate(
        &self,
        slot: Slot,
    ) -> Option<Vec<Arc<VersionedTransaction>>> {
        self.get_certificate(slot, CertificateType::Notarize)
    }

    pub fn get_finalization_certificate(
        &self,
        slot: Slot,
    ) -> Option<Vec<Arc<VersionedTransaction>>> {
        self.get_certificate(slot, CertificateType::Finalize)
    }

    pub fn highest_certificate_slot(&self) -> Slot {
        self.highest_slot_map.values().max().copied().unwrap_or(0)
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
        *self
            .highest_slot_map
            .get(&CertificateType::Notarize)
            .unwrap_or(&0)
    }

    pub fn highest_skip_slot(&self) -> Slot {
        *self
            .highest_slot_map
            .get(&CertificateType::Skip)
            .unwrap_or(&0)
    }

    pub fn highest_finalized_slot(&self) -> Slot {
        *self
            .highest_slot_map
            .get(&CertificateType::Finalize)
            .unwrap_or(&0)
    }

    pub fn is_finalized_slot(&self, slot: Slot) -> bool {
        self.certificates
            .get(&(slot, CertificateType::Finalize))
            .map(|certificate| certificate.is_complete())
            .unwrap_or(false)
    }

    pub fn skip_certified(&self, slot: Slot) -> bool {
        self.certificates
            .get(&(slot, CertificateType::Skip))
            .map(|certificate| certificate.is_complete())
            .unwrap_or(false)
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
            && self
                .is_notarization_certificate_complete(parent_slot)
                .is_none()
            && !self.is_finalized_slot(parent_slot)
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
            .split_off(&(bank.slot(), CertificateType::Notarize));
        self.update_epoch_stakes_map(&bank);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
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

    fn create_keypairs_and_pool() -> (Vec<ValidatorVoteKeypairs>, CertificatePool) {
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
        pool: &mut CertificatePool,
        validator_keypairs: &[ValidatorVoteKeypairs],
        vote: Vote,
    ) {
        for keys in validator_keypairs.iter().take(6) {
            assert_eq!(
                pool.add_vote(&vote, dummy_transaction(), &keys.vote_keypair.pubkey(),)
                    .unwrap(),
                None
            );
        }
        assert_eq!(
            pool.add_vote(
                &vote,
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .unwrap()
            .unwrap(),
            match vote {
                Vote::Notarize(vote) => NewHighestCertificate::Notarize(vote.slot()),
                Vote::Skip(vote) => NewHighestCertificate::Skip(vote.slot()),
                Vote::Finalize(vote) => NewHighestCertificate::Finalize(vote.slot()),
                Vote::NotarizeFallback(_vote) => todo!(),
                Vote::SkipFallback(_vote) => todo!(),
            },
        );
    }

    fn add_skip_vote_range(pool: &mut CertificatePool, start: Slot, end: Slot, pubkey: Pubkey) {
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
            add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
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
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction(),
                &pubkey,
            ),
            Ok(None)
        );
        for keys in validator_keypairs.iter().take(5) {
            assert!(pool
                .add_vote(
                    &Vote::new_finalization_vote(5),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .unwrap()
                .is_none());
        }
        assert_eq!(
            pool.add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Finalize(5)
        );
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
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction(),
                &pubkey,
            ),
            Ok(None)
        );
        for keys in validator_keypairs.iter().take(5) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .unwrap()
                .is_none());
        }
        assert_eq!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Notarize(5)
        );
    }

    #[test]
    fn test_add_vote_new_skip_certificate() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(&Vote::new_skip_vote(5), dummy_transaction(), &pubkey,)
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(&Vote::new_skip_vote(5), dummy_transaction(), &pubkey,),
            Ok(None)
        );
        for keys in validator_keypairs.iter().take(5) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(5),
                    dummy_transaction(),
                    &keys.vote_keypair.pubkey(),
                )
                .unwrap()
                .is_none());
        }
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(5),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(5)
        );
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
        pool: &CertificatePool,
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
        // 60% of the validators vote for (1, 10)
        for pubkeys in validator_keypairs.iter().take(6) {
            add_skip_vote_range(&mut pool, 1, 10, pubkeys.vote_keypair.pubkey());
        }
        // 10% vote for (2, 2)
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(2),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            ),
            Ok(Some(NewHighestCertificate::Skip(2)))
        );
        assert_single_certificate_range(&pool, 2, 2);
        // 10% vote for (4, 4)
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(4),
                dummy_transaction(),
                &validator_keypairs[7].vote_keypair.pubkey(),
            ),
            Ok(Some(NewHighestCertificate::Skip(4)))
        );
        assert_single_certificate_range(&pool, 2, 2);
        assert_single_certificate_range(&pool, 4, 4);
        // 10% vote for (3, 3)
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(3),
                dummy_transaction(),
                &validator_keypairs[8].vote_keypair.pubkey(),
            ),
            Ok(None) // Return is None because current highest slot is 4
        );
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
        // 60% voted on (1, 6)
        for pubkeys in validator_keypairs.iter().take(6) {
            add_skip_vote_range(&mut pool, 1, 6, pubkeys.vote_keypair.pubkey());
        }
        // Range expansion on a singleton vote should be ok
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(1),
                dummy_transaction(),
                &validator_keypairs[6].vote_keypair.pubkey()
            ),
            Ok(Some(NewHighestCertificate::Skip(1)))
        );
        add_skip_vote_range(&mut pool, 1, 6, validator_keypairs[6].vote_keypair.pubkey());
        assert_eq!(pool.highest_skip_slot(), 6);
        assert_single_certificate_range(&pool, 1, 6);
    }

    #[test]
    fn test_update_existing_vote() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 60% voted for (10, 25)
        for pubkeys in validator_keypairs.iter().take(6) {
            add_skip_vote_range(&mut pool, 10, 25, pubkeys.vote_keypair.pubkey());
        }
        let pubkey = validator_keypairs[6].vote_keypair.pubkey();

        add_skip_vote_range(&mut pool, 10, 20, pubkey);
        assert_eq!(pool.highest_skip_slot(), 20);
        assert_single_certificate_range(&pool, 10, 20);

        // AlreadyExists, silently fail
        assert_eq!(
            pool.add_vote(&Vote::new_skip_vote(20), dummy_transaction(), &pubkey),
            Ok(None)
        );
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
}
