use {
    super::{
        skip_pool::{self, SkipPool},
        vote_certificate::{self, VoteCertificate},
        Stake,
    },
    alpenglow_vote::vote::Vote,
    itertools::Either,
    solana_pubkey::Pubkey,
    solana_sdk::{clock::Slot, transaction::VersionedTransaction},
    std::collections::BTreeMap,
    thiserror::Error,
};

pub type CertificateId = (Slot, CertificateType);

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    #[error("Add vote to skip pool failed: {0}")]
    AddToSkipPoolFailed(#[from] skip_pool::AddVoteError),

    #[error("Add vote to vote certificate failed: {0}")]
    AddToCertificatePool(#[from] vote_certificate::AddVoteError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewHighestCertificate {
    Notarize(Slot),
    Skip(Slot),
    Finalize(Slot),
}

impl NewHighestCertificate {
    pub fn is_finalize(&self) -> bool {
        matches!(self, NewHighestCertificate::Finalize(_slot))
    }

    pub fn slot(&self) -> Slot {
        match self {
            NewHighestCertificate::Notarize(slot) => *slot,
            NewHighestCertificate::Skip(slot) => *slot,
            NewHighestCertificate::Finalize(slot) => *slot,
        }
    }
}

pub struct StartLeaderCertificates {
    pub notarization_certificate: Vec<VersionedTransaction>,
    pub skip_certificate: Vec<VersionedTransaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
        }
    }
}

pub struct CertificatePool {
    // Notarization and finalization vote certificates
    certificates: BTreeMap<CertificateId, VoteCertificate>,
    // Pool of latest skip votes per validator
    skip_pool: SkipPool<VersionedTransaction>,
    // Highest slot with a notarized certificate
    highest_notarized_slot: Slot,
    // Highest slot with a finalized certificate
    highest_finalized_slot: Slot,
}

impl Default for CertificatePool {
    fn default() -> Self {
        Self::new()
    }
}

impl CertificatePool {
    pub fn new() -> Self {
        Self {
            certificates: BTreeMap::default(),
            skip_pool: SkipPool::new(),
            highest_notarized_slot: 0,
            highest_finalized_slot: 0,
        }
    }

    pub fn add_vote(
        &mut self,
        vote: &Vote,
        transaction: VersionedTransaction,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
        total_stake: Stake,
    ) -> Result<Option<NewHighestCertificate>, AddVoteError> {
        match vote.voted_slots() {
            Either::Left(vote_slot) => {
                let certificate = self
                    .certificates
                    .entry((vote_slot, CertificateType::get_type(vote)))
                    .or_insert_with(|| VoteCertificate::new(vote_slot));

                certificate.add_vote(
                    validator_vote_key,
                    transaction,
                    validator_stake,
                    total_stake,
                )?;

                if certificate.is_complete() {
                    if vote.is_notarization() {
                        let old_highest_notarized_slot = self.highest_notarized_slot;
                        self.highest_notarized_slot = self.highest_notarized_slot.max(vote_slot);
                        if old_highest_notarized_slot != self.highest_notarized_slot {
                            return Ok(Some(NewHighestCertificate::Notarize(
                                self.highest_notarized_slot,
                            )));
                        }
                    } else {
                        let old_highest_finalized_slot = self.highest_finalized_slot;
                        self.highest_finalized_slot = self.highest_finalized_slot.max(vote_slot);
                        if old_highest_finalized_slot != self.highest_finalized_slot {
                            return Ok(Some(NewHighestCertificate::Finalize(
                                self.highest_finalized_slot,
                            )));
                        }
                    }
                }
            }
            Either::Right(skip_range) => {
                let old_highest_skip_certificate_slot = self.highest_skip_slot();
                self.skip_pool.add_vote(
                    validator_vote_key,
                    skip_range,
                    transaction,
                    validator_stake,
                )?;
                // TODO(ashwin): figure out batching, this is expensive to call everytime
                self.skip_pool.update(total_stake);
                let highest_skip_certificate_slot = self.highest_skip_slot();
                if old_highest_skip_certificate_slot != highest_skip_certificate_slot {
                    return Ok(Some(NewHighestCertificate::Skip(
                        highest_skip_certificate_slot,
                    )));
                }
            }
        }
        Ok(None)
    }

    pub fn update(&mut self, total_stake: Stake) {
        self.skip_pool.update(total_stake);
    }

    pub fn is_notarization_certificate_complete(&self, slot: Slot) -> bool {
        self.certificates
            .get(&(slot, CertificateType::Notarize))
            .map(|certificate| certificate.is_complete())
            .unwrap_or(false)
    }

    pub fn get_notarization_certificate(&self, slot: Slot) -> Option<Vec<VersionedTransaction>> {
        self.certificates
            .get(&(slot, CertificateType::Notarize))
            .and_then(|certificate| {
                if certificate.is_complete() {
                    Some(certificate.get_certificate())
                } else {
                    None
                }
            })
    }

    pub fn get_finalization_certificate(&self, slot: Slot) -> Option<Vec<VersionedTransaction>> {
        self.certificates
            .get(&(slot, CertificateType::Finalize))
            .and_then(|certificate| {
                if certificate.is_complete() {
                    Some(certificate.get_certificate())
                } else {
                    None
                }
            })
    }

    pub fn highest_certificate_slot(&self) -> Slot {
        self.highest_finalized_slot.max(
            self.highest_notarized_slot
                .max(*self.skip_pool.max_skip_certificate_range().end()),
        )
    }

    pub fn highest_not_skip_certificate_slot(&self) -> Slot {
        self.highest_finalized_slot.max(self.highest_notarized_slot)
    }

    pub fn highest_notarized_slot(&self) -> Slot {
        self.highest_notarized_slot
    }

    pub fn highest_skip_slot(&self) -> Slot {
        *self.skip_pool.max_skip_certificate_range().end()
    }

    pub fn highest_finalized_slot(&self) -> Slot {
        self.highest_finalized_slot
    }

    pub fn is_finalized_slot(&self, slot: Slot) -> bool {
        self.certificates
            .get(&(slot, CertificateType::Finalize))
            .map(|certificate| certificate.is_complete())
            .unwrap_or(false)
    }

    #[allow(dead_code)]
    pub fn skip_certified(&mut self, slot: Slot, total_stake: Stake) -> bool {
        self.skip_pool.skip_certified(slot, total_stake)
    }

    /// Determines if the leader can start based on notarization and skip certificates.
    pub fn make_start_leader_decision(
        &self,
        my_leader_slot: Slot,
        parent_slot: Slot,
        first_alpenglow_slot: Slot,
        total_stake: Stake,
    ) -> Option<StartLeaderCertificates> {
        let needs_notarization_certificate =
            parent_slot >= first_alpenglow_slot && parent_slot != 0;

        let notarization_certificate = {
            if needs_notarization_certificate {
                if let Some(notarization_certificate) =
                    self.get_notarization_certificate(parent_slot)
                {
                    notarization_certificate
                } else if let Some(finalization_certificate) =
                    self.get_finalization_certificate(parent_slot)
                {
                    finalization_certificate
                } else {
                    return None;
                }
            } else {
                vec![]
            }
        };

        let needs_skip_certificate =
            // handles cases where we are entering the alpenglow epoch, where the first
            // slot in the epoch will pass my_leader_slot == parent_slot
            my_leader_slot != first_alpenglow_slot &&
            my_leader_slot != parent_slot + 1;

        let skip_certificate = {
            if needs_skip_certificate {
                let begin_skip_slot = first_alpenglow_slot.max(parent_slot + 1);
                let end_skip_slot = my_leader_slot - 1;
                let max_skip_range = self.skip_pool.max_skip_certificate_range();
                if max_skip_range.contains(&begin_skip_slot)
                    && max_skip_range.contains(&end_skip_slot)
                {
                    self.skip_pool
                        .get_skip_certificates(total_stake)
                        .last()
                        .expect("valid skip certificate must exist")
                        .1
                        .clone()
                } else {
                    return None;
                }
            } else {
                vec![]
            }
        };

        Some(StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        })
    }

    /// Cleanup old finalized slots from the certificate pool
    pub fn purge(&mut self, finalized_slot: Slot) {
        // `certificates`` now only contains entries >= `finalized_slot`
        self.certificates = self
            .certificates
            .split_off(&(finalized_slot, CertificateType::Notarize));
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

    fn create_bank_forks(validator_keypairs: Vec<ValidatorVoteKeypairs>) -> Arc<RwLock<BankForks>> {
        let genesis = create_genesis_config_with_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![100; validator_keypairs.len()],
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        BankForks::new_rw_arc(bank0)
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_notarization_missing() {
        let pool = CertificatePool::new();
        let total_stake = 100;

        // No notarization set, pool is default
        let parent_slot = 1;
        let my_leader_slot = 2;
        let first_alpenglow_slot = 0;
        let decision = pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
            total_stake,
        );
        assert!(
            decision.is_none(),
            "Leader should not be allowed to start without notarization"
        );
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_1() {
        let pool = CertificatePool::new();
        let total_stake = 100;

        // If parent_slot == 0, you don't need a notarization certificate
        // Because leader_slot == parent_slot + 1, you don't need a skip certificate
        let parent_slot = 0;
        let my_leader_slot = 1;
        let first_alpenglow_slot = 0;
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();
        assert!(notarization_certificate.is_empty());
        assert!(skip_certificate.is_empty());
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_2() {
        let mut pool = CertificatePool::new();
        let my_pubkey = Pubkey::new_unique();
        let my_stake = 67;
        let total_stake = 100;

        // If parent_slot < first_alpenglow_slot, and parent_slot > 0
        // no notarization certificate is required, but a skip
        // certificate will be
        let parent_slot = 1;
        let my_leader_slot = 3;
        let first_alpenglow_slot = 2;
        assert!(pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .is_none());

        // Add skip certifcate
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(first_alpenglow_slot, first_alpenglow_slot),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(first_alpenglow_slot)
        );
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();
        assert!(notarization_certificate.is_empty());
        assert!(!skip_certificate.is_empty());
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_3() {
        let pool = CertificatePool::new();
        let total_stake = 100;
        // If parent_slot == first_alpenglow_slot, and
        // first_alpenglow_slot > 0, you need a notarization certificate
        let parent_slot = 1;
        let my_leader_slot = 2;
        let first_alpenglow_slot = 1;
        assert!(pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .is_none());
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_4() {
        let mut pool = CertificatePool::new();
        let my_pubkey = Pubkey::new_unique();
        let my_stake = 67;
        let total_stake = 100;

        // If parent_slot < first_alpenglow_slot, and parent_slot == 0,
        // no notarization certificate is required, but a skip certificate will
        // be
        let parent_slot = 0;
        let my_leader_slot = 2;
        let first_alpenglow_slot = 1;
        assert!(pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .is_none());

        // Add skip certifcate
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(first_alpenglow_slot, first_alpenglow_slot),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(first_alpenglow_slot)
        );
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();
        assert!(notarization_certificate.is_empty());
        assert!(!skip_certificate.is_empty());
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_5() {
        let my_pubkey = Pubkey::new_unique();
        let mut pool = CertificatePool::new();
        let my_stake = 67;
        let total_stake = 100;

        // Valid skip certificate for 1-9 exists
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(1, 9),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(9)
        );

        // Parent slot is equal to 0, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 0;
        let first_alpenglow_slot = 0;
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();

        assert!(notarization_certificate.is_empty());
        assert!(!skip_certificate.is_empty());
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_6() {
        let my_pubkey = Pubkey::new_unique();
        let mut pool = CertificatePool::new();
        let my_stake = 67;
        let total_stake = 100;

        // Valid skip certificate for 1-9 exists
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(1, 9),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(9)
        );
        // Parent slot is less than first_alpenglow_slot, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 4;
        let first_alpenglow_slot = 5;
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();

        assert!(notarization_certificate.is_empty());
        assert!(!skip_certificate.is_empty());
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_skip_certificate_missing() {
        let my_keypairs = ValidatorVoteKeypairs::new_rand();
        let my_pubkey = my_keypairs.node_keypair.pubkey();
        let mut pool = CertificatePool::new();
        let bank_forks = create_bank_forks(vec![my_keypairs]);
        let total_stake = 100;
        let my_stake = 67;

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        bank.freeze();
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(), None),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Notarize(5)
        );
        assert_eq!(pool.highest_notarized_slot, 5);

        // No skip certificate for 6-10
        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        let decision = pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
            total_stake,
        );
        assert!(
            decision.is_none(),
            "Leader should not be allowed to start if a skip certificate is missing"
        );
    }

    #[test]
    fn test_make_decision_leader_starts_when_no_skip_required() {
        let my_pubkey = Pubkey::new_unique();
        let mut pool = CertificatePool::new();
        let my_stake = 67;
        let total_stake = 100;

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(), None),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Notarize(5)
        );
        assert_eq!(pool.highest_notarized_slot, 5);

        // Leader slot is just +1 from notarized slot (no skip needed)
        let my_leader_slot = 6;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();

        assert!(!notarization_certificate.is_empty());
        assert!(skip_certificate.is_empty());
    }

    #[test]
    fn test_make_decision_leader_starts_if_notarized_and_skips_valid() {
        let my_pubkey = Pubkey::new_unique();
        let my_stake = 67;
        let total_stake = 100;
        let mut pool = CertificatePool::new();

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(), None),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Notarize(5)
        );
        assert_eq!(pool.highest_notarized_slot, 5);

        // Valid skip certificate for 6-9 exists
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(6, 9),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(9)
        );

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();

        assert!(!notarization_certificate.is_empty());
        assert!(!skip_certificate.is_empty());
    }

    #[test]
    fn test_make_decision_leader_starts_if_skip_range_superset() {
        let my_pubkey = Pubkey::new_unique();
        let my_stake = 67;
        let total_stake = 100;
        let mut pool = CertificatePool::new();

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(), None),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Notarize(5)
        );
        assert_eq!(pool.highest_notarized_slot, 5);

        // Valid skip certificate for 4-9 exists
        // Should start leader block even if the beginning of the range is from
        // before your last notarized slot
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(4, 9),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(9)
        );

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        let StartLeaderCertificates {
            notarization_certificate,
            skip_certificate,
        } = pool
            .make_start_leader_decision(
                my_leader_slot,
                parent_slot,
                first_alpenglow_slot,
                total_stake,
            )
            .unwrap();

        assert!(
            !notarization_certificate.is_empty(),
            "Leader should be allowed to start when no skip certificate is needed"
        );
        assert!(
            !skip_certificate.is_empty(),
            "Leader should be allowed to start when no skip certificate is needed"
        );
    }

    #[test]
    fn test_add_vote_new_finalize_certificate() {
        let mut pool = CertificatePool::new();
        let pubkey = Pubkey::new_unique();
        assert!(pool
            .add_vote(
                &Vote::new_finalization_vote(5, Hash::default(), Hash::default()),
                dummy_transaction(),
                &pubkey,
                60,
                100
            )
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(
                &Vote::new_finalization_vote(5, Hash::default(), Hash::default()),
                dummy_transaction(),
                &pubkey,
                60,
                100
            ),
            Err(AddVoteError::AddToCertificatePool(_))
        );
        assert_eq!(
            pool.add_vote(
                &Vote::new_finalization_vote(5, Hash::default(), Hash::default()),
                dummy_transaction(),
                &Pubkey::new_unique(),
                10,
                100
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Finalize(5)
        );
    }

    #[test]
    fn test_add_vote_new_notarize_certificate() {
        let mut pool = CertificatePool::new();
        let pubkey = Pubkey::new_unique();
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(), None),
                dummy_transaction(),
                &pubkey,
                60,
                100
            )
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(), None),
                dummy_transaction(),
                &pubkey,
                10,
                100
            ),
            Err(AddVoteError::AddToCertificatePool(_))
        );
        assert_eq!(
            pool.add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(), None),
                dummy_transaction(),
                &Pubkey::new_unique(),
                10,
                100
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Notarize(5)
        );
    }

    #[test]
    fn test_add_vote_new_skip_certificate() {
        let mut pool = CertificatePool::new();
        let pubkey = Pubkey::new_unique();
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(0, 5),
                dummy_transaction(),
                &pubkey,
                60,
                100
            )
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(
                &Vote::new_skip_vote(0, 5),
                dummy_transaction(),
                &pubkey,
                60,
                100
            ),
            Err(AddVoteError::AddToSkipPoolFailed(_))
        );
        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(0, 5),
                dummy_transaction(),
                &Pubkey::new_unique(),
                10,
                100
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(5)
        );
    }
}
