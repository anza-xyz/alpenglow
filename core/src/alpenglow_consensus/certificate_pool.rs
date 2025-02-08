#[cfg(feature = "alpenglow")]
use std::sync::atomic::Ordering;
use {
    super::{
        skip_pool::{self, SkipPool},
        vote_certificate::{self, VoteCertificate},
    },
    crate::banking_stage::consumer::Consumer,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::{Bank, NewBankOptions},
        bank_forks::BankForks,
    },
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk::{
        clock::Slot,
        transaction::{MessageHash, SanitizedTransaction, VersionedTransaction},
    },
    std::{
        collections::BTreeMap,
        ops::RangeInclusive,
        sync::{Arc, RwLock},
        time::Instant,
    },
    thiserror::Error,
};

pub type CertificateId = (Slot, CertificateType);

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    #[error("Vote already exists for this pubkey")]
    AddSkipPoolFailed(#[from] skip_pool::AddVoteError),

    #[error("Add vote to vote certificate failed: {0}")]
    AddVoteCertificateFailed(#[from] vote_certificate::AddVoteError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewHighestCertificate {
    Notarize(Slot),
    Skip(Slot),
    Finalize(Slot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Vote {
    Notarize(Slot),
    Skip(RangeInclusive<Slot>),
    Finalize(Slot),
}

impl Vote {
    fn slot(&self) -> Slot {
        match self {
            Vote::Notarize(slot) => *slot,
            Vote::Skip(skip_range) => *skip_range.end(),
            Vote::Finalize(slot) => *slot,
        }
    }

    fn certificate_type(&self) -> CertificateType {
        match self {
            Vote::Notarize(_slot) => CertificateType::Notarize,
            Vote::Skip(_skip_range) => CertificateType::Skip,
            Vote::Finalize(_slot) => CertificateType::Finalize,
        }
    }

    fn is_notarize(&self) -> bool {
        matches!(self, Vote::Notarize(_slot))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CertificateType {
    Notarize,
    Skip,
    Finalize,
}

pub struct CertificatePool {
    // Notarization and finalization vote certificates
    certificates: BTreeMap<CertificateId, VoteCertificate>,
    // Pool of latest skip votes per validator
    skip_pool: SkipPool,
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
        vote: Vote,
        transaction: VersionedTransaction,
        validator_key: &Pubkey,
        validator_stake: u64,
        total_stake: u64,
    ) -> Result<Option<NewHighestCertificate>, AddVoteError> {
        match vote {
            Vote::Notarize(vote_slot) | Vote::Finalize(vote_slot) => {
                let certificate = self
                    .certificates
                    .entry((vote_slot, vote.certificate_type()))
                    .or_insert_with(|| VoteCertificate::new(vote_slot));

                certificate.add_vote(validator_key, transaction, validator_stake, total_stake)?;

                if certificate.is_complete() {
                    if vote.is_notarize() {
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
            Vote::Skip(skip_range) => {
                let old_highest_skip_certificate_slot =
                    *self.skip_pool.max_skip_certificate_range().end();
                self.skip_pool.add_vote(
                    validator_key,
                    skip_range,
                    transaction,
                    validator_stake,
                    total_stake,
                )?;
                let highest_skip_certificate_slot =
                    *self.skip_pool.max_skip_certificate_range().end();
                if old_highest_skip_certificate_slot != highest_skip_certificate_slot {
                    return Ok(Some(NewHighestCertificate::Skip(
                        highest_skip_certificate_slot,
                    )));
                }
            }
        }
        Ok(None)
    }

    fn get_notarization_certificate(&self, slot: Slot) -> Option<Vec<VersionedTransaction>> {
        self.certificates
            .get(&(slot - 1, CertificateType::Notarize))
            .map(|certificate| certificate.get_certificate())
    }

    fn commit_certificate(
        bank: &Arc<Bank>,
        poh_recorder: &RwLock<PohRecorder>,
        certificate: Vec<VersionedTransaction>,
    ) -> bool {
        let consumer = Consumer::create_consumer(poh_recorder);
        let runtime_transactions: Result<Vec<RuntimeTransaction<SanitizedTransaction>>, _> =
            certificate
                .into_iter()
                .map(|versioned_tx| {
                    // Short circuits on first error
                    RuntimeTransaction::try_create(
                        versioned_tx,
                        MessageHash::Compute,
                        None,
                        &**bank,
                        bank.get_reserved_account_keys(),
                    )
                })
                .collect();

        //TODO: guarantee these transactions don't fail
        if let Err(e) = runtime_transactions {
            error!(
                "Error in bank {} creating runtime transaction in certificate {:?}",
                bank.slot(),
                e
            );
            return false;
        }

        let runtime_transactions = runtime_transactions.unwrap();
        let summary = consumer.process_transactions(bank, &Instant::now(), &runtime_transactions);

        if summary.reached_max_poh_height {
            datapoint_error!(
                "vote_certiificate_commit_failure",
                ("error", "slot took too long to ingest votes", String),
                ("slot", bank.slot(), i64)
            );
            // TODO: check if 2/3 of the stake landed, otherwise return false
            return false;
        }

        if summary.error_counters.total.0 != 0 {
            datapoint_error!(
                "vote_certiificate_commit_failure",
                (
                    "error",
                    format!("{} errors occurred", summary.error_counters.total.0),
                    String
                ),
                ("slot", bank.slot(), i64)
            );
            // TODO: check if 2/3 of the stake landed, otherwise return false
            return false;
        }

        true
    }

    pub fn maybe_start_leader(
        &self,
        my_pubkey: &Pubkey,
        my_leader_slot: Slot,
        bank_forks: &RwLock<BankForks>,
        poh_recorder: &RwLock<PohRecorder>,
        total_stake: u64,
    ) -> Option<Arc<Bank>> {
        assert!(!poh_recorder.read().unwrap().has_bank());
        let (parent_bank, skip_certificate) =
            self.make_start_leader_decision(my_leader_slot, bank_forks, total_stake)?;
        let parent_slot = parent_bank.slot();

        // Create new bank
        let new_bank = Bank::new_from_parent_with_options(
            parent_bank,
            my_pubkey,
            my_leader_slot,
            NewBankOptions::default(),
        );

        let bank_with_scheduler = bank_forks.write().unwrap().insert(new_bank);
        let new_bank = bank_with_scheduler.clone_without_scheduler();

        #[cfg(feature = "alpenglow")]
        let poh_bank_start = poh_recorder
            .write()
            .unwrap()
            .set_bank(bank_with_scheduler, false);
        #[cfg(not(feature = "alpenglow"))]
        poh_recorder
            .write()
            .unwrap()
            .set_bank(bank_with_scheduler, false);

        // Commit the notarization certificate
        if self.highest_notarized_slot > 0 {
            let notarization_certificate = self
                .get_notarization_certificate(parent_slot)
                .expect("highest notarized slot's certificate can't be found");

            if !Self::commit_certificate(&new_bank, poh_recorder, notarization_certificate) {
                error!(
                    "Commit certificate (notarize) for leader slot {} with parent {} failed",
                    new_bank.slot(),
                    parent_slot
                );
                return None;
            }
        }

        // Commit the skip certificate if needed
        if let Some(skip_certificate) = skip_certificate {
            if !Self::commit_certificate(&new_bank, poh_recorder, skip_certificate) {
                error!(
                        "Commit certificate (skip) for leader slot {}, skip range {:?} with parent {} failed",
                        new_bank.slot(),
                        self.skip_pool.max_skip_certificate_range(),
                        parent_slot
                    );
                return None;
            }
        }

        #[cfg(feature = "alpenglow")]
        {
            // Enable transaction processing
            poh_bank_start
                .contains_valid_certificate
                .store(true, Ordering::Relaxed);
        }

        Some(new_bank)
    }

    /// Determines if the leader can start based on notarization and skip certificates.
    fn make_start_leader_decision(
        &self,
        my_leader_slot: Slot,
        bank_forks: &RwLock<BankForks>,
        total_stake: u64,
    ) -> Option<(Arc<Bank>, Option<Vec<VersionedTransaction>>)> {
        let parent_bank = bank_forks
            .read()
            .unwrap()
            .get(self.highest_notarized_slot)?;

        // Ensure the parent bank is frozen before proceeding.
        if !parent_bank.is_frozen() {
            return None;
        }

        let parent_slot = parent_bank.slot();
        let needs_skip_certificate = my_leader_slot != parent_slot + 1;

        let skip_certificate = if needs_skip_certificate {
            let max_skip_range = self.skip_pool.max_skip_certificate_range();
            if max_skip_range.contains(&(parent_slot + 1))
                && max_skip_range.contains(&(my_leader_slot - 1))
            {
                Some(
                    self.skip_pool
                        .get_skip_certificate(total_stake)
                        .expect("valid skip certificate must exist")
                        .1,
                )
            } else {
                return None;
            }
        } else {
            None
        };

        Some((parent_bank, skip_certificate))
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
    use super::*;
    use solana_runtime::{
        bank::{Bank, NewBankOptions},
        bank_forks::BankForks,
        genesis_utils::{create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs},
    };
    use solana_sdk::{
        clock::Slot, pubkey::Pubkey, signer::Signer, transaction::VersionedTransaction,
    };
    use std::sync::{Arc, RwLock};

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
        let my_keypairs = ValidatorVoteKeypairs::new_rand();
        let pool = CertificatePool::new();
        let bank_forks = create_bank_forks(vec![my_keypairs]);
        let total_stake = 100;

        // No notarization set, pool is default
        let decision = pool.make_start_leader_decision(10, &bank_forks, total_stake);
        assert!(
            decision.is_none(),
            "Leader should not be allowed to start without notarization"
        );

        // Even in cases where the defualt highest_notarized_slot = 0 is missing, should not panic
        assert_eq!(pool.highest_notarized_slot, 0);
        bank_forks.write().unwrap().remove(0);
        let decision = pool.make_start_leader_decision(10, &bank_forks, total_stake);
        assert!(
            decision.is_none(),
            "Leader should not be allowed to start without notarization"
        );
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
                Vote::Notarize(5),
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
        let decision = pool.make_start_leader_decision(10, &bank_forks, total_stake);
        assert!(
            decision.is_none(),
            "Leader should not be allowed to start if a skip certificate is missing"
        );
    }

    #[test]
    fn test_make_decision_leader_starts_when_no_skip_required() {
        let my_keypairs = ValidatorVoteKeypairs::new_rand();
        let my_pubkey = my_keypairs.node_keypair.pubkey();
        let mut pool = CertificatePool::new();
        let bank_forks = create_bank_forks(vec![my_keypairs]);
        let my_stake = 67;
        let total_stake = 100;

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        bank.freeze();
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                Vote::Notarize(5),
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
        let decision = pool.make_start_leader_decision(6, &bank_forks, total_stake);
        assert!(
            decision.is_some(),
            "Leader should be allowed to start when no skip certificate is needed"
        );
    }

    #[test]
    fn test_make_decision_leader_starts_if_notarized_and_skips_valid() {
        let my_keypairs = ValidatorVoteKeypairs::new_rand();
        let my_pubkey = my_keypairs.node_keypair.pubkey();
        let my_stake = 67;
        let total_stake = 100;
        let mut pool = CertificatePool::new();
        let bank_forks = create_bank_forks(vec![my_keypairs]);

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        bank.freeze();
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                Vote::Notarize(5),
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
                Vote::Skip(6..=9),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(9)
        );

        let decision = pool.make_start_leader_decision(10, &bank_forks, total_stake);
        assert!(
            decision.is_some(),
            "Leader should be allowed to start when valid skip certificate exists"
        );
    }

    #[test]
    fn test_make_decision_leader_starts_if_skip_range_superset() {
        let my_keypairs = ValidatorVoteKeypairs::new_rand();
        let my_pubkey = my_keypairs.node_keypair.pubkey();
        let my_stake = 67;
        let total_stake = 100;
        let mut pool = CertificatePool::new();
        let bank_forks = create_bank_forks(vec![my_keypairs]);

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        bank.freeze();
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                Vote::Notarize(5),
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
                Vote::Skip(4..=9),
                dummy_transaction(),
                &my_pubkey,
                my_stake,
                total_stake,
            )
            .unwrap()
            .unwrap(),
            NewHighestCertificate::Skip(9)
        );

        let decision = pool.make_start_leader_decision(10, &bank_forks, total_stake);
        assert!(
            decision.is_some(),
            "Leader should be allowed to start when valid skip certificate exists"
        );
    }

    #[test]
    fn test_make_decision_fails_if_parent_not_frozen() {
        let my_keypairs = ValidatorVoteKeypairs::new_rand();
        let my_pubkey = my_keypairs.node_keypair.pubkey();
        let my_stake = 67;
        let mut pool = CertificatePool::new();
        let bank_forks = create_bank_forks(vec![my_keypairs]);
        let total_stake = 100;

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        // Ensure parent bank is *not* frozen
        assert!(
            !bank.is_frozen(),
            "Test setup: Parent bank must not be frozen"
        );
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        assert_eq!(
            pool.add_vote(
                Vote::Notarize(5),
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

        // Attempt to start leader
        let decision = pool.make_start_leader_decision(10, &bank_forks, total_stake);

        // Since the parent is not frozen, the decision should be None
        assert!(
            decision.is_none(),
            "Leader should not start if parent bank is not frozen"
        );
    }

    #[test]
    fn test_add_vote_new_finalize_certificate() {
        let mut pool = CertificatePool::new();
        let pubkey = Pubkey::new_unique();
        assert!(pool
            .add_vote(Vote::Finalize(5), dummy_transaction(), &pubkey, 60, 100)
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(Vote::Finalize(5), dummy_transaction(), &pubkey, 60, 100),
            Err(AddVoteError::AddVoteCertificateFailed(_))
        );
        assert_eq!(
            pool.add_vote(
                Vote::Finalize(5),
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
            .add_vote(Vote::Notarize(5), dummy_transaction(), &pubkey, 60, 100)
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(Vote::Notarize(5), dummy_transaction(), &pubkey, 60, 100),
            Err(AddVoteError::AddVoteCertificateFailed(_))
        );
        assert_eq!(
            pool.add_vote(
                Vote::Notarize(5),
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
            .add_vote(Vote::Skip(0..=5), dummy_transaction(), &pubkey, 60, 100)
            .unwrap()
            .is_none());
        // Same key voting again shouldn't make a certificate
        assert_matches!(
            pool.add_vote(Vote::Skip(0..=5), dummy_transaction(), &pubkey, 60, 100),
            Err(AddVoteError::AddSkipPoolFailed(_))
        );
        assert_eq!(
            pool.add_vote(
                Vote::Skip(0..=5),
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
