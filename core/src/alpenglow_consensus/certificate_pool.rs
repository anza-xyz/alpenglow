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
}

impl CertificatePool {
    pub fn new() -> Self {
        Self {
            certificates: BTreeMap::default(),
            skip_pool: SkipPool::new(),
            highest_notarized_slot: 0,
        }
    }

    pub fn add_vote(
        &mut self,
        vote: Vote,
        transaction: VersionedTransaction,
        validator_key: &Pubkey,
        validator_stake: u64,
        total_stake: u64,
    ) -> Result<(), AddVoteError> {
        match vote {
            Vote::Notarize(vote_slot) | Vote::Finalize(vote_slot) => {
                let certificate = self
                    .certificates
                    .entry((vote_slot, vote.certificate_type()))
                    .or_insert_with(|| VoteCertificate::new(vote_slot));

                certificate.add_vote(validator_key, transaction, validator_stake, total_stake)?;

                if certificate.is_complete() {
                    self.highest_notarized_slot = self.highest_notarized_slot.max(vote_slot);
                }
            }
            Vote::Skip(skip_range) => self.skip_pool.add_vote(
                validator_key,
                skip_range,
                transaction,
                validator_stake,
                total_stake,
            )?,
        }
        Ok(())
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
        let parent_bank = bank_forks.read().unwrap().get(self.highest_notarized_slot);
        if let Some(parent_bank) = &parent_bank {
            // We haven't finished replaying the parent yet
            // TODO: Note it's also possible you may have a skip certificate for this parent_bank,
            // and could immediately start building your slot off an earlier parent.
            if !parent_bank.is_frozen() {
                return None;
            }
        } else {
            // We haven't started replaying the parent yet
            return None;
        }

        let parent_bank = parent_bank.unwrap();
        let parent_slot = parent_bank.slot();
        // Check all slots between `my_leader_slot` and `parent_slot` have a Skip certificate
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
                // Missing skip certificates, can't start leader
                return None;
            }
        } else {
            None
        };

        // Create new bank
        let new_bank = Arc::new(Bank::new_from_parent_with_options(
            parent_bank,
            my_pubkey,
            my_leader_slot,
            NewBankOptions::default(),
        ));

        // Commit the notarization certificate
        if self.highest_notarized_slot > 0 {
            let notarization_certificate = self
                .get_notarization_certificate(parent_slot)
                .expect("highest notarized slot's certificate can't be found");
            // Commit the notarizations
            if !Self::commit_certificate(&new_bank, poh_recorder, notarization_certificate) {
                // commit failed
                error!(
                    "Commit certificate (notarize) my leader slot {} with parent {} failed",
                    new_bank.slot(),
                    parent_slot
                );
                return None;
            }
        }

        // Commit the skip certificate
        if let Some(skip_certificate) = skip_certificate {
            if !Self::commit_certificate(&new_bank, poh_recorder, skip_certificate) {
                error!("Commit Commit certificate (skip) for leader slot {}, skip range {:?} with parent {} failed", new_bank.slot(), self.skip_pool.max_skip_certificate_range(), parent_slot);
                return None;
            }
        }

        // Success!
        Some(new_bank)
    }

    /// Cleanup old finalized slots from the certificate pool
    pub fn purge(&mut self, finalized_slot: Slot) {
        // `certificates`` now only contains entries >= `finalized_slot`
        self.certificates = self
            .certificates
            .split_off(&(finalized_slot, CertificateType::Notarize));
    }
}
