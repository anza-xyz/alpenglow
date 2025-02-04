use {
    crate::banking_stage::consumer::Consumer,
    itertools::Itertools,
    solana_metrics::datapoint_error,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk::{
        clock::Slot,
        transaction::{MessageHash, SanitizedTransaction, TransactionError, VersionedTransaction},
    },
    std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        time::Instant,
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum AddVoteError {
    #[error("Vote already exists for this pubkey")]
    DuplicateVote,

    #[error("Transaction failed: {0}")]
    TransactionFailed(#[from] TransactionError),
}

pub const SUPERMAJORITY: f64 = 2f64 / 3f64;

/// Holding ground for votes for `slot` before all the parents are notarized, i.e.
/// there exists some parent bank with an incomplete `VoteCertificate` (could be skipped
/// or notarzied but we don't know)
pub struct OrphanVotes {
    // Must be either all skip votes, or all notarization votes.
    // We keep separate certificates for each type
    certificate: HashMap<Pubkey, VersionedTransaction>,
    // The slot the votes in the certificate are for. Must be all notarizations
    // or all skips
    slot: Slot,
}

impl OrphanVotes {
    pub fn new(slot: Slot) -> Self {
        Self {
            certificate: HashMap::new(),
            slot,
        }
    }

    pub fn promote(
        self,
        poh_recorder: &RwLock<PohRecorder>,
        parent_bank: Arc<Bank>,
        get_stake: impl Fn(&Pubkey) -> u64,
    ) -> VoteCertificate {
        let mut vote_certificate = VoteCertificate::new(poh_recorder, parent_bank, self.slot);
        for (pubkey, vote) in self.certificate.into_iter() {
            let _res = vote_certificate.add_vote(&pubkey, vote, get_stake(&pubkey));
        }
        vote_certificate
    }
}

pub struct VoteCertificate {
    // Either:
    // 1. The frozen bank for `slot` if the certificate is
    // notarization votes
    // 2. The last notarized bank < slot if the certificate is skip votes
    parent_bank: Arc<Bank>,
    // Must be either all skip votes, or all notarization votes.
    // We keep separate certificates for each type
    certificate: HashMap<Pubkey, RuntimeTransaction<SanitizedTransaction>>,
    // Total stake of all the slots in the certificate
    stake: u64,
    // The slot the votes in the certificate are for
    slot: Slot,
    consumer: Consumer,
}

impl VoteCertificate {
    pub fn new(poh_recorder: &RwLock<PohRecorder>, parent_bank: Arc<Bank>, slot: Slot) -> Self {
        let consumer = Consumer::create_consumer(poh_recorder);
        Self {
            parent_bank,
            certificate: HashMap::new(),
            stake: 0,
            slot,
            consumer,
        }
    }

    pub fn add_vote(
        &mut self,
        pubkey: &Pubkey,
        transaction: VersionedTransaction,
        stake: u64,
    ) -> Result<(), AddVoteError> {
        // Caller needs to verify that this is the same type (Notarization, Skip) as all the other votes in the current certificate
        if self.certificate.contains_key(pubkey) {
            return Err(AddVoteError::DuplicateVote);
        }
        // Simulate the vote on the parent bank
        let transaction = RuntimeTransaction::try_create(
            transaction,
            MessageHash::Compute,
            None,
            &*self.parent_bank,
            self.parent_bank.get_reserved_account_keys(),
        )?;
        let res = self
            .parent_bank
            .simulate_transaction(&transaction, false)
            .result;
        if res.is_ok() {
            self.certificate.insert(*pubkey, transaction);
            self.stake += stake
        }
        Ok(res?)
    }

    pub fn is_valid(&mut self, total_stake: u64) -> bool {
        (self.stake as f64 / total_stake as f64) > SUPERMAJORITY
    }

    pub fn commit(&self, new_bank: &Arc<Bank>) -> bool {
        assert_eq!(self.parent_bank.slot(), new_bank.parent_slot());
        assert_eq!(self.parent_bank.hash(), new_bank.parent_hash());
        let summary = self.consumer.process_transactions(
            new_bank,
            &Instant::now(),
            &self.certificate.values().cloned().collect_vec(),
        );
        if summary.reached_max_poh_height {
            datapoint_error!(
                "vote_certiificate_commit_failure",
                ("error", "slot took too long to ingest votes", String),
                ("slot", new_bank.slot(), i64)
            );
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
                ("slot", new_bank.slot(), i64)
            );
            return false;
        }

        true
    }
}
