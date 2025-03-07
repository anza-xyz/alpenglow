use {
    super::{Stake, SUPERMAJORITY},
    solana_pubkey::Pubkey,
    solana_sdk::{
        clock::Slot,
        transaction::{TransactionError, VersionedTransaction},
    },
    std::collections::HashMap,
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    #[error("Vote already exists for this pubkey")]
    DuplicateVote,

    #[error("Transaction failed: {0}")]
    TransactionFailed(#[from] TransactionError),
}

pub struct VoteCertificate {
    // Must be either all notarization or finalization votes.
    // We keep separate certificates for each type
    certificate: HashMap<Pubkey, VersionedTransaction>,
    // Total stake of all the slots in the certificate
    stake: Stake,
    // The slot the votes in the certificate are for
    slot: Slot,
    is_complete: bool,
}

impl VoteCertificate {
    pub fn new(slot: Slot) -> Self {
        Self {
            certificate: HashMap::new(),
            stake: 0,
            slot,
            is_complete: false,
        }
    }

    pub fn add_vote(
        &mut self,
        validator_key: &Pubkey,
        transaction: VersionedTransaction,
        validator_stake: Stake,
        total_stake: Stake,
    ) -> Result<(), AddVoteError> {
        // Caller needs to verify that this is the same type (Notarization, Skip) as all the other votes in the current certificate
        if self.certificate.contains_key(validator_key) {
            return Err(AddVoteError::DuplicateVote);
        }
        // TODO: verification that this vote can land
        self.certificate.insert(*validator_key, transaction);
        self.stake += validator_stake;
        self.is_complete = self.check_complete(total_stake);

        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    pub fn check_complete(&mut self, total_stake: Stake) -> bool {
        (self.stake as f64 / total_stake as f64) > SUPERMAJORITY
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn size(&self) -> usize {
        self.certificate.len()
    }

    pub fn get_certificate(&self) -> Vec<VersionedTransaction> {
        self.certificate.values().cloned().collect()
    }
}
