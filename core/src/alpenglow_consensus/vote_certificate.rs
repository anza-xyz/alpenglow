use {
    super::{
        bit_vector::BitVector,
        transaction::{AlpenglowVoteTransaction, BlsVoteTransaction},
        Stake,
    },
    solana_bls::{Signature, SignatureProjective},
    solana_sdk::{pubkey::Pubkey as ValidatorPubkey, transaction::VersionedTransaction},
    std::{collections::HashMap, sync::Arc},
    thiserror::Error,
};

pub trait VoteCertificate: Default {
    type VoteTransaction: AlpenglowVoteTransaction;

    fn new(stake: Stake, transactions: Vec<Arc<Self::VoteTransaction>>) -> Self;
    fn size(&self) -> Option<usize>;
    fn transactions(&self) -> Vec<Arc<Self::VoteTransaction>>;
    fn stake(&self) -> Stake;
}

// NOTE: This will go away after BLS implementation is finished.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LegacyVoteCertificate {
    // We don't need to send the actual vote transactions out for now.
    transactions: Vec<Arc<VersionedTransaction>>,
    // Total stake of all the slots in the certificate
    stake: Stake,
}

impl VoteCertificate for LegacyVoteCertificate {
    type VoteTransaction = VersionedTransaction;

    fn new(stake: Stake, transactions: Vec<Arc<VersionedTransaction>>) -> Self {
        Self {
            stake,
            transactions,
        }
    }

    fn size(&self) -> Option<usize> {
        Some(self.transactions.len())
    }

    fn transactions(&self) -> Vec<Arc<VersionedTransaction>> {
        self.transactions.clone()
    }

    fn stake(&self) -> Stake {
        self.stake
    }
}

impl VoteCertificate for BlsCertificate {
    type VoteTransaction = BlsVoteTransaction;

    fn new(_stake: Stake, _transactions: Vec<Arc<BlsVoteTransaction>>) -> Self {
        unimplemented!()
    }

    fn size(&self) -> Option<usize> {
        unimplemented!()
    }

    fn transactions(&self) -> Vec<Arc<BlsVoteTransaction>> {
        unimplemented!()
    }

    fn stake(&self) -> Stake {
        unimplemented!()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum BlsCertificateError {
    #[error("Index out of bounds")]
    IndexOutOfBound,
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Validator does not exist")]
    ValidatorDoesNotExist,
}

/// Vote data included in a BLS certificate
#[derive(Debug, Default, Eq, Clone, PartialEq)]
pub struct CertificateVoteData {
    // TODO: decide on vote data to be included in cert
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct BlsCertificate {
    /// Vote message
    pub vote_data: CertificateVoteData,
    /// BLS aggregate signature
    pub aggregate_signature: Signature,
    /// Bit-vector indicating which votes are invluded in the aggregate signature
    pub bit_vector: BitVector,
}

impl BlsCertificate {
    pub fn new(
        vote_data: CertificateVoteData,
        validator_pubkey_map: &HashMap<ValidatorPubkey, usize>,
        transactions_map: &HashMap<ValidatorPubkey, BlsVoteTransaction>,
    ) -> Result<Self, BlsCertificateError> {
        let mut aggregate_signature = SignatureProjective::default();
        let mut bit_vector = BitVector::default();
        let pubkey_transactions = transactions_map.iter();

        // TODO: signature aggregation can be done out-of-order;
        // consider aggregating signatures separately in parallel
        for (pubkey, transaction) in pubkey_transactions {
            // aggregate the signature
            let signature: SignatureProjective = transaction
                .signature
                .try_into()
                .map_err(|_| BlsCertificateError::InvalidSignature)?;
            aggregate_signature.aggregate_with([&signature]);

            // set bit-vector for the validator
            let validator_index = validator_pubkey_map
                .get(pubkey)
                .ok_or(BlsCertificateError::ValidatorDoesNotExist)?;
            bit_vector
                .set_bit(*validator_index, true)
                .map_err(|_| BlsCertificateError::IndexOutOfBound)?;
        }

        Ok(Self {
            vote_data,
            aggregate_signature: aggregate_signature.into(),
            bit_vector,
        })
    }

    pub fn add(
        &mut self,
        validator_pubkey_map: &HashMap<ValidatorPubkey, usize>,
        validator_pubkey: &ValidatorPubkey,
        transaction: &BlsVoteTransaction,
    ) -> Result<(), BlsCertificateError> {
        let aggregate_signature: SignatureProjective = self
            .aggregate_signature
            .try_into()
            .map_err(|_| BlsCertificateError::InvalidSignature)?;
        let new_signature: SignatureProjective = transaction
            .signature
            .try_into()
            .map_err(|_| BlsCertificateError::InvalidSignature)?;

        // the function aggregate fails only on empty signatures, so it is safe to unwrap here
        // TODO: update this after simplfying signature aggregation interface in `solana_bls`
        let new_aggregate =
            SignatureProjective::aggregate([&aggregate_signature, &new_signature]).unwrap();
        self.aggregate_signature = new_aggregate.into();

        // set bit-vector for the validator
        let validator_index = validator_pubkey_map
            .get(validator_pubkey)
            .ok_or(BlsCertificateError::ValidatorDoesNotExist)?;
        self.bit_vector
            .set_bit(*validator_index, true)
            .map_err(|_| BlsCertificateError::IndexOutOfBound)?;
        Ok(())
    }
}
