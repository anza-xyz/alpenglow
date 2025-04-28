use {
    super::{
        bit_vector::BitVector,
        transaction::{AlpenglowVoteTransaction, BlsVoteTransaction},
        Stake,
    },
    solana_bls::{Pubkey as BlsPubkey, PubkeyProjective, Signature, SignatureProjective},
    solana_sdk::transaction::VersionedTransaction,
    std::{collections::HashMap, sync::Arc},
    thiserror::Error,
};

pub trait VoteCertificate: Default {
    type VoteTransaction: AlpenglowVoteTransaction;

    fn new(
        stake: Stake,
        transactions: Vec<Arc<Self::VoteTransaction>>,
        transactions: &HashMap<BlsPubkey, usize>,
    ) -> Self;
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

    fn new(
        stake: Stake,
        transactions: Vec<Arc<VersionedTransaction>>,
        _validator_bls_pubkey_map: &HashMap<BlsPubkey, usize>,
    ) -> Self {
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

    fn new(
        stake: Stake,
        transactions: Vec<Arc<BlsVoteTransaction>>,
        validator_bls_pubkey_map: &HashMap<BlsPubkey, usize>,
    ) -> Self {
        // TODO: unwrapping here for now for simplicity, but we should handle
        // this error properly once error handling is set in place for the
        // alpenglow implementation
        BlsCertificate::new(stake, transactions, validator_bls_pubkey_map).unwrap()
    }

    fn size(&self) -> Option<usize> {
        unimplemented!()
    }

    fn transactions(&self) -> Vec<Arc<BlsVoteTransaction>> {
        unimplemented!()
    }

    fn stake(&self) -> Stake {
        self.stake
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum BlsCertificateError {
    #[error("Index out of bounds")]
    IndexOutOfBound,
    #[error("Invalid pubkey")]
    InvalidPubkey,
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Validator does not exist")]
    ValidatorDoesNotExist,
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct BlsCertificate {
    /// BLS aggregate pubkey
    pub aggregate_pubkey: BlsPubkey,
    /// BLS aggregate signature
    pub aggregate_signature: Signature,
    /// Bit-vector indicating which votes are invluded in the aggregate signature
    pub bit_vector: BitVector,
    /// Total stake in the certificate
    pub stake: Stake,
}

impl BlsCertificate {
    pub fn new(
        stake: Stake,
        transactions: Vec<Arc<BlsVoteTransaction>>,
        validator_bls_pubkey_map: &HashMap<BlsPubkey, usize>,
    ) -> Result<Self, BlsCertificateError> {
        let mut aggregate_pubkey = PubkeyProjective::default();
        let mut aggregate_signature = SignatureProjective::default();
        let mut bit_vector = BitVector::default();

        // TODO: signature aggregation can be done out-of-order;
        // consider aggregating signatures separately in parallel
        for transaction in transactions {
            // aggregate the pubkey
            let bls_pubkey: PubkeyProjective = transaction
                .pubkey
                .try_into()
                .map_err(|_| BlsCertificateError::InvalidPubkey)?;
            aggregate_pubkey.aggregate_with([&bls_pubkey]);

            // aggregate the signature
            let signature: SignatureProjective = transaction
                .signature
                .try_into()
                .map_err(|_| BlsCertificateError::InvalidSignature)?;
            aggregate_signature.aggregate_with([&signature]);

            // set bit-vector for the validator
            let validator_index = validator_bls_pubkey_map
                .get(&transaction.pubkey)
                .ok_or(BlsCertificateError::ValidatorDoesNotExist)?;
            bit_vector
                .set_bit(*validator_index, true)
                .map_err(|_| BlsCertificateError::IndexOutOfBound)?;
        }

        Ok(Self {
            aggregate_pubkey: aggregate_pubkey.into(),
            aggregate_signature: aggregate_signature.into(),
            bit_vector,
            stake,
        })
    }

    pub fn add(
        &mut self,
        stake: Stake,
        validator_pubkey_map: &HashMap<BlsPubkey, usize>,
        transaction: &BlsVoteTransaction,
    ) -> Result<(), BlsCertificateError> {
        let aggregate_pubkey: PubkeyProjective = self
            .aggregate_pubkey
            .try_into()
            .map_err(|_| BlsCertificateError::InvalidPubkey)?;
        let new_pubkey: PubkeyProjective = transaction
            .pubkey
            .try_into()
            .map_err(|_| BlsCertificateError::InvalidPubkey)?;

        let aggregate_signature: SignatureProjective = self
            .aggregate_signature
            .try_into()
            .map_err(|_| BlsCertificateError::InvalidSignature)?;
        let new_signature: SignatureProjective = transaction
            .signature
            .try_into()
            .map_err(|_| BlsCertificateError::InvalidSignature)?;

        // the function aggregate fails only on empty pubkeys or signatures,
        // so it is safe to unwrap here
        // TODO: update this after simplfying aggregation interface in `solana_bls`
        let new_aggregate_pubkey =
            PubkeyProjective::aggregate([&aggregate_pubkey, &new_pubkey]).unwrap();
        self.aggregate_pubkey = new_aggregate_pubkey.into();

        let new_aggregate_signature =
            SignatureProjective::aggregate([&aggregate_signature, &new_signature]).unwrap();
        self.aggregate_signature = new_aggregate_signature.into();

        // set bit-vector for the validator
        let validator_index = validator_pubkey_map
            .get(&transaction.pubkey)
            .ok_or(BlsCertificateError::ValidatorDoesNotExist)?;
        self.bit_vector
            .set_bit(*validator_index, true)
            .map_err(|_| BlsCertificateError::IndexOutOfBound)?;

        self.stake += stake;
        Ok(())
    }
}
