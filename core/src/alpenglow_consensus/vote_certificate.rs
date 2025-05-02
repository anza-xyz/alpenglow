use {
    super::{
        bit_vector::BitVector, bls_vote_transaction::BlsVoteTransaction,
        transaction::AlpenglowVoteTransaction, Stake,
    },
    solana_bls::{Pubkey as BlsPubkey, PubkeyProjective, Signature, SignatureProjective},
    solana_sdk::transaction::VersionedTransaction,
    std::{collections::HashMap, sync::Arc},
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq)]
pub enum CertificateError {
    #[error("Index out of bounds")]
    IndexOutOfBound,
    #[error("Invalid pubkey")]
    InvalidPubkey,
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Validator does not exist")]
    ValidatorDoesNotExist,
}

pub trait VoteCertificate: Default {
    type VoteTransaction: AlpenglowVoteTransaction;

    fn new(
        stake: Stake,
        transactions: Vec<Arc<Self::VoteTransaction>>,
        // TODO: make this variable non-option after we have the sorted list of pubkeys
        validator_bls_pubkey_map: Option<&HashMap<BlsPubkey, usize>>,
    ) -> Result<Self, CertificateError>;
    fn vote_count(&self) -> usize;
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
        _validator_bls_pubkey_map: Option<&HashMap<BlsPubkey, usize>>,
    ) -> Result<Self, CertificateError> {
        Ok(Self {
            stake,
            transactions,
        })
    }

    fn vote_count(&self) -> usize {
        self.transactions.len()
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
        validator_bls_pubkey_map: Option<&HashMap<BlsPubkey, usize>>,
    ) -> Result<Self, CertificateError> {
        BlsCertificate::new(stake, transactions, validator_bls_pubkey_map)
    }

    fn vote_count(&self) -> usize {
        self.vote_count.into()
    }

    fn stake(&self) -> Stake {
        self.stake
    }
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
    /// Number of votes accumulated
    pub vote_count: u16, // u16 covers up to 65k votes
}

impl BlsCertificate {
    pub fn new(
        stake: Stake,
        transactions: Vec<Arc<BlsVoteTransaction>>,
        validator_bls_pubkey_map: Option<&HashMap<BlsPubkey, usize>>,
    ) -> Result<Self, CertificateError> {
        let mut aggregate_pubkey = PubkeyProjective::default();
        let mut aggregate_signature = SignatureProjective::default();
        let mut bit_vector = BitVector::default();
        let vote_count = transactions.len() as u16;

        // TODO: signature aggregation can be done out-of-order;
        // consider aggregating signatures separately in parallel
        for transaction in transactions {
            // aggregate the pubkey
            let bls_pubkey: PubkeyProjective = transaction
                .pubkey
                .try_into()
                .map_err(|_| CertificateError::InvalidPubkey)?;
            aggregate_pubkey.aggregate_with([&bls_pubkey]);

            // aggregate the signature
            let signature: SignatureProjective = transaction
                .signature
                .try_into()
                .map_err(|_| CertificateError::InvalidSignature)?;
            aggregate_signature.aggregate_with([&signature]);

            // TODO: remove this if condition once validator hashmap becomes non-option
            if let Some(validator_bls_pubkey_map) = validator_bls_pubkey_map {
                // set bit-vector for the validator
                let validator_index = validator_bls_pubkey_map
                    .get(&transaction.pubkey)
                    .ok_or(CertificateError::ValidatorDoesNotExist)?;
                bit_vector
                    .set_bit(*validator_index, true)
                    .map_err(|_| CertificateError::IndexOutOfBound)?;
            }
        }

        Ok(Self {
            aggregate_pubkey: aggregate_pubkey.into(),
            aggregate_signature: aggregate_signature.into(),
            bit_vector,
            stake,
            vote_count,
        })
    }

    pub fn add(
        &mut self,
        stake: Stake,
        validator_pubkey_map: &HashMap<BlsPubkey, usize>,
        transaction: &BlsVoteTransaction,
    ) -> Result<(), CertificateError> {
        let aggregate_pubkey: PubkeyProjective = self
            .aggregate_pubkey
            .try_into()
            .map_err(|_| CertificateError::InvalidPubkey)?;
        let new_pubkey: PubkeyProjective = transaction
            .pubkey
            .try_into()
            .map_err(|_| CertificateError::InvalidPubkey)?;

        let aggregate_signature: SignatureProjective = self
            .aggregate_signature
            .try_into()
            .map_err(|_| CertificateError::InvalidSignature)?;
        let new_signature: SignatureProjective = transaction
            .signature
            .try_into()
            .map_err(|_| CertificateError::InvalidSignature)?;

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
            .ok_or(CertificateError::ValidatorDoesNotExist)?;
        self.bit_vector
            .set_bit(*validator_index, true)
            .map_err(|_| CertificateError::IndexOutOfBound)?;

        self.stake += stake;
        self.vote_count += 1;
        Ok(())
    }

    /// Given a bit vector and a list of validator BLS pubkeys, generate an
    /// aggregate BLS pubkey.
    ///
    /// TODO: Defining this to be a static function for now, but it more
    /// naturally belongs to where we keep the sorted list of BLS pubkeys
    pub fn aggregate_pubkey(
        bit_vector: &BitVector,
        validator_bls_pubkeys: &[&BlsPubkey],
    ) -> Result<BlsPubkey, CertificateError> {
        let mut aggregate_pubkey = PubkeyProjective::default();
        for (i, pubkey) in validator_bls_pubkeys.iter().enumerate() {
            if bit_vector
                .get_bit(i)
                .map_err(|_| CertificateError::IndexOutOfBound)?
            {
                let pubkey_projective: PubkeyProjective = (*pubkey)
                    .try_into()
                    .map_err(|_| CertificateError::InvalidPubkey)?;
                aggregate_pubkey.aggregate_with([&pubkey_projective]);
            }
        }
        Ok(aggregate_pubkey.into())
    }
}
