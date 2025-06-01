use {
    crate::alpenglow_consensus::CertificateId,
    alpenglow_vote::{
        bls_message::{CertificateMessage, VoteMessage},
        certificate::{Certificate, CertificateType},
    },
    bitvec::prelude::*,
    solana_bls::{Pubkey as BlsPubkey, PubkeyProjective, Signature, SignatureProjective},
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    thiserror::Error,
};

/// The number of bytes in a bitmap to represent up to 4096 validators
/// (`MAXIMUM_VALIDATORS` / 8)
const VALIDATOR_BITMAP_U8_SIZE: usize = 512;

#[derive(Debug, Error, PartialEq)]
pub enum CertificateError {
    #[error("BLS signature error {0}")]
    BlsSignatureError(#[from] solana_bls::error::BlsError),
    #[error("Index out of bounds")]
    IndexOutOfBound,
    #[error("Invalid pubkey")]
    InvalidPubkey,
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Validator does not exist")]
    ValidatorDoesNotExist,
    #[error("Invalid vote type")]
    InvalidVoteType,
}

pub struct VoteCertificate {
    certificate: CertificateMessage,
    signature: SignatureProjective,
}

impl VoteCertificate {
    pub fn new(certificate_id: CertificateId) -> Self {
        Self {
            certificate: CertificateMessage {
                certificate: certificate_id.into(),
                signature: Signature::default(),
                bitmap: BitVec::<u8, Lsb0>::repeat(false, VALIDATOR_BITMAP_U8_SIZE),
            },
            signature: SignatureProjective::default(),
        }
    }

    pub fn aggregate<'a, T>(&mut self, messages: T) -> Result<(), CertificateError>
    where
        T: Iterator<Item = &'a VoteMessage>,
    {
        // TODO: signature aggregation can be done out-of-order;
        // consider aggregating signatures separately in parallel
        for vote_message in messages {
            // set bit-vector for the validator
            //
            // TODO: This only accounts for one type of vote. Update this after
            // we have a base3 encoding implementation.
            if self.certificate.bitmap.len() < vote_message.rank as usize {
                return Err(CertificateError::IndexOutOfBound);
            }
            if self
                .certificate
                .bitmap
                .get(vote_message.rank as usize)
                .is_some()
            {
                return Ok(());
            }
            self.certificate
                .bitmap
                .set(vote_message.rank as usize, true);
            // aggregate the signature
            // TODO(wen): put this into bls crate
            let uncompressed = SignatureProjective::try_from(vote_message.signature)?;
            self.signature.aggregate_with([&uncompressed]);
        }
        self.certificate.signature = Signature::from(self.signature.clone());
        Ok(())
    }

    /// Returns the number of votes in the certificate.
    pub fn vote_count(&self) -> usize {
        self.certificate.bitmap.count_ones()
    }

    /// Copy out the certificate message for transmission.
    pub fn certificate(&self) -> CertificateMessage {
        self.certificate.clone()
    }
}

impl From<CertificateMessage> for VoteCertificate {
    fn from(certificate: CertificateMessage) -> Self {
        let signature = SignatureProjective::try_from(certificate.signature).unwrap();
        Self {
            certificate,
            signature,
        }
    }
}

/// Given a bit vector and a list of validator BLS pubkeys, generate an
/// aggregate BLS pubkey.
pub fn aggregate_pubkey(
    bitmap: &BitVec<u8, Lsb0>,
    bls_pubkey_to_rank_map: &BLSPubkeyToRankMap,
) -> Result<BlsPubkey, CertificateError> {
    let mut aggregate_pubkey = PubkeyProjective::default();
    for (i, included) in bitmap.iter().enumerate() {
        if *included {
            let bls_pubkey: PubkeyProjective = bls_pubkey_to_rank_map
                .get_pubkey(i)
                .ok_or(CertificateError::IndexOutOfBound)?
                .1
                .try_into()
                .map_err(|_| CertificateError::InvalidPubkey)?;

            aggregate_pubkey.aggregate_with([&bls_pubkey]);
        }
    }

    Ok(aggregate_pubkey.into())
}

impl From<CertificateId> for Certificate {
    fn from(certificate_id: CertificateId) -> Certificate {
        match certificate_id {
            CertificateId::Finalize(slot) => Certificate {
                certificate_type: CertificateType::Finalize,
                slot,
                block_id: None,
                replayed_bank_hash: None,
            },
            CertificateId::FinalizeFast(slot, block_id, replayed_bank_hash) => Certificate {
                slot,
                certificate_type: CertificateType::FinalizeFast,
                block_id: Some(block_id),
                replayed_bank_hash: Some(replayed_bank_hash),
            },
            CertificateId::Notarize(slot, block_id, replayed_bank_hash) => Certificate {
                certificate_type: CertificateType::Notarize,
                slot,
                block_id: Some(block_id),
                replayed_bank_hash: Some(replayed_bank_hash),
            },
            CertificateId::NotarizeFallback(slot, block_id, replayed_bank_hash) => Certificate {
                certificate_type: CertificateType::NotarizeFallback,
                slot,
                block_id: Some(block_id),
                replayed_bank_hash: Some(replayed_bank_hash),
            },
            CertificateId::Skip(slot) => Certificate {
                certificate_type: CertificateType::Skip,
                slot,
                block_id: None,
                replayed_bank_hash: None,
            },
        }
    }
}
