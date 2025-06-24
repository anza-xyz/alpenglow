use {
    crate::alpenglow_consensus::CertificateId,
    alpenglow_vote::{
        bls_message::{CertificateMessage, VoteMessage},
        certificate::{Certificate, CertificateType},
    },
    bitvec::prelude::*,
    solana_bls::{BlsError, Pubkey as BlsPubkey, PubkeyProjective, Signature, SignatureProjective},
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    thiserror::Error,
};

/// Maximum number of validators in a certificate
///
/// There are around 1500 validators currently. For a clean power-of-two
/// implementation, we should chosoe either 2048 or 4096. Choose a more
/// conservative number 4096 for now.
/// The number of bytes in a bitmap to represent up to 4096 validators
/// (`MAXIMUM_VALIDATORS` / 8)
const VALIDATOR_BITMAP_U8_SIZE: usize = 512;

#[derive(Debug, Error, PartialEq)]
pub enum CertificateError {
    #[error("BLS error: {0}")]
    BlsError(#[from] BlsError),
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

#[derive(Debug, Clone)]
pub struct VoteCertificate {
    certificate: CertificateMessage,
}

impl VoteCertificate {
    pub fn new(certificate_id: CertificateId) -> Self {
        VoteCertificate {
            certificate: CertificateMessage {
                certificate: certificate_id.into(),
                signature: Signature::default(),
                bitmap: BitVec::<u8, Lsb0>::repeat(false, VALIDATOR_BITMAP_U8_SIZE),
            },
        }
    }

    pub fn vote_count(&self) -> usize {
        self.certificate.bitmap.count_ones()
    }

    pub fn aggregate<'a, 'b, T>(&mut self, messages: T) -> Result<(), CertificateError>
    where
        T: Iterator<Item = &'a VoteMessage>,
        Self: 'b,
        'b: 'a,
    {
        // TODO: signature aggregation can be done out-of-order;
        // consider aggregating signatures separately in parallel
        let mut current_signature_uncompressed =
            if self.certificate.signature == Signature::default() {
                SignatureProjective::default()
            } else {
                SignatureProjective::try_from(self.certificate.signature)
                    .map_err(|_| CertificateError::InvalidSignature)?
            };

        // aggregate the votes
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
                .as_deref()
                == Some(&true)
            {
                panic!("Conflicting vote check should make this unreachable {vote_message:?}");
            }
            self.certificate
                .bitmap
                .set(vote_message.rank as usize, true);
            // aggregate the signature
            // TODO(wen): put this into bls crate
            let uncompressed = SignatureProjective::try_from(vote_message.signature)?;
            current_signature_uncompressed.aggregate_with([&uncompressed]);
        }
        self.certificate.signature = Signature::from(current_signature_uncompressed);
        Ok(())
    }

    pub fn certificate(&self) -> CertificateMessage {
        self.certificate.clone()
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
