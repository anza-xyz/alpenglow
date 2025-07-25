use {
    crate::CertificateId,
    bitvec::prelude::*,
    solana_bls_signatures::{
        BlsError, Pubkey as BlsPubkey, PubkeyProjective, Signature, SignatureProjective,
    },
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    solana_vote::alpenglow::{
        bls_message::{CertificateMessage, VoteMessage},
        certificate::{Certificate, CertificateType},
    },
    thiserror::Error,
};

/// Maximum number of validators in a certificate
///
/// There are around 1500 validators currently. For a clean power-of-two
/// implementation, we should chosoe either 2048 or 4096. Choose a more
/// conservative number 4096 for now.
const MAXIMUM_VALIDATORS: usize = 4096;

#[derive(Debug, Error, PartialEq)]
pub enum CertificateError {
    #[error("BLS error: {0}")]
    BlsError(#[from] BlsError),
    #[error("Invalid pubkey")]
    InvalidPubkey,
    #[error("Validator does not exist for given rank: {0}")]
    ValidatorDoesNotExist(usize),
}

//TODO(wen): Maybe we can merge all the below functions into CertificateMessage.
#[derive(Clone)]
pub struct VoteCertificate(CertificateMessage);

impl From<CertificateMessage> for VoteCertificate {
    fn from(certificate_message: CertificateMessage) -> Self {
        Self(certificate_message)
    }
}

impl VoteCertificate {
    pub fn new(certificate_id: CertificateId) -> Self {
        VoteCertificate(CertificateMessage {
            certificate: certificate_id.into(),
            signature: Signature::default(),
            bitmap: BitVec::<u8, Lsb0>::repeat(false, MAXIMUM_VALIDATORS),
        })
    }

    pub fn aggregate<'a, 'b, T>(&mut self, messages: T)
    where
        T: Iterator<Item = &'a VoteMessage>,
        Self: 'b,
        'b: 'a,
    {
        let signature = &mut self.0.signature;
        // TODO: signature aggregation can be done out-of-order;
        // consider aggregating signatures separately in parallel
        let mut current_signature = if signature == &Signature::default() {
            SignatureProjective::identity()
        } else {
            SignatureProjective::try_from(*signature).expect("Invalid signature")
        };

        // aggregate the votes
        let bitmap = &mut self.0.bitmap;
        for vote_message in messages {
            // set bit-vector for the validator
            //
            // TODO: This only accounts for one type of vote. Update this after
            // we have a base3 encoding implementation.
            assert!(
                bitmap.len() > vote_message.rank as usize,
                "Vote rank {} exceeds bitmap length {}",
                vote_message.rank,
                bitmap.len()
            );
            assert!(
                bitmap.get(vote_message.rank as usize).as_deref() != Some(&true),
                "Conflicting vote check should make this unreachable {vote_message:?}"
            );
            bitmap.set(vote_message.rank as usize, true);
            // aggregate the signature
            current_signature
                .aggregate_with([&vote_message.signature])
                .expect(
                    "Failed to aggregate signature: {vote_message.signature:?} into {current_signature:?}"
                );
        }
        *signature = Signature::from(current_signature);
    }

    pub fn certificate(self) -> CertificateMessage {
        self.0
    }
}

/// Given a bit vector and a list of validator BLS pubkeys, generate an
/// aggregate BLS pubkey.
#[allow(dead_code)]
pub fn aggregate_pubkey(
    bitmap: &BitVec<u8, Lsb0>,
    bls_pubkey_to_rank_map: &BLSPubkeyToRankMap,
) -> Result<BlsPubkey, CertificateError> {
    let mut aggregate_pubkey = PubkeyProjective::identity();
    for (i, included) in bitmap.iter().enumerate() {
        if *included {
            let bls_pubkey: PubkeyProjective = bls_pubkey_to_rank_map
                .get_pubkey(i)
                .ok_or(CertificateError::ValidatorDoesNotExist(i))?
                .1
                .try_into()
                .map_err(|_| CertificateError::InvalidPubkey)?;

            aggregate_pubkey.aggregate_with([&bls_pubkey])?;
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
