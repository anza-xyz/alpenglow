//! The BLS Cert Verify logic.

use {
    bitvec::prelude::{BitVec, Lsb0},
    rayon::prelude::*,
    solana_bls_signatures::{
        pubkey::{Pubkey as BlsPubkey, PubkeyProjective, VerifiablePubkey},
        signature::SignatureProjective,
    },
    solana_signer_store::{decode, DecodeError},
    solana_votor_messages::consensus_message::{Certificate, CertificateType},
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq)]
pub enum CertVerifyError {
    #[error("Failed to decode bitmap {0:?}")]
    BitmapDecodingFailed(DecodeError),

    #[error("Failed to aggregate public keys")]
    KeyAggregationFailed,

    #[error("Failed to serialize original vote")]
    SerializationFailed,

    #[error("The signature doesn't match")]
    SignatureVerificationFailed,

    #[error("Base 3 encoding on unexpected cert {0:?}")]
    Base3EncodingOnUnexpectedCert(CertificateType),
}

fn aggregate_keys_from_bitmap<F>(
    bit_vec: &BitVec<u8, Lsb0>,
    rank_to_pubkey: &F,
) -> Option<PubkeyProjective>
where
    F: Fn(usize) -> Option<BlsPubkey> + Sync,
{
    let pubkeys: Result<Vec<_>, _> = bit_vec
        .iter_ones()
        .filter_map(rank_to_pubkey)
        .map(PubkeyProjective::try_from)
        .collect();
    let pubkeys = pubkeys.ok()?;
    PubkeyProjective::par_aggregate(pubkeys.par_iter()).ok()
}

pub fn verify_base2_certificate<F>(
    cert_to_verify: &Certificate,
    bit_vec: &BitVec<u8, Lsb0>,
    rank_to_pubkey: &F,
) -> Result<(), CertVerifyError>
where
    F: Fn(usize) -> Option<BlsPubkey> + Sync,
{
    let original_vote = cert_to_verify.cert_type.to_source_vote();

    let Ok(signed_payload) = bincode::serialize(&original_vote) else {
        return Err(CertVerifyError::SerializationFailed);
    };

    let Some(aggregate_bls_pubkey) = aggregate_keys_from_bitmap(bit_vec, rank_to_pubkey) else {
        return Err(CertVerifyError::KeyAggregationFailed);
    };

    if let Ok(true) =
        aggregate_bls_pubkey.verify_signature(&cert_to_verify.signature, &signed_payload)
    {
        Ok(())
    } else {
        Err(CertVerifyError::SignatureVerificationFailed)
    }
}

fn verify_base3_certificate<F>(
    cert_to_verify: &Certificate,
    bit_vec1: &BitVec<u8, Lsb0>,
    bit_vec2: &BitVec<u8, Lsb0>,
    rank_to_pubkey: &F,
) -> Result<(), CertVerifyError>
where
    F: Fn(usize) -> Option<BlsPubkey> + Sync,
{
    let Some((vote1, vote2)) = cert_to_verify.cert_type.to_source_votes() else {
        return Err(CertVerifyError::Base3EncodingOnUnexpectedCert(
            cert_to_verify.cert_type,
        ));
    };

    let Ok(signed_payload1) = bincode::serialize(&vote1) else {
        return Err(CertVerifyError::SerializationFailed);
    };
    let Ok(signed_payload2) = bincode::serialize(&vote2) else {
        return Err(CertVerifyError::SerializationFailed);
    };

    let messages_to_verify: Vec<&[u8]> = vec![&signed_payload1, &signed_payload2];

    // Aggregate the two sets of public keys separately from the two bitmaps.
    let Some(agg_pk1) = aggregate_keys_from_bitmap(bit_vec1, rank_to_pubkey) else {
        return Err(CertVerifyError::KeyAggregationFailed);
    };
    let Some(agg_pk2) = aggregate_keys_from_bitmap(bit_vec2, rank_to_pubkey) else {
        return Err(CertVerifyError::KeyAggregationFailed);
    };

    let pubkeys_affine: Vec<BlsPubkey> = vec![agg_pk1.into(), agg_pk2.into()];

    match SignatureProjective::par_verify_distinct_aggregated(
        &pubkeys_affine,
        &cert_to_verify.signature,
        &messages_to_verify,
    ) {
        Ok(true) => Ok(()),
        _ => Err(CertVerifyError::SignatureVerificationFailed),
    }
}

pub fn verify_votor_message_certificate<F>(
    cert_to_verify: &Certificate,
    max_len: usize,
    rank_to_pubkey: F,
) -> Result<(), CertVerifyError>
where
    F: Fn(usize) -> Option<BlsPubkey> + Sync,
{
    let decoded_bitmap =
        decode(&cert_to_verify.bitmap, max_len).map_err(CertVerifyError::BitmapDecodingFailed)?;

    match decoded_bitmap {
        solana_signer_store::Decoded::Base2(bit_vec) => {
            verify_base2_certificate(cert_to_verify, &bit_vec, &rank_to_pubkey)
        }
        solana_signer_store::Decoded::Base3(bit_vec1, bit_vec2) => {
            verify_base3_certificate(cert_to_verify, &bit_vec1, &bit_vec2, &rank_to_pubkey)
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_bls_signatures::{
            keypair::Keypair as BLSKeypair, signature::Signature as BLSSignature,
        },
        solana_hash::Hash,
        solana_signer_store::encode_base2,
        solana_votor::consensus_pool::certificate_builder::CertificateBuilder,
        solana_votor_messages::{consensus_message::VoteMessage, vote::Vote},
    };

    fn create_bls_keypairs(num_signers: usize) -> Vec<BLSKeypair> {
        (0..num_signers)
            .map(|_| BLSKeypair::new())
            .collect::<Vec<_>>()
    }

    fn create_signed_vote_message(
        bls_keypairs: &[BLSKeypair],
        vote: Vote,
        rank: usize,
    ) -> VoteMessage {
        let bls_keypair = &bls_keypairs[rank];
        let payload = bincode::serialize(&vote).expect("Failed to serialize vote");
        let signature: BLSSignature = bls_keypair.sign(&payload).into();
        VoteMessage {
            vote,
            signature,
            rank: rank as u16,
        }
    }

    fn create_signed_certificate_message(
        bls_keypairs: &[BLSKeypair],
        cert_type: CertificateType,
        ranks: &[usize],
    ) -> Certificate {
        let mut builder = CertificateBuilder::new(cert_type);
        // Assumes Base2 encoding (single vote type) for simplicity in this helper.
        let vote = cert_type.to_source_vote();
        let vote_messages: Vec<VoteMessage> = ranks
            .iter()
            .map(|&rank| create_signed_vote_message(bls_keypairs, vote, rank))
            .collect();

        builder
            .aggregate(&vote_messages)
            .expect("Failed to aggregate votes");
        builder.build().expect("Failed to build certificate")
    }

    #[test]
    fn test_verify_certificate_base2_valid() {
        let bls_keypairs = create_bls_keypairs(10);
        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        let cert = create_signed_certificate_message(
            &bls_keypairs,
            cert_type,
            &(0..5).collect::<Vec<_>>(),
        );
        assert!(verify_votor_message_certificate(&cert, 10, |rank| {
            bls_keypairs.get(rank).map(|kp| kp.public)
        })
        .is_ok());
    }

    #[test]
    fn test_verify_certificate_base3_valid() {
        let bls_keypairs = create_bls_keypairs(10);
        let slot = 20;
        let block_hash = Hash::new_unique();
        let notarize_vote = Vote::new_notarization_vote(slot, block_hash);
        let notarize_fallback_vote = Vote::new_notarization_fallback_vote(slot, block_hash);
        let mut all_vote_messages = Vec::new();
        (0..4).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(&bls_keypairs, notarize_vote, i))
        });
        (4..7).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(
                &bls_keypairs,
                notarize_fallback_vote,
                i,
            ))
        });
        let cert_type = CertificateType::NotarizeFallback(slot, block_hash);
        let mut builder = CertificateBuilder::new(cert_type);
        builder
            .aggregate(&all_vote_messages)
            .expect("Failed to aggregate votes");
        let cert = builder.build().expect("Failed to build certificate");
        assert!(verify_votor_message_certificate(&cert, 10, |rank| {
            bls_keypairs.get(rank).map(|kp| kp.public)
        })
        .is_ok());
    }

    #[test]
    fn test_verify_certificate_invalid_signature() {
        let bls_keypairs = create_bls_keypairs(10);

        let num_signers = 7;
        let slot = 10;
        let block_hash = Hash::new_unique();
        let cert_type = CertificateType::Notarize(slot, block_hash);
        let mut bitmap = BitVec::<u8, Lsb0>::new();
        bitmap.resize(num_signers, false);
        for i in 0..num_signers {
            bitmap.set(i, true);
        }
        let encoded_bitmap = encode_base2(&bitmap).unwrap();

        let cert = Certificate {
            cert_type,
            signature: BLSSignature::default(), // Use a default/wrong signature
            bitmap: encoded_bitmap,
        };
        assert_eq!(
            verify_votor_message_certificate(&cert, 10, |rank| {
                bls_keypairs.get(rank).map(|kp| kp.public)
            }),
            Err(CertVerifyError::SignatureVerificationFailed)
        );
    }
}
