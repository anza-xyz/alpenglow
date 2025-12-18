//! The BLS Cert Verify logic.

use {
    bitvec::prelude::{BitVec, Lsb0},
    rayon::prelude::*,
    solana_bls_signatures::{
        pubkey::{Pubkey as BlsPubkey, PubkeyProjective, VerifiablePubkey},
        signature::SignatureProjective,
    },
    solana_signer_store::{decode, DecodeError},
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateType},
        vote::Vote,
    },
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

fn aggregate_keys_and_stakes_from_bitmap<F>(
    bit_vec: &BitVec<u8, Lsb0>,
    rank_to_pubkey_and_stake: &F,
) -> Option<(PubkeyProjective, u64)>
where
    F: Fn(usize) -> Option<(BlsPubkey, u64)> + Sync,
{
    // This function should return None if any of the following happens:
    // - A rank in the bitmap does not have a corresponding pubkey.
    // - A pubkey fails to convert to projective form.
    // It only returns Some(aggregated_pubkey) if all pubkeys are found and valid.
    let results: Option<Vec<_>> = bit_vec
        .iter_ones()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|rank| {
            let (pubkey, stake) = rank_to_pubkey_and_stake(rank)?;
            let pk_proj = PubkeyProjective::try_from(&pubkey).ok()?;
            Some((pk_proj, stake))
        })
        .collect();

    let results = results?; // fail-fast

    let total_stake = results.iter().map(|(_, stake)| *stake).sum::<u64>();
    let pubkeys: Vec<_> = results.into_iter().map(|(pk, _)| pk).collect();

    let aggregated = PubkeyProjective::par_aggregate(pubkeys.par_iter()).ok()?;
    Some((aggregated, total_stake))
}

fn serialize_vote(vote: &Vote) -> Result<Vec<u8>, CertVerifyError> {
    bincode::serialize(vote).map_err(|_| CertVerifyError::SerializationFailed)
}

pub fn verify_base2_certificate<F>(
    cert_to_verify: &Certificate,
    bit_vec: &BitVec<u8, Lsb0>,
    rank_to_pubkey_and_stake: &F,
) -> Result<u64, CertVerifyError>
where
    F: Fn(usize) -> Option<(BlsPubkey, u64)> + Sync,
{
    let original_vote = cert_to_verify.cert_type.to_source_vote();

    let signed_payload = serialize_vote(&original_vote)?;

    let (aggregate_bls_pubkey, aggregate_stake) =
        aggregate_keys_and_stakes_from_bitmap(bit_vec, rank_to_pubkey_and_stake)
            .ok_or(CertVerifyError::KeyAggregationFailed)?;

    if let Ok(true) =
        aggregate_bls_pubkey.verify_signature(&cert_to_verify.signature, &signed_payload)
    {
        Ok(aggregate_stake)
    } else {
        Err(CertVerifyError::SignatureVerificationFailed)
    }
}

fn verify_base3_certificate<F>(
    cert_to_verify: &Certificate,
    bit_vec1: &BitVec<u8, Lsb0>,
    bit_vec2: &BitVec<u8, Lsb0>,
    rank_to_pubkey_and_stake: &F,
) -> Result<u64, CertVerifyError>
where
    F: Fn(usize) -> Option<(BlsPubkey, u64)> + Sync,
{
    let (vote1, vote2) = cert_to_verify.cert_type.to_source_votes().ok_or(
        CertVerifyError::Base3EncodingOnUnexpectedCert(cert_to_verify.cert_type),
    )?;
    let signed_payload1 = serialize_vote(&vote1)?;
    let signed_payload2 = serialize_vote(&vote2)?;

    let messages_to_verify: Vec<&[u8]> = vec![&signed_payload1, &signed_payload2];

    // Aggregate the two sets of public keys separately from the two bitmaps.
    let (agg_pk1, stake1) =
        aggregate_keys_and_stakes_from_bitmap(bit_vec1, rank_to_pubkey_and_stake)
            .ok_or(CertVerifyError::KeyAggregationFailed)?;
    let (agg_pk2, stake2) =
        aggregate_keys_and_stakes_from_bitmap(bit_vec2, rank_to_pubkey_and_stake)
            .ok_or(CertVerifyError::KeyAggregationFailed)?;

    let pubkeys_affine: Vec<BlsPubkey> = vec![agg_pk1.into(), agg_pk2.into()];

    let aggregate_stake = stake1
        .checked_add(stake2)
        .expect("Stake addition should never overflow");

    match SignatureProjective::par_verify_distinct_aggregated(
        &pubkeys_affine,
        &cert_to_verify.signature,
        &messages_to_verify,
    ) {
        Ok(true) => Ok(aggregate_stake),
        _ => Err(CertVerifyError::SignatureVerificationFailed),
    }
}

pub fn verify_votor_message_certificate<F>(
    cert_to_verify: &Certificate,
    max_len: usize,
    rank_to_pubkey_and_stake: F,
) -> Result<u64, CertVerifyError>
where
    F: Fn(usize) -> Option<(BlsPubkey, u64)> + Sync,
{
    let decoded_bitmap =
        decode(&cert_to_verify.bitmap, max_len).map_err(CertVerifyError::BitmapDecodingFailed)?;

    match decoded_bitmap {
        solana_signer_store::Decoded::Base2(bit_vec) => {
            verify_base2_certificate(cert_to_verify, &bit_vec, &rank_to_pubkey_and_stake)
        }
        solana_signer_store::Decoded::Base3(bit_vec1, bit_vec2) => verify_base3_certificate(
            cert_to_verify,
            &bit_vec1,
            &bit_vec2,
            &rank_to_pubkey_and_stake,
        ),
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_bls_signatures::{
            keypair::Keypair as BLSKeypair, pubkey::Pubkey as BLSPubkey,
            signature::Signature as BLSSignature,
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
            &(0..6).collect::<Vec<_>>(),
        );
        assert_eq!(
            verify_votor_message_certificate(&cert, 10, |rank| {
                bls_keypairs.get(rank).map(|kp| (kp.public, 100))
            }),
            Ok(600)
        );
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
        assert_eq!(
            verify_votor_message_certificate(&cert, 10, |rank| {
                bls_keypairs.get(rank).map(|kp| (kp.public, 100))
            }),
            Ok(700)
        );
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
                bls_keypairs.get(rank).map(|kp| (kp.public, 100))
            }),
            Err(CertVerifyError::SignatureVerificationFailed)
        );
    }

    fn rank_to_pubkey_and_stake(
        bls_keypairs: &[BLSKeypair],
        bad_pubkey_rank: usize,
    ) -> impl Fn(usize) -> Option<(BlsPubkey, u64)> + Sync + '_ {
        move |rank| {
            if rank == bad_pubkey_rank {
                Some((BLSPubkey::default(), 100))
            } else {
                bls_keypairs
                    .get(rank)
                    .map(|keypair: &BLSKeypair| (keypair.public, 100))
            }
        }
    }

    #[test]
    fn test_aggregate_keys_and_stakes_from_bitmap() {
        let bls_keypairs = create_bls_keypairs(4);
        let mut bitmap = BitVec::<u8, Lsb0>::new();
        bitmap.resize(10, false);
        bitmap.set(1, true);
        bitmap.set(3, true);

        assert!(aggregate_keys_and_stakes_from_bitmap(
            &bitmap,
            &rank_to_pubkey_and_stake(&bls_keypairs, 4)
        )
        .is_some());

        bitmap.set(4, true); // rank 4 gets us a bad pubkey
        assert!(aggregate_keys_and_stakes_from_bitmap(
            &bitmap,
            &rank_to_pubkey_and_stake(&bls_keypairs, 4)
        )
        .is_none());

        bitmap.set(4, false);
        bitmap.set(6, true); // rank 6 does not have a corresponding pubkey
        assert!(aggregate_keys_and_stakes_from_bitmap(
            &bitmap,
            &rank_to_pubkey_and_stake(&bls_keypairs, 4)
        )
        .is_none());
    }
}
