use {
    bitvec::vec::BitVec,
    rayon::{iter::IntoParallelRefIterator, join},
    solana_bls_signatures::{
        pubkey::Pubkey as BlsPubkey, signature::AsSignatureAffine, BlsError, PubkeyProjective,
        Signature as BlsSignature, SignatureProjective, VerifiablePubkey,
    },
    solana_signer_store::{decode, DecodeError, Decoded},
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateType},
        vote::Vote,
    },
    thiserror::Error,
};

#[derive(Debug, PartialEq, Eq, Error)]
pub enum Error {
    #[error("missing rank in rank map")]
    MissingRank,
    #[error("missing rank map")]
    MissingRankMap,
    #[error("verify signature failed with {0:?}")]
    VerifySig(#[from] BlsError),
    #[error("verify signature return false")]
    VerifySigFalse,
    #[error("decoding bitmap failed with {0:?}")]
    Decode(DecodeError),
    #[error("wrong encoding base")]
    WrongEncoding,
}

/// Verifies a [`Certificate`] and calculates the total signing stake.
///
/// This function verifies that the aggregate signature matches the vote payload(s)
/// derived from the certificate type. It concurrently accumulates the stake of
/// the signers using the provided `rank_map`.
///
/// On success, returns the total stake.
pub fn verify_certificate(
    cert: &Certificate,
    max_validators: usize,
    mut rank_map: impl FnMut(usize) -> Option<(u64, BlsPubkey)>,
) -> Result<u64, Error> {
    let mut total_stake = 0u64;

    // Wrap the `rank_map` to accumulate stake as a side-effect
    let accumulating_rank_map = |ind: usize| {
        rank_map(ind).map(|(stake, pubkey)| {
            total_stake = total_stake.saturating_add(stake);
            pubkey
        })
    };

    let (primary_vote, fallback_vote) = get_vote_payloads(&cert.cert_type);
    let primary_payload = serialize_vote(&primary_vote);

    if let Some(fallback_vote) = fallback_vote {
        let fallback_payload = serialize_vote(&fallback_vote);
        verify_base3(
            &primary_payload,
            &fallback_payload,
            &cert.signature,
            &cert.bitmap,
            max_validators,
            accumulating_rank_map,
        )
    } else {
        verify_base2(
            &primary_payload,
            &cert.signature,
            &cert.bitmap,
            max_validators,
            accumulating_rank_map,
        )
    }?;

    Ok(total_stake)
}

fn get_vote_payloads(cert_type: &CertificateType) -> (Vote, Option<Vote>) {
    match *cert_type {
        CertificateType::Notarize(slot, hash) | CertificateType::FinalizeFast(slot, hash) => {
            (Vote::new_notarization_vote(slot, hash), None)
        }
        CertificateType::Finalize(slot) => (Vote::new_finalization_vote(slot), None),
        CertificateType::Genesis(slot, hash) => (Vote::new_genesis_vote(slot, hash), None),
        CertificateType::NotarizeFallback(slot, hash) => (
            Vote::new_notarization_vote(slot, hash),
            Some(Vote::new_notarization_fallback_vote(slot, hash)),
        ),
        CertificateType::Skip(slot) => (
            Vote::new_skip_vote(slot),
            Some(Vote::new_skip_fallback_vote(slot)),
        ),
    }
}

fn serialize_vote(vote: &Vote) -> Vec<u8> {
    // `expect` is safe because the `Vote` struct is composed entirely of primitive
    // types (u64, Hash, enums) that are inherently serializable and it is constructed
    // locally within this module.
    bincode::serialize(vote).expect("Vote serialization should never fail for valid Vote structs")
}

/// Verifies the `signature` of the `payload` signed by validators in `ranks`.
/// This handles the standard case where all validators signed the exact same payload.
pub fn verify_base2<S: AsSignatureAffine>(
    payload: &[u8],
    signature: &S,
    ranks: &[u8],
    max_validators: usize,
    rank_map: impl FnMut(usize) -> Option<BlsPubkey>,
) -> Result<(), Error> {
    let ranks = decode(ranks, max_validators).map_err(Error::Decode)?;
    let ranks = match ranks {
        Decoded::Base2(ranks) => ranks,
        Decoded::Base3(_, _) => return Err(Error::WrongEncoding),
    };
    verify_single_vote_signature(payload, signature, &ranks, rank_map)
}

fn verify_single_vote_signature<S: AsSignatureAffine>(
    payload: &[u8],
    signature: &S,
    ranks: &BitVec<u8>,
    rank_map: impl FnMut(usize) -> Option<BlsPubkey>,
) -> Result<(), Error> {
    let pubkeys = collect_pubkeys(ranks, rank_map)?;
    let pk = PubkeyProjective::par_aggregate(pubkeys.par_iter())?;
    pk.verify_signature(signature, payload)
        .map_err(|_| Error::VerifySigFalse)
}

/// Verifies a split-vote scenario where some validators signed `payload`
/// and others signed `fallback_payload`.
fn verify_base3(
    payload: &[u8],
    fallback_payload: &[u8],
    signature: &BlsSignature,
    ranks: &[u8],
    max_validators: usize,
    mut rank_map: impl FnMut(usize) -> Option<BlsPubkey>,
) -> Result<(), Error> {
    let ranks = decode(ranks, max_validators).map_err(Error::Decode)?;
    match ranks {
        Decoded::Base2(ranks) => verify_single_vote_signature(payload, signature, &ranks, rank_map),
        Decoded::Base3(ranks, fallback_ranks) => {
            // Must run sequentially because `rank_map` captures `total_stake` (FnMut).
            // We pass a mutable reference for the first call so we can reuse the
            // closure for the second.
            let pubkeys_1 = collect_pubkeys(&ranks, &mut rank_map)?;
            let pubkeys_2 = collect_pubkeys(&fallback_ranks, rank_map)?;

            let (pk1_res, pk2_res) = join(
                || PubkeyProjective::par_aggregate(pubkeys_1.par_iter()),
                || PubkeyProjective::par_aggregate(pubkeys_2.par_iter()),
            );

            let pubkeys = [pk1_res?, pk2_res?];

            SignatureProjective::par_verify_distinct_aggregated(
                &pubkeys,
                signature,
                &[payload, fallback_payload],
            )
            .map_err(|_| Error::VerifySigFalse)
        }
    }
}

/// Collects public keys sequentially
fn collect_pubkeys(
    ranks: &BitVec<u8>,
    mut rank_map: impl FnMut(usize) -> Option<BlsPubkey>,
) -> Result<Vec<BlsPubkey>, Error> {
    let mut pubkeys = Vec::with_capacity(ranks.count_ones());
    for rank in ranks.iter_ones() {
        let pubkey = rank_map(rank).ok_or(Error::MissingRank)?;
        pubkeys.push(pubkey);
    }
    Ok(pubkeys)
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
            &(0..6).collect::<Vec<_>>(),
        );
        assert_eq!(
            verify_certificate(&cert, 10, |rank| {
                bls_keypairs.get(rank).map(|kp| (100, kp.public.into()))
            })
            .unwrap(),
            600
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
            verify_certificate(&cert, 10, |rank| {
                bls_keypairs.get(rank).map(|kp| (100, kp.public.into()))
            })
            .unwrap(),
            700
        );
    }

    #[test]
    fn test_verify_certificate_invalid_signature() {
        let bls_keypairs = create_bls_keypairs(10);

        let num_signers = 7;
        let slot = 10;
        let block_hash = Hash::new_unique();
        let cert_type = CertificateType::Notarize(slot, block_hash);
        let mut bitmap = BitVec::new();
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
            verify_certificate(&cert, 10, |rank| {
                bls_keypairs.get(rank).map(|kp| (100, kp.public.into()))
            })
            .unwrap_err(),
            Error::VerifySigFalse
        );
    }
}
