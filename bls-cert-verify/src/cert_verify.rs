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

/// Verifies a [`Certificate`] that is signed at most by [`max_validators`] using the provided [`rank_map`] closure to look up the [`BlsPubkey`] and stake.
///
/// The [`rank_map`] closure can also be used by the caller to perform its own computation based on which ranks are accessed by the verification logic.
///
/// On success, returns the total stake that signed the certificate.
pub fn verify_cert_get_total_stake(
    cert: &Certificate,
    max_validators: usize,
    mut rank_map: impl FnMut(usize) -> Option<(u64, BlsPubkey)>,
) -> Result<u64, Error> {
    let mut total_stake = 0u64;
    let rank_map = |ind: usize| {
        rank_map(ind).map(|(stake, pubkey)| {
            total_stake = total_stake.saturating_add(stake);
            pubkey
        })
    };

    // SAFETY: unwrap()s when serializing [`Vote`] below are safe as votes are not an input.

    let () = match cert.cert_type {
        CertificateType::Notarize(slot, block_id)
        | CertificateType::FinalizeFast(slot, block_id) => verify_base2(
            &bincode::serialize(&Vote::new_notarization_vote(slot, block_id)).unwrap(),
            &cert.signature,
            &cert.bitmap,
            max_validators,
            rank_map,
        ),
        CertificateType::Finalize(slot) => verify_base2(
            &bincode::serialize(&Vote::new_finalization_vote(slot)).unwrap(),
            &cert.signature,
            &cert.bitmap,
            max_validators,
            rank_map,
        ),
        CertificateType::Genesis(slot, block_id) => verify_base2(
            &bincode::serialize(&Vote::new_genesis_vote(slot, block_id)).unwrap(),
            &cert.signature,
            &cert.bitmap,
            max_validators,
            rank_map,
        ),
        CertificateType::NotarizeFallback(slot, block_id) => verify_base3(
            &bincode::serialize(&Vote::new_notarization_vote(slot, block_id)).unwrap(),
            &bincode::serialize(&Vote::new_notarization_fallback_vote(slot, block_id)).unwrap(),
            &cert.signature,
            &cert.bitmap,
            max_validators,
            rank_map,
        ),
        CertificateType::Skip(slot) => verify_base3(
            &bincode::serialize(&Vote::new_skip_vote(slot)).unwrap(),
            &bincode::serialize(&Vote::new_skip_fallback_vote(slot)).unwrap(),
            &cert.signature,
            &cert.bitmap,
            max_validators,
            rank_map,
        ),
    }?;
    Ok(total_stake)
}

/// Verifies the [`signature`] of the [`payload`] which is signed by at most [`max_validators`] validators in the base2 encoded [`ranks`] using the [`rank_map`] to lookup the [`BlsPubkey`].
///
/// The [`rank_map`] closure can also be used by the caller to perform its own computation based on which ranks are accessed by the verification logic.
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

    let pubkeys = if cfg!(debug_assertions) {
        collect_pubkeys(&ranks, checked_rank_map(rank_map, &ranks))?
    } else {
        collect_pubkeys(&ranks, rank_map)?
    };

    let pk = PubkeyProjective::par_aggregate(pubkeys.par_iter())?;

    pk.verify_signature(signature, payload)
        .map_err(|_| Error::VerifySigFalse)
}

/// Add assertions to ensure that the rank_map is only accessed for ranks that are actually set.
fn checked_rank_map<F>(
    mut rank_map: F,
    ranks: &BitVec<u8>,
) -> impl FnMut(usize) -> Option<BlsPubkey> + use<'_, F>
where
    F: FnMut(usize) -> Option<BlsPubkey>,
{
    move |ind: usize| {
        let pos = ranks
            .get(ind)
            .unwrap_or_else(|| panic!("{ind} is not valid in {ranks:?}"));
        assert!(pos == true, "{ind} is not set in {ranks:?}");
        rank_map(ind)
    }
}

/// Returns the [`PubkeyProjective`] by aggregating the [`BlsPubkey`] of the validators present in [`ranks`] using the [`rank_map`] to look up the [`BlsPubkey`].
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

/// Verifies the [`signature`] of [`payload`] and [`fallback_payload`] which is signed by the validators in the base3 encoded [`ranks`]  using the [`rank_map`] to lookup the [`BlsPubkey`].
///
/// [`rank_map`] is [`FnMut`] allowing caller to perform computation based on which validators signed the payload.
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
        Decoded::Base2(ranks) => {
            let pubkeys = if cfg!(debug_assertions) {
                collect_pubkeys(&ranks, checked_rank_map(rank_map, &ranks))?
            } else {
                collect_pubkeys(&ranks, rank_map)?
            };
            let pk = PubkeyProjective::par_aggregate(pubkeys.par_iter())?;

            pk.verify_signature(signature, payload)
                .map_err(|_| Error::VerifySigFalse)
        }
        Decoded::Base3(ranks, fallback_ranks) => {
            let (pubkeys_1, pubkeys_2) = if cfg!(debug_assertions) {
                (
                    collect_pubkeys(&ranks, checked_rank_map(&mut rank_map, &ranks))?,
                    collect_pubkeys(&fallback_ranks, checked_rank_map(rank_map, &fallback_ranks))?,
                )
            } else {
                (
                    collect_pubkeys(&ranks, &mut rank_map)?,
                    collect_pubkeys(&fallback_ranks, rank_map)?,
                )
            };

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
            verify_cert_get_total_stake(&cert, 10, |rank| {
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
            verify_cert_get_total_stake(&cert, 10, |rank| {
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
            verify_cert_get_total_stake(&cert, 10, |rank| {
                bls_keypairs.get(rank).map(|kp| (100, kp.public.into()))
            })
            .unwrap_err(),
            Error::VerifySigFalse
        );
    }
}
