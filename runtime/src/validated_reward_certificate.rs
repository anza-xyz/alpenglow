use {
    crate::epoch_stakes::VersionedEpochStakes,
    rayon::iter::IntoParallelRefIterator,
    solana_bls_signatures::{
        pubkey::VerifiablePubkey, Pubkey as BLSPubkey, PubkeyProjective, Signature as BLSSignature,
    },
    solana_pubkey::Pubkey,
    solana_signer_store::{decode, Decoded},
    solana_votor_messages::{
        rewards_certificate::{NotarRewardCertificate, SkipRewardCertificate},
        vote::Vote,
    },
};

fn aggregate_keys_from_bitmap(validators: Vec<BLSPubkey>) -> Result<PubkeyProjective, String> {
    let pubkeys = validators
        .into_iter()
        .map(PubkeyProjective::try_from)
        .collect::<Result<Vec<_>, _>>();
    let pubkeys = pubkeys.unwrap();
    Ok(PubkeyProjective::par_aggregate(pubkeys.par_iter()).unwrap())
}

fn verify_signature(
    vote: Vote,
    signature: &BLSSignature,
    validators: Vec<BLSPubkey>,
) -> Result<(), String> {
    let signed_payload = bincode::serialize(&vote).unwrap();
    let aggregate_bls_pubkey = aggregate_keys_from_bitmap(validators).unwrap();

    if aggregate_bls_pubkey
        .verify_signature(signature, &signed_payload)
        .unwrap()
    {
        Ok(())
    } else {
        unimplemented!();
    }
}

fn into_validators(
    vote: Vote,
    signature: &BLSSignature,
    bitmap: &[u8],
    epoch_stakes: &VersionedEpochStakes,
) -> Result<Vec<Pubkey>, String> {
    let rank_map = epoch_stakes.bls_pubkey_to_rank_map();
    let bitmap = decode(bitmap, rank_map.len()).unwrap();
    let bitmap = match bitmap {
        Decoded::Base2(bitmap) => bitmap,
        Decoded::Base3(_, _) => unimplemented!(),
    };
    let mut bls_pubkeys = vec![];
    let mut validators = vec![];
    for rank in bitmap.iter_ones() {
        let (pubkey, bls_pubkey) = rank_map.get_pubkey(rank).unwrap();
        validators.push(*pubkey);
        bls_pubkeys.push(*bls_pubkey);
    }
    verify_signature(vote, signature, bls_pubkeys)?;
    Ok(validators)
}

// XXX: should we have a cache of verified certs for forks in the chain?
pub struct ValidatedRewardCertificate {
    validators: Vec<Pubkey>,
}

impl ValidatedRewardCertificate {
    pub(crate) fn try_new(
        epoch_stakes: &VersionedEpochStakes,
        skip: &Option<SkipRewardCertificate>,
        notar: &Option<NotarRewardCertificate>,
    ) -> Result<Self, String> {
        let skip_validators = match skip {
            None => vec![],
            Some(skip) => into_validators(
                Vote::new_skip_vote(skip.slot),
                &skip.signature,
                &skip.bitmap,
                epoch_stakes,
            )?,
        };
        let notar_validators = match notar {
            None => vec![],
            Some(notar) => into_validators(
                Vote::new_notarization_vote(notar.slot, notar.block_id),
                &notar.signature,
                &notar.bitmap,
                epoch_stakes,
            )?,
        };
        let mut validators = skip_validators;
        validators.extend_from_slice(&notar_validators);
        Ok(Self { validators })
    }

    pub fn new_for_block_producer(validators: Vec<Pubkey>) -> Self {
        Self { validators }
    }

    pub(crate) fn into_validators(self) -> Vec<Pubkey> {
        self.validators
    }
}
