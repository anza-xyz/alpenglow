use {
    crate::{bank::Bank, epoch_stakes::BLSPubkeyToRankMap},
    rayon::iter::IntoParallelRefIterator,
    solana_bls_signatures::{
        pubkey::VerifiablePubkey, BlsError, Pubkey as BLSPubkey, PubkeyProjective,
        SignatureCompressed as BLSSignatureCompressed,
    },
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signer_store::{decode, DecodeError, Decoded},
    solana_votor_messages::{
        reward_certificate::{NotarRewardCertificate, SkipRewardCertificate, NUM_SLOTS_FOR_REWARD},
        vote::Vote,
    },
    std::sync::Arc,
    thiserror::Error,
};

/// Different types of errors that can happen when trying to construct a [`ValidatedRewardCert`].
#[derive(Debug, PartialEq, Eq, Error)]
pub(crate) enum Error {
    #[error("skip or notar certs have invalid slot numbers")]
    InvalidSlotNumbers,
    #[error("rank map unavailable")]
    NoRankMap,
    #[error("decoding bitmap failed with {0:?}")]
    Decode(DecodeError),
    #[error("wrong encoding base")]
    WrongEncoding,
    #[error("missing rank in rank map")]
    MissingRank,
    #[error("verify signature failed with {0:?}")]
    VerifySig(#[from] BlsError),
    #[error("verify signature return false")]
    VerifySigFalse,
}

/// Verifies the [`BLSSignatureCompressed`] that signed the [`payload`] using the [`Vec<BLSPubkey>`].
fn verify_signature(
    payload: &[u8],
    signature: &BLSSignatureCompressed,
    validators: Vec<BLSPubkey>,
) -> Result<(), Error> {
    let pubkeys = validators
        .into_iter()
        .map(PubkeyProjective::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let aggregate_pubkey = PubkeyProjective::par_aggregate(pubkeys.par_iter())?;
    if aggregate_pubkey.verify_signature(signature, payload)? {
        Ok(())
    } else {
        Err(Error::VerifySigFalse)
    }
}

/// Extracts a list of validator pubkeys from the rank bitmap and pushes them to the validators Vec.
fn extract_validators(
    payload: &[u8],
    signature: &BLSSignatureCompressed,
    bitmap: &[u8],
    rank_map: &BLSPubkeyToRankMap,
    validators: &mut Vec<Pubkey>,
) -> Result<(), Error> {
    let bitmap = decode(bitmap, rank_map.len()).map_err(Error::Decode)?;
    let bitmap = match bitmap {
        Decoded::Base2(bitmap) => bitmap,
        Decoded::Base3(_, _) => return Err(Error::WrongEncoding),
    };
    let mut bls_pubkeys = vec![];
    for rank in bitmap.iter_ones() {
        let (pubkey, bls_pubkey, _) = rank_map
            .get_pubkey_and_stake(rank)
            .ok_or(Error::MissingRank)?;
        validators.push(*pubkey);
        bls_pubkeys.push(*bls_pubkey);
    }
    verify_signature(payload, signature, bls_pubkeys)?;
    Ok(())
}

/// Returns the rank map corresponding to the provided slot in the provided bank.
fn get_rank_map(bank: &Bank, slot: Slot) -> Option<&Arc<BLSPubkeyToRankMap>> {
    let stakes = bank.epoch_stakes_map();
    let epoch = bank.epoch_schedule().get_epoch(slot);
    stakes
        .get(&epoch)
        .map(|stake| stake.bls_pubkey_to_rank_map())
}

/// Extracts the slot corresponding to the provided reward certs.
///
/// Returns Ok(None) if no certs were provided.
/// Returns Error if the reward slot is invalid.
fn extract_slot(
    current_slot: Slot,
    skip: &Option<SkipRewardCertificate>,
    notar: &Option<NotarRewardCertificate>,
) -> Result<Option<Slot>, Error> {
    let slot = match (skip, notar) {
        (None, None) => return Ok(None),
        (Some(s), None) => s.slot,
        (None, Some(n)) => n.slot,
        (Some(s), Some(n)) => {
            if s.slot != n.slot {
                return Err(Error::InvalidSlotNumbers);
            }
            s.slot
        }
    };
    if slot.saturating_add(NUM_SLOTS_FOR_REWARD) != current_slot {
        return Err(Error::InvalidSlotNumbers);
    }
    Ok(Some(slot))
}

/// Struct built by validating incoming reward certs.
#[allow(dead_code)]
pub(crate) struct ValidatedRewardCert {
    /// List of validators that were present in the reward certs.
    validators: Vec<Pubkey>,
}

impl ValidatedRewardCert {
    /// If validattion of the provided reward certs succeeds, returns an instance of [`ValidatedRewardCert`].
    #[allow(dead_code)]
    pub(crate) fn try_new(
        bank: &Bank,
        skip: &Option<SkipRewardCertificate>,
        notar: &Option<NotarRewardCertificate>,
    ) -> Result<Self, Error> {
        let Some(slot) = extract_slot(bank.slot(), skip, notar)? else {
            return Ok(Self { validators: vec![] });
        };
        let rank_map = get_rank_map(bank, slot).ok_or(Error::NoRankMap)?;
        let max_validators = rank_map.len();

        let mut validators = Vec::with_capacity(max_validators);
        if let Some(skip) = skip {
            let vote = Vote::new_skip_vote(skip.slot);
            // unwrap should be safe as we contructed the vote ourselves.
            let payload = bincode::serialize(&vote).unwrap();
            extract_validators(
                &payload,
                &skip.signature,
                skip.bitmap(),
                rank_map,
                &mut validators,
            )?
        }
        if let Some(notar) = notar {
            let vote = Vote::new_notarization_vote(notar.slot, notar.block_id);
            // unwrap should be safe as we contructed the vote ourselves.
            let payload = bincode::serialize(&vote).unwrap();
            extract_validators(
                &payload,
                &notar.signature,
                notar.bitmap(),
                rank_map,
                &mut validators,
            )?
        }
        Ok(Self { validators })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::genesis_utils::{
            create_genesis_config_with_alpenglow_vote_accounts, ValidatorVoteKeypairs,
        },
        bitvec::vec::BitVec,
        solana_bls_signatures::{
            Keypair as BLSKeypair, Signature as BLSSignature, SignatureProjective,
        },
        solana_hash::Hash,
        solana_signer_store::encode_base2,
        solana_votor_messages::consensus_message::VoteMessage,
        std::collections::HashMap,
    };

    fn new_vote(vote: Vote, rank: usize, keypair: &BLSKeypair) -> VoteMessage {
        let serialized = bincode::serialize(&vote).unwrap();
        let signature = keypair.sign(&serialized).into();
        VoteMessage {
            vote,
            signature,
            rank: rank.try_into().unwrap(),
        }
    }

    fn build_sig_bitmap(votes: &[VoteMessage]) -> (BLSSignatureCompressed, Vec<u8>) {
        let max_rank = votes.last().unwrap().rank;
        let mut signature = SignatureProjective::identity();
        let mut bitvec = BitVec::repeat(false, (max_rank + 1) as usize);
        for vote in votes {
            signature
                .aggregate_with(std::iter::once(&vote.signature))
                .unwrap();
            bitvec.set(vote.rank as usize, true);
        }
        (
            BLSSignature::from(signature).try_into().unwrap(),
            encode_base2(&bitvec).unwrap(),
        )
    }

    #[test]
    fn validate_try_new() {
        let reward_slot = 1;
        let bank_slot = reward_slot + NUM_SLOTS_FOR_REWARD;
        let num_skip_validators = 3;
        let num_notar_validators = 5;
        let num_validators = num_skip_validators + num_notar_validators;

        let validator_keypairs = (0..num_validators)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let keypair_map = validator_keypairs
            .iter()
            .map(|k| (k.bls_keypair.public, k.bls_keypair.clone()))
            .collect::<HashMap<_, _>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![100; validator_keypairs.len()],
        );
        let bank = Arc::new(Bank::new_for_tests(&genesis.genesis_config));
        let bank = Bank::new_from_parent(bank, &Pubkey::default(), bank_slot);

        let rank_map = get_rank_map(&bank, reward_slot).unwrap();
        let signing_keys = (0..num_validators)
            .map(|index| {
                keypair_map
                    .get(&rank_map.get_pubkey_and_stake(index).unwrap().1)
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let blockid = Hash::new_unique();
        let notar_vote = Vote::new_notarization_vote(reward_slot, blockid);
        let notar_votes = (0..num_notar_validators)
            .map(|rank| new_vote(notar_vote, rank, signing_keys[rank]))
            .collect::<Vec<_>>();
        let (signature, bitmap) = build_sig_bitmap(&notar_votes);
        let notar_reward_cert =
            NotarRewardCertificate::try_new(reward_slot, blockid, signature, bitmap).unwrap();

        let skip_vote = Vote::new_skip_vote(reward_slot);
        let skip_votes = (num_notar_validators..num_validators)
            .map(|rank| new_vote(skip_vote, rank, signing_keys[rank]))
            .collect::<Vec<_>>();
        let (signature, bitmap) = build_sig_bitmap(&skip_votes);
        let skip_reward_cert =
            SkipRewardCertificate::try_new(reward_slot, signature, bitmap).unwrap();

        let validated_reward_cert =
            ValidatedRewardCert::try_new(&bank, &Some(skip_reward_cert), &Some(notar_reward_cert))
                .unwrap();
        assert_eq!(validated_reward_cert.validators.len(), num_validators);
    }
}
