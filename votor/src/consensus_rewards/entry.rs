use {
    super::BuildRewardCertsResponse,
    notar_entry::NotarEntry,
    partial_cert::PartialCert,
    solana_bls_signatures::BlsError,
    solana_clock::Slot,
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    solana_votor_messages::{
        consensus_message::VoteMessage, reward_certificate::SkipRewardCertificate, vote::Vote,
    },
    std::sync::Arc,
    thiserror::Error,
};

mod notar_entry;
mod partial_cert;

/// Different types of errors that can be returned from adding votes.
#[derive(Debug, Error)]
pub(super) enum AddVoteError {
    #[error("rank on vote is invalid")]
    InvalidRank,
    #[error("duplicate vote")]
    Duplicate,
    #[error("BLS error: {0}")]
    Bls(#[from] BlsError),
}

/// Per slot container for storing notar and skip votes for creating rewards certificates.
#[derive(Clone)]
pub(super) struct Entry {
    /// [`PartialCert`] for observed skip votes.
    skip: PartialCert,
    /// Struct to store state for observed notar votes.
    notar: NotarEntry,
    /// Maximum number of validators for the slot this entry is working on.
    max_validators: usize,
}

impl Entry {
    /// Creates a new instance of [`Entry`].
    pub(super) fn new(max_validators: usize) -> Self {
        Self {
            skip: PartialCert::new(max_validators),
            notar: NotarEntry::new(max_validators),
            max_validators,
        }
    }

    /// Returns true if the [`Entry`] needs the vote else false.
    pub(super) fn wants_vote(&self, vote: &VoteMessage) -> bool {
        match vote.vote {
            Vote::Skip(_) => self.skip.wants_vote(vote.rank),
            Vote::Notarize(_) => self.notar.wants_vote(vote.rank),
            Vote::Finalize(_)
            | Vote::NotarizeFallback(_)
            | Vote::SkipFallback(_)
            | Vote::Genesis(_) => false,
        }
    }

    /// Adds the given [`VoteMessage`] to the aggregate.
    pub(super) fn add_vote(
        &mut self,
        rank_map: &Arc<BLSPubkeyToRankMap>,
        vote: &VoteMessage,
    ) -> Result<(), AddVoteError> {
        match vote.vote {
            Vote::Notarize(notar) => self.notar.add_vote(
                rank_map,
                vote.rank,
                &vote.signature,
                notar.block_id,
                self.max_validators,
            ),
            Vote::Skip(_) => self.skip.add_vote(rank_map, vote.rank, &vote.signature),
            _ => Ok(()),
        }
    }

    /// Builds reward certificates from the observed votes and returns a [`BuildRewardCertsResponse`].
    pub(super) fn build_certs(self, slot: Slot) -> BuildRewardCertsResponse {
        let (notar, notar_validators) = self.notar.build_cert(slot).unzip();
        let (skip, skip_validators) = match self.skip.build_sig_bitmap() {
            Err(e) => {
                warn!("build_sig_bitmap for skip cert failed with {e}");
                (None, None)
            }
            Ok((signature, bitmap, skip_validators)) => {
                match SkipRewardCertificate::try_new(slot, signature, bitmap) {
                    Err(e) => {
                        warn!("SkipRewardCertificate::try_ne() failed with {e}");
                        (None, None)
                    }
                    Ok(c) => (Some(c), Some(skip_validators)),
                }
            }
        };
        let mut validators = notar_validators.unwrap_or_default();
        validators.extend(skip_validators.unwrap_or_default());
        let reward_slot_and_validators = (!validators.is_empty()).then_some((slot, validators));
        BuildRewardCertsResponse {
            skip,
            notar,
            reward_slot_and_validators,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_bls_signatures::Keypair as BlsKeypair,
        solana_epoch_schedule::EpochSchedule,
        solana_hash::Hash,
        solana_pubkey::Pubkey,
        solana_runtime::{
            bank::Bank,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts, ValidatorVoteKeypairs,
            },
        },
        solana_signer_store::{decode, Decoded},
        std::collections::HashMap,
    };

    pub(crate) fn validate_bitmap(bitmap: &[u8], num_set: usize, max_len: usize) {
        let bitvec = decode(bitmap, max_len).unwrap();
        match bitvec {
            Decoded::Base2(bitvec) => assert_eq!(bitvec.count_ones(), num_set),
            Decoded::Base3(_, _) => panic!("unexpected variant"),
        }
    }

    pub(crate) fn new_vote(vote: Vote, rank: usize, keypairs: &[BlsKeypair]) -> VoteMessage {
        let serialized = bincode::serialize(&vote).unwrap();
        let signature = keypairs[rank].sign(&serialized).into();
        VoteMessage {
            vote,
            signature,
            rank: rank.try_into().unwrap(),
        }
    }

    pub(crate) fn get_rank_map_keypairs(
        max_validators: usize,
        slot: Slot,
    ) -> (Arc<BLSPubkeyToRankMap>, Vec<BlsKeypair>) {
        let validator_keypairs = (0..max_validators)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let keypair_map = validator_keypairs
            .iter()
            .map(|k| (k.bls_keypair.public, k.bls_keypair.clone()))
            .collect::<HashMap<_, _>>();
        let mut genesis_config = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![100; validator_keypairs.len()],
        )
        .genesis_config;
        genesis_config.epoch_schedule = EpochSchedule::without_warmup();
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let bank = Bank::new_from_parent(bank, &Pubkey::default(), slot);
        let rank_map = bank
            .epoch_stakes_from_slot(slot)
            .unwrap()
            .bls_pubkey_to_rank_map()
            .clone();
        let signing_keys = (0..max_validators)
            .map(|index| {
                keypair_map
                    .get(&rank_map.get_pubkey_and_stake(index).unwrap().1)
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        (rank_map, signing_keys)
    }

    #[test]
    fn validate_build_skip_cert() {
        let slot = 123;
        let max_validators = 5;
        let (rank_map, keypairs) = get_rank_map_keypairs(max_validators, slot);
        let mut entry = Entry::new(max_validators);
        let resp = entry.clone().build_certs(slot);
        assert_eq!(resp.skip, None);
        assert_eq!(resp.notar, None);

        let skip = Vote::new_skip_vote(7);
        let vote = new_vote(skip, 0, &keypairs);
        entry.add_vote(&rank_map, &vote).unwrap();
        let resp = entry.build_certs(slot);
        assert_eq!(resp.notar, None);
        let skip = resp.skip.unwrap();
        assert_eq!(skip.slot, slot);
        validate_bitmap(skip.bitmap(), 1, 5);
    }

    #[test]
    fn validate_build_notar_cert() {
        let slot = 123;
        let max_validators = 5;
        let (rank_map, keypairs) = get_rank_map_keypairs(max_validators, slot);

        let mut entry = Entry::new(max_validators);
        let resp = entry.clone().build_certs(slot);
        assert_eq!(resp.skip, None);
        assert_eq!(resp.notar, None);

        let blockid0 = Hash::new_unique();
        let blockid1 = Hash::new_unique();

        for rank in 0..2 {
            let notar = Vote::new_notarization_vote(slot, blockid0);
            let vote = new_vote(notar, rank, &keypairs);
            entry.add_vote(&rank_map, &vote).unwrap();
        }
        for rank in 2..5 {
            let notar = Vote::new_notarization_vote(slot, blockid1);
            let vote = new_vote(notar, rank, &keypairs);
            entry.add_vote(&rank_map, &vote).unwrap();
        }
        let resp = entry.build_certs(slot);
        assert_eq!(resp.skip, None);
        let notar = resp.notar.unwrap();
        assert_eq!(notar.slot, slot);
        assert_eq!(notar.block_id, blockid1);
        validate_bitmap(notar.bitmap(), 3, 5);
    }
}
