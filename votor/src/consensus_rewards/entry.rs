use {
    bitvec::vec::BitVec,
    solana_bls_signatures::{Signature as BLSSignature, SignatureProjective},
    solana_clock::Slot,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    solana_signer_store::encode_base2,
    solana_votor_messages::{
        consensus_message::VoteMessage,
        rewards_certificate::{NotarRewardCertificate, SkipRewardCertificate},
        vote::Vote,
    },
    std::{collections::BTreeMap, sync::Arc},
};

/// Builds a signature and bitmap suitable for creating a rewards certificate.
fn build_sig_bitmap(
    votes: &BTreeMap<u16, VoteMessage>,
    rank_map: &BLSPubkeyToRankMap,
) -> Option<(BLSSignature, Vec<u8>, Vec<Pubkey>)> {
    let Some(max_rank) = votes.last_key_value().map(|(rank, _)| rank).cloned() else {
        return None;
    };
    let mut bitmap = BitVec::repeat(false, max_rank as usize);
    for vote in votes.keys() {
        bitmap.set(*vote as usize, true);
    }
    let mut signature = SignatureProjective::identity();
    // XXX: panics below.
    signature
        .aggregate_with(votes.values().map(|v| &v.signature))
        .unwrap();
    let validators = votes
        .keys()
        .map(|rank| rank_map.get_pubkey((*rank).try_into().unwrap()).unwrap().0)
        .collect();
    // XXX: panics below.
    Some((signature.into(), encode_base2(&bitmap).unwrap(), validators))
}

/// Per slot container for storing notar and skip votes for creating rewards certificates.
pub(super) struct Entry {
    /// map from validator rank to the skip vote.
    skip: BTreeMap<u16, VoteMessage>,
    /// notar votes are indexed by block id as different validators may vote for different blocks.
    notar: BTreeMap<Hash, BTreeMap<u16, VoteMessage>>,
    rank_map: Arc<BLSPubkeyToRankMap>,
}

impl Entry {
    pub(super) fn new(rank_map: Arc<BLSPubkeyToRankMap>) -> Self {
        Self {
            skip: BTreeMap::default(),
            notar: BTreeMap::default(),
            rank_map,
        }
    }

    pub(super) fn wants_vote(&self, vote: &VoteMessage) -> bool {
        match vote.vote {
            Vote::Skip(_) => !self.skip.contains_key(&vote.rank),
            Vote::Notarize(notar) => {
                let Some(notar) = self.notar.get(&notar.block_id) else {
                    return true;
                };
                !notar.contains_key(&vote.rank)
            }
            Vote::Finalize(_)
            | Vote::NotarizeFallback(_)
            | Vote::SkipFallback(_)
            | Vote::Genesis(_) => false,
        }
    }

    pub(super) fn add_vote(&mut self, vote: VoteMessage) {
        match vote.vote {
            Vote::Notarize(notar) => {
                self.notar
                    .entry(notar.block_id)
                    .or_default()
                    .insert(vote.rank, vote);
            }
            Vote::Skip(_) => {
                self.skip.insert(vote.rank, vote);
            }
            _ => (),
        }
    }

    pub(super) fn build_certs(
        &self,
        slot: Slot,
    ) -> (
        Option<SkipRewardCertificate>,
        Option<NotarRewardCertificate>,
        Vec<Pubkey>,
    ) {
        let (skip, skip_validators) = match build_sig_bitmap(&self.skip, &self.rank_map) {
            Some((signature, bitmap, skip_validators)) => (
                Some(SkipRewardCertificate {
                    slot,
                    signature,
                    bitmap,
                }),
                skip_validators,
            ),
            None => (None, vec![]),
        };

        // we can only submit one notar rewards certificate but different validators may vote for different blocks and we cannot combine notar votes for different blocks together in one cert.
        // pick the block_id with most votes.
        // in practice, all validators should have voted for the same block otherwise, we have evidence of leader equivocation.
        // TODO: collect metrics for when equivocation is detected.
        let mut notar = None;
        for (block_id, map) in &self.notar {
            match notar {
                None => notar = Some((block_id, map)),
                Some((_, ref notar_map)) => {
                    if map.len() > notar_map.len() {
                        notar = Some((block_id, map));
                    }
                }
            }
        }

        let (notar, notar_validators) = match notar {
            None => (None, vec![]),
            Some((block_id, votes)) => match build_sig_bitmap(votes, &self.rank_map) {
                None => (None, vec![]),
                Some((signature, bitmap, validators)) => (
                    Some(NotarRewardCertificate {
                        slot,
                        block_id: *block_id,
                        signature,
                        bitmap,
                    }),
                    validators,
                ),
            },
        };

        let mut validators = skip_validators;
        validators.extend_from_slice(&notar_validators);

        (skip, notar, validators)
    }
}
