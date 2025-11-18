use {
    bitvec::vec::BitVec,
    solana_bls_signatures::{Signature as BLSSignature, SignatureProjective},
    solana_clock::Slot,
    solana_hash::Hash,
    solana_signer_store::encode_base2,
    solana_votor_messages::{
        consensus_message::VoteMessage,
        rewards_certificate::{NotarRewardCertificate, SkipRewardCertificate},
        vote::Vote,
    },
    std::collections::HashMap,
};

/// Builds a signature and bitmap suitable for creating a rewards certificate.
fn build_sig_bitmap(
    votes: &HashMap<u16, VoteMessage>,
    max_rank: u16,
) -> Option<(BLSSignature, Vec<u8>)> {
    let mut bitmap = BitVec::repeat(false, max_rank as usize);
    for vote in votes.keys() {
        bitmap.set(*vote as usize, true);
    }
    let mut signature = SignatureProjective::identity();
    // XXX: panics below.
    signature
        .aggregate_with(votes.values().map(|v| &v.signature))
        .unwrap();
    // XXX: panics below.
    Some((signature.into(), encode_base2(&bitmap).unwrap()))
}

/// Per slot container for storing notar and skip votes for creating rewards certificates.
pub(super) struct Entry {
    /// Map from validator rank to the skip vote.
    skip: HashMap<u16, VoteMessage>,
    /// Largest ranked validator that voted skip.
    skip_max_rank: u16,
    /// Notar votes are indexed by block id as different validators may vote for different blocks.
    /// Per block id, store a map from validator rank to notar vote and also the largest ranked validator that voted notar.
    notar: HashMap<Hash, (HashMap<u16, VoteMessage>, u16)>,
    /// Maximum number of validators for the slot this entry is working on.
    max_validators: usize,
}

impl Entry {
    /// Creates a new instance of [`Entry`].
    pub(super) fn new(max_validators: usize) -> Self {
        Self {
            skip: HashMap::with_capacity(max_validators),
            skip_max_rank: 0,
            // under normal operations, all validators should vote for a single block id, still allocate space for a few more to hopefully avoid allocations.
            notar: HashMap::with_capacity(5),
            max_validators,
        }
    }

    pub(super) fn wants_vote(&self, vote: &VoteMessage) -> bool {
        match vote.vote {
            Vote::Skip(_) => !self.skip.contains_key(&vote.rank),
            Vote::Notarize(notar) => {
                let Some((notar, _)) = self.notar.get(&notar.block_id) else {
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
                let (map, max_rank) = self
                    .notar
                    .entry(notar.block_id)
                    .or_insert((HashMap::with_capacity(self.max_validators), 0));
                map.insert(vote.rank, vote);
                *max_rank = (*max_rank).max(vote.rank);
            }
            Vote::Skip(_) => {
                self.skip.insert(vote.rank, vote);
                self.skip_max_rank = self.skip_max_rank.max(vote.rank);
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
    ) {
        let skip = build_sig_bitmap(&self.skip, self.skip_max_rank).map(|(signature, bitmap)| {
            SkipRewardCertificate {
                slot,
                signature,
                bitmap,
            }
        });

        // we can only submit one notar rewards certificate but different validators may vote for different blocks and we cannot combine notar votes for different blocks together in one cert.
        // pick the block_id with most votes.
        // in practice, all validators should have voted for the same block otherwise, we have evidence of leader equivocation.
        // TODO: collect metrics for when equivocation is detected.
        let mut notar = None;
        for (block_id, (map, max_rank)) in &self.notar {
            match notar {
                None => notar = Some((block_id, map, max_rank)),
                Some((_, notar_map, _)) => {
                    if map.len() > notar_map.len() {
                        notar = Some((block_id, map, max_rank));
                    }
                }
            }
        }
        let notar = notar.and_then(|(block_id, votes, max_rank)| {
            build_sig_bitmap(votes, *max_rank).map(|(signature, bitmap)| NotarRewardCertificate {
                slot,
                block_id: *block_id,
                signature,
                bitmap,
            })
        });
        (skip, notar)
    }
}
