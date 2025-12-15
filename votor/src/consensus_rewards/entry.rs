use {
    partial_cert::PartialCert,
    solana_bls_signatures::BlsError,
    solana_clock::Slot,
    solana_hash::Hash,
    solana_signer_store::EncodeError,
    solana_votor_messages::{
        consensus_message::VoteMessage,
        rewards_certificate::{NotarRewardCertificate, SkipRewardCertificate},
        vote::Vote,
    },
    std::collections::HashMap,
    thiserror::Error,
};

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
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
}

/// Different types of errors that can be returned from building reward certs.
#[derive(Debug, Error)]
pub(super) enum BuildCertError {
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
}
/// Per slot container for storing notar and skip votes for creating rewards certificates.
pub(super) struct Entry {
    skip: PartialCert,
    /// Notar votes are indexed by block id as different validators may vote for different blocks.
    notar: HashMap<Hash, PartialCert>,
    /// Maximum number of validators for the slot this entry is working on.
    max_validators: usize,
}

impl Entry {
    /// Creates a new instance of [`Entry`].
    pub(super) fn new(max_validators: usize) -> Self {
        Self {
            skip: PartialCert::new(max_validators),
            // under normal operations, all validators should vote for a single block id, still allocate space for a few more to hopefully avoid allocations.
            notar: HashMap::with_capacity(5),
            max_validators,
        }
    }

    /// Returns true if the [`Entry`] needs the vote else false.
    pub(super) fn wants_vote(&self, vote: &VoteMessage) -> bool {
        match vote.vote {
            Vote::Skip(_) => self.skip.wants_vote(vote),
            Vote::Notarize(notar) => match self.notar.get(&notar.block_id) {
                None => true,
                Some(sub_entry) => sub_entry.wants_vote(vote),
            },
            Vote::Finalize(_)
            | Vote::NotarizeFallback(_)
            | Vote::SkipFallback(_)
            | Vote::Genesis(_) => false,
        }
    }

    /// Adds the given [`VoteMessage`] to the aggregate.
    pub(super) fn add_vote(&mut self, vote: &VoteMessage) -> Result<(), AddVoteError> {
        match vote.vote {
            Vote::Notarize(notar) => {
                let sub_entry = self
                    .notar
                    .entry(notar.block_id)
                    .or_insert(PartialCert::new(self.max_validators));
                sub_entry.add_vote(vote)
            }
            Vote::Skip(_) => self.skip.add_vote(vote),
            _ => Ok(()),
        }
    }

    /// Builds a skip reward certificate from the collected votes.
    pub(super) fn build_skip_cert(
        &self,
        slot: Slot,
    ) -> Result<Option<SkipRewardCertificate>, BuildCertError> {
        self.skip.build_sig_bitmap().map(|r| {
            r.map(|(signature, bitmap)| SkipRewardCertificate {
                slot,
                signature,
                bitmap,
            })
        })
    }

    /// Builds a notar reward certificate from the collected votes.
    pub(super) fn build_notar_cert(
        &self,
        slot: Slot,
    ) -> Result<Option<NotarRewardCertificate>, BuildCertError> {
        // we can only submit one notar rewards certificate but different validators may vote for different blocks and we cannot combine notar votes for different blocks together in one cert.
        // pick the block_id with most votes.
        let mut notar = None;
        for (block_id, sub_entry) in &self.notar {
            match notar {
                None => notar = Some((block_id, sub_entry)),
                Some((_, max_sub_entry)) => {
                    if sub_entry.votes_seen() > max_sub_entry.votes_seen() {
                        notar = Some((block_id, sub_entry));
                    }
                }
            }
        }
        match notar {
            None => Ok(None),
            Some((block_id, sub_entry)) => sub_entry.build_sig_bitmap().map(|r| {
                r.map(|(signature, bitmap)| NotarRewardCertificate {
                    slot,
                    block_id: *block_id,
                    signature,
                    bitmap,
                })
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_bls_signatures::Keypair as BLSKeypair,
        solana_signer_store::{decode, Decoded},
    };

    fn validate_bitmap(bitmap: &[u8], num_set: usize, max_len: usize) {
        let bitvec = decode(bitmap, max_len).unwrap();
        match bitvec {
            Decoded::Base2(bitvec) => assert_eq!(bitvec.count_ones(), num_set),
            Decoded::Base3(_, _) => panic!("unexpected variant"),
        }
    }

    fn new_vote(vote: Vote, rank: usize) -> VoteMessage {
        let serialized = bincode::serialize(&vote).unwrap();
        let keypair = BLSKeypair::new();
        let signature = keypair.sign(&serialized).into();
        VoteMessage {
            vote,
            signature,
            rank: rank.try_into().unwrap(),
        }
    }

    #[test]
    fn validate_build_skip_cert() {
        let slot = 123;
        let mut entry = Entry::new(5);
        assert!(matches!(entry.build_skip_cert(slot), Ok(None)));
        let skip = Vote::new_skip_vote(7);
        let vote = new_vote(skip, 0);
        entry.add_vote(&vote).unwrap();
        let skip_cert = entry.build_skip_cert(slot).unwrap().unwrap();
        assert_eq!(skip_cert.slot, slot);
        validate_bitmap(&skip_cert.bitmap, 1, 5);
    }

    #[test]
    fn validate_build_notar_cert() {
        let slot = 123;
        let mut entry = Entry::new(5);
        assert!(matches!(entry.build_notar_cert(slot), Ok(None)));

        let blockid0 = Hash::new_unique();
        let blockid1 = Hash::new_unique();

        for rank in 0..2 {
            let notar = Vote::new_notarization_vote(slot, blockid0);
            let vote = new_vote(notar, rank);
            entry.add_vote(&vote).unwrap();
        }
        for rank in 2..5 {
            let notar = Vote::new_notarization_vote(slot, blockid1);
            let vote = new_vote(notar, rank);
            entry.add_vote(&vote).unwrap();
        }
        let notar_cert = entry.build_notar_cert(slot).unwrap().unwrap();
        assert_eq!(notar_cert.slot, slot);
        assert_eq!(notar_cert.block_id, blockid1);
        validate_bitmap(&notar_cert.bitmap, 3, 5);
    }
}
