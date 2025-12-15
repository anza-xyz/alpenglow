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
