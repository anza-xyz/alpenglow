use {
    bitvec::{order::Lsb0, vec::BitVec},
    solana_bls_signatures::{BlsError, Signature as BLSSignature, SignatureProjective},
    solana_clock::Slot,
    solana_hash::Hash,
    solana_signer_store::{encode_base2, EncodeError},
    solana_votor_messages::{
        consensus_message::VoteMessage,
        rewards_certificate::{NotarRewardCertificate, SkipRewardCertificate},
        vote::Vote,
    },
    std::collections::HashMap,
    thiserror::Error,
};

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

/// Different types of errors that can be returned from building reward certs.
#[derive(Debug, Error)]
pub(super) enum BuildCertError {
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
}

/// Struct to hold state for building a single reward cert.
struct PartialCert {
    /// In progress signature aggregate.
    signature: SignatureProjective,
    /// bitvec of ranks whose signatures is included in the aggregate above.
    bitvec: BitVec<u8, Lsb0>,
    /// the largest rank in the aggregate above.
    max_rank: u16,
    /// number of signatures in the aggregate above.
    cnt: usize,
}

impl PartialCert {
    /// Returns a new instance of [`PartialCert`].
    fn new(max_validator: usize) -> Self {
        Self {
            signature: SignatureProjective::identity(),
            bitvec: BitVec::repeat(false, max_validator),
            max_rank: 0,
            cnt: 0,
        }
    }

    /// Returns true if the [`PartialCert`] needs the vote else false.
    fn wants_vote(&self, vote: &VoteMessage) -> bool {
        match self.bitvec.get(vote.rank as usize) {
            None => false,
            Some(ind) => !*ind,
        }
    }

    /// Adds the given [`VoteMessage`] to the aggregate.
    fn add_vote(&mut self, vote: &VoteMessage) -> Result<(), AddVoteError> {
        match self.bitvec.get_mut(vote.rank as usize) {
            None => Err(AddVoteError::InvalidRank),
            Some(mut ind) => {
                if *ind {
                    return Err(AddVoteError::Duplicate);
                }
                self.signature
                    .aggregate_with(std::iter::once(&vote.signature))?;
                *ind = true;
                self.max_rank = std::cmp::max(self.max_rank, vote.rank);
                self.cnt = self.cnt.saturating_add(1);
                Ok(())
            }
        }
    }

    /// Builds a signature and associated bitmap from the collected votes.
    ///
    /// Returns Ok(None) if on votes were collected.
    fn build_sig_bitmap(&self) -> Result<Option<(BLSSignature, Vec<u8>)>, BuildCertError> {
        if self.cnt == 0 {
            return Ok(None);
        }
        let mut bitvec = self.bitvec.clone();
        bitvec.resize(self.max_rank as usize, false);
        let bitmap = encode_base2(&bitvec).map_err(BuildCertError::Encode)?;
        Ok(Some((self.signature.into(), bitmap)))
    }
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

    /// Builds skip and notar reward certificates from the collected votes.
    pub(super) fn build_certs(
        &self,
        slot: Slot,
    ) -> (
        Result<Option<SkipRewardCertificate>, BuildCertError>,
        Result<Option<NotarRewardCertificate>, BuildCertError>,
    ) {
        let skip = self.skip.build_sig_bitmap().map(|r| {
            r.map(|(signature, bitmap)| SkipRewardCertificate {
                slot,
                signature,
                bitmap,
            })
        });

        // we can only submit one notar rewards certificate but different validators may vote for different blocks and we cannot combine notar votes for different blocks together in one cert.
        // pick the block_id with most votes.
        let mut notar = None;
        for (block_id, sub_entry) in &self.notar {
            match notar {
                None => notar = Some((block_id, sub_entry)),
                Some((_, max_sub_entry)) => {
                    if sub_entry.cnt > max_sub_entry.cnt {
                        notar = Some((block_id, sub_entry));
                    }
                }
            }
        }
        let notar = match notar {
            None => Ok(None),
            Some((block_id, sub_entry)) => sub_entry.build_sig_bitmap().map(|r| {
                r.map(|(signature, bitmap)| NotarRewardCertificate {
                    slot,
                    block_id: *block_id,
                    signature,
                    bitmap,
                })
            }),
        };
        (skip, notar)
    }
}
