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
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
}

/// Different types of errors that can be returned from building reward certs.
#[derive(Debug, Error)]
pub(super) enum BuildCertError {
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
}

/// State for when we have not seen votes from all validators.
struct InProgressState {
    /// In progress signature aggregate.
    signature: SignatureProjective,
    /// bitvec of ranks whose signatures is included in the aggregate above.
    bitvec: BitVec<u8, Lsb0>,
    /// the largest rank in the aggregate above.
    max_rank: u16,
    /// number of signatures in the aggregate above.
    cnt: usize,
}

/// State for when we have seen votes from all the validators so we can build the certificate already.
struct DoneState {
    /// The final aggregate signature.
    signature: BLSSignature,
    /// Bitmap of rank of validators included in the aggregate above.
    bitmap: Vec<u8>,
    /// max number of validators for this slot.
    max_validators: usize,
}

/// Struct to hold state for building a single reward cert.
enum PartialCert {
    /// Variant for when we have not seen votes from all validators.
    InProgress(InProgressState),
    /// Variant for when we have seen votes from all the validators.
    Done(DoneState),
}

impl PartialCert {
    /// Returns a new instance of [`PartialCert`].
    fn new(max_validators: usize) -> Self {
        let state = InProgressState {
            signature: SignatureProjective::identity(),
            bitvec: BitVec::repeat(false, max_validators),
            max_rank: 0,
            cnt: 0,
        };
        Self::InProgress(state)
    }

    /// Returns true if the [`PartialCert`] needs the vote else false.
    fn wants_vote(&self, vote: &VoteMessage) -> bool {
        match self {
            Self::Done(_) => false,
            Self::InProgress(state) => match state.bitvec.get(vote.rank as usize) {
                None => false,
                Some(ind) => !*ind,
            },
        }
    }

    /// Adds the given [`VoteMessage`] to the aggregate.
    fn add_vote(&mut self, vote: &VoteMessage) -> Result<(), AddVoteError> {
        match self {
            Self::Done(_) => Err(AddVoteError::Duplicate),
            Self::InProgress(state) => {
                match state.bitvec.get_mut(vote.rank as usize) {
                    None => return Err(AddVoteError::InvalidRank),
                    Some(mut ind) => {
                        if *ind {
                            return Err(AddVoteError::Duplicate);
                        }
                        state
                            .signature
                            .aggregate_with(std::iter::once(&vote.signature))?;
                        *ind = true;
                    }
                }
                state.max_rank = std::cmp::max(state.max_rank, vote.rank);
                state.cnt = state.cnt.saturating_add(1);
                if state.cnt == state.bitvec.len() {
                    *self = Self::Done(DoneState {
                        signature: state.signature.into(),
                        bitmap: encode_base2(&state.bitvec).map_err(AddVoteError::Encode)?,
                        max_validators: state.bitvec.len(),
                    });
                }
                Ok(())
            }
        }
    }

    /// Builds a signature and associated bitmap from the collected votes.
    ///
    /// Returns Ok(None) if no votes were collected.
    fn build_sig_bitmap(&self) -> Result<Option<(BLSSignature, Vec<u8>)>, BuildCertError> {
        match self {
            Self::Done(state) => Ok(Some((state.signature, state.bitmap.clone()))),
            Self::InProgress(state) => {
                if state.cnt == 0 {
                    return Ok(None);
                }
                let mut bitvec = state.bitvec.clone();
                bitvec.resize(state.max_rank as usize, false);
                let bitmap = encode_base2(&bitvec).map_err(BuildCertError::Encode)?;
                Ok(Some((state.signature.into(), bitmap)))
            }
        }
    }

    /// Returns how many votes have been seen.
    fn votes_seen(&self) -> usize {
        match self {
            Self::InProgress(state) => state.cnt,
            Self::Done(state) => state.max_validators,
        }
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
