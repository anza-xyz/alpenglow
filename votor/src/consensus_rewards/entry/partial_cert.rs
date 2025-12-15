use {
    super::{AddVoteError, BuildCertError},
    bitvec::{order::Lsb0, vec::BitVec},
    solana_bls_signatures::{Signature as BLSSignature, SignatureProjective},
    solana_signer_store::encode_base2,
    solana_votor_messages::consensus_message::VoteMessage,
};

/// State for when we have not seen votes from all validators.
pub(super) struct InProgressState {
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
pub(super) struct DoneState {
    /// The final aggregate signature.
    signature: BLSSignature,
    /// Bitmap of rank of validators included in the aggregate above.
    bitmap: Vec<u8>,
    /// max number of validators for this slot.
    max_validators: usize,
}

/// Struct to hold state for building a single reward cert.
pub(super) enum PartialCert {
    /// Variant for when we have not seen votes from all validators.
    InProgress(InProgressState),
    /// Variant for when we have seen votes from all the validators.
    Done(DoneState),
}

impl PartialCert {
    /// Returns a new instance of [`PartialCert`].
    pub(super) fn new(max_validators: usize) -> Self {
        let state = InProgressState {
            signature: SignatureProjective::identity(),
            bitvec: BitVec::repeat(false, max_validators),
            max_rank: 0,
            cnt: 0,
        };
        Self::InProgress(state)
    }

    /// Returns true if the [`PartialCert`] needs the vote else false.
    pub(super) fn wants_vote(&self, vote: &VoteMessage) -> bool {
        match self {
            Self::Done(_) => false,
            Self::InProgress(state) => match state.bitvec.get(vote.rank as usize) {
                None => false,
                Some(ind) => !*ind,
            },
        }
    }

    /// Adds the given [`VoteMessage`] to the aggregate.
    pub(super) fn add_vote(&mut self, vote: &VoteMessage) -> Result<(), AddVoteError> {
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
    pub(super) fn build_sig_bitmap(
        &self,
    ) -> Result<Option<(BLSSignature, Vec<u8>)>, BuildCertError> {
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
    pub(super) fn votes_seen(&self) -> usize {
        match self {
            Self::InProgress(state) => state.cnt,
            Self::Done(state) => state.max_validators,
        }
    }
}
