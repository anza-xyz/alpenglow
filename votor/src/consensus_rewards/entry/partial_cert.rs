use {
    super::{AddVoteError, BuildCertError},
    bitvec::{order::Lsb0, vec::BitVec},
    solana_bls_signatures::{
        Signature as BLSSignature, SignatureCompressed as BLSSignatureCompressed,
        SignatureProjective,
    },
    solana_signer_store::encode_base2,
};

/// State for when we have not seen votes from all validators.
pub(super) struct InProgressState {
    /// In progress signature aggregate.
    signature: SignatureProjective,
    /// bitvec of ranks whose signatures is included in the aggregate above.
    bitvec: BitVec<u8, Lsb0>,
    /// number of signatures in the aggregate above.
    cnt: usize,
}

/// State for when we have seen votes from all the validators so the cert can be pre-built.
pub(super) struct DoneState {
    /// The final aggregate signature.
    signature: BLSSignatureCompressed,
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
            cnt: 0,
        };
        Self::InProgress(state)
    }

    /// Returns true if the [`PartialCert`] needs the vote else false.
    pub(super) fn wants_vote(&self, rank: u16) -> bool {
        match self {
            Self::Done(_) => false,
            Self::InProgress(state) => match state.bitvec.get(rank as usize) {
                None => false,
                Some(ind) => !*ind,
            },
        }
    }

    /// Adds a new observed vote to the aggregate.
    pub(super) fn add_vote(
        &mut self,
        rank: u16,
        signature: &BLSSignature,
    ) -> Result<(), AddVoteError> {
        match self {
            Self::Done(_) => Err(AddVoteError::Duplicate),
            Self::InProgress(state) => {
                match state.bitvec.get_mut(rank as usize) {
                    None => return Err(AddVoteError::InvalidRank),
                    Some(mut ind) => {
                        if *ind {
                            return Err(AddVoteError::Duplicate);
                        }
                        state.signature.aggregate_with(std::iter::once(signature))?;
                        *ind = true;
                    }
                }
                state.cnt = state.cnt.saturating_add(1);
                if state.cnt == state.bitvec.len() {
                    let signature = BLSSignature::from(state.signature).try_into().unwrap();
                    *self = Self::Done(DoneState {
                        signature,
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
    ) -> Result<Option<(BLSSignatureCompressed, Vec<u8>)>, BuildCertError> {
        match self {
            Self::Done(state) => Ok(Some((state.signature, state.bitmap.clone()))),
            Self::InProgress(state) => {
                if state.cnt == 0 {
                    return Ok(None);
                }
                let mut bitvec = state.bitvec.clone();
                let new_len = bitvec.last_one().map_or(0, |i| i.saturating_add(1));
                bitvec.resize(new_len, false);
                let bitmap = encode_base2(&bitvec).map_err(BuildCertError::Encode)?;
                let signature = BLSSignature::from(state.signature).try_into().unwrap();
                Ok(Some((signature, bitmap)))
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_bls_signatures::Keypair as BLSKeypair,
        solana_signer_store::{decode, Decoded},
        solana_votor_messages::{consensus_message::VoteMessage, vote::Vote},
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
    fn validate_votes_seen() {
        let max_validators = 2;
        let skip = Vote::new_skip_vote(7);
        let mut partial_cert = PartialCert::new(max_validators);
        for rank in 0..max_validators {
            let vote = new_vote(skip, rank);
            partial_cert.add_vote(vote.rank, &vote.signature).unwrap();
            assert_eq!(partial_cert.votes_seen(), rank + 1);
        }
    }

    #[test]
    fn validate_build_sig_bitmap() {
        let max_validators = 2;
        let mut partial_cert = PartialCert::new(max_validators);
        assert!(matches!(partial_cert.build_sig_bitmap(), Ok(None)));
        let skip = Vote::new_skip_vote(7);
        for rank in 0..max_validators {
            let vote = new_vote(skip, rank);
            partial_cert.add_vote(vote.rank, &vote.signature).unwrap();
            let (_signature, bitmap) = partial_cert.build_sig_bitmap().unwrap().unwrap();
            validate_bitmap(&bitmap, rank + 1, max_validators);
        }
    }

    #[test]
    fn validate_add_vote() {
        let mut partial_cert = PartialCert::new(2);
        let skip = Vote::new_skip_vote(7);
        let vote = new_vote(skip, 2);
        assert!(matches!(
            partial_cert.add_vote(vote.rank, &vote.signature),
            Err(AddVoteError::InvalidRank)
        ));
        let vote = new_vote(skip, 0);
        partial_cert.add_vote(vote.rank, &vote.signature).unwrap();
        assert!(matches!(
            partial_cert.add_vote(vote.rank, &vote.signature),
            Err(AddVoteError::Duplicate)
        ));
        let vote = new_vote(skip, 1);
        partial_cert.add_vote(vote.rank, &vote.signature).unwrap();
        let vote = new_vote(skip, 0);
        assert!(matches!(
            partial_cert.add_vote(vote.rank, &vote.signature),
            Err(AddVoteError::Duplicate)
        ));
    }

    #[test]
    fn validate_wants_vote() {
        let mut partial_cert = PartialCert::new(2);
        let skip = Vote::new_skip_vote(7);
        let vote = new_vote(skip, 2);
        assert!(!partial_cert.wants_vote(vote.rank));
        let vote = new_vote(skip, 0);
        assert!(partial_cert.wants_vote(vote.rank));
        partial_cert.add_vote(vote.rank, &vote.signature).unwrap();
        assert!(!partial_cert.wants_vote(vote.rank));
        let vote = new_vote(skip, 1);
        partial_cert.add_vote(vote.rank, &vote.signature).unwrap();
        assert!(!partial_cert.wants_vote(vote.rank));
    }
}
