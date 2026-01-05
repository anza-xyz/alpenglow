use {
    notar_entry::NotarEntry,
    partial_cert::PartialCert,
    solana_bls_signatures::BlsError,
    solana_clock::Slot,
    solana_signer_store::EncodeError,
    solana_votor_messages::{
        consensus_message::VoteMessage,
        reward_certificate::{NotarRewardCertificate, RewardCertError, SkipRewardCertificate},
        vote::Vote,
    },
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
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
}

/// Different types of errors that can be returned from building reward certs.
#[derive(Debug, Error)]
pub(super) enum BuildCertError {
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
    #[error("creating a reward cert failed")]
    RewardCert(#[from] RewardCertError),
}

/// Per slot container for storing notar and skip votes for creating rewards certificates.
pub(super) struct Entry {
    /// [`PartialCert`] for observed skip votes.
    skip: PartialCert,
    /// struct to store state for observed notar votes.
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
    pub(super) fn add_vote(&mut self, vote: &VoteMessage) -> Result<(), AddVoteError> {
        match vote.vote {
            Vote::Notarize(notar) => self.notar.add_vote(
                vote.rank,
                &vote.signature,
                notar.block_id,
                self.max_validators,
            ),
            Vote::Skip(_) => self.skip.add_vote(vote.rank, &vote.signature),
            _ => Ok(()),
        }
    }

    /// Builds a [`SkipRewardCertificate`] from the collected votes.
    pub(super) fn build_skip_cert(
        &self,
        slot: Slot,
    ) -> Result<Option<SkipRewardCertificate>, BuildCertError> {
        match self.skip.build_sig_bitmap() {
            Err(e) => Err(e),
            Ok(None) => Ok(None),
            Ok(Some((signature, bitmap))) => {
                let cert = SkipRewardCertificate::try_new(slot, signature, bitmap)?;
                Ok(Some(cert))
            }
        }
    }

    /// Builds a [`NotarRewardCertificate`] from the collected votes.
    pub(super) fn build_notar_cert(
        &self,
        slot: Slot,
    ) -> Result<Option<NotarRewardCertificate>, BuildCertError> {
        self.notar.build_cert(slot)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_bls_signatures::Keypair as BLSKeypair,
        solana_hash::Hash,
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
        validate_bitmap(skip_cert.bitmap(), 1, 5);
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
        validate_bitmap(notar_cert.bitmap(), 3, 5);
    }
}
