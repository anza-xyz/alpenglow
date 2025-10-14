use {
    crate::common::{certificate_limits_and_vote_types, VoteType},
    bitvec::prelude::*,
    itertools::Itertools,
    solana_bls_signatures::{BlsError, SignatureProjective},
    solana_signer_store::{encode_base2, encode_base3, DecodeError, EncodeError},
    solana_votor_messages::consensus_message::{Certificate, CertificateMessage, VoteMessage},
    thiserror::Error,
};

/// Maximum number of validators in a certificate
///
/// There are around 1500 validators currently. For a clean power-of-two
/// implementation, we should choose either 2048 or 4096. Choose a more
/// conservative number 4096 for now. During build() we will cut off end
/// of the bitmaps if the tail contains only zeroes, so actual bitmap
/// length will be less than or equal to this number.
const MAXIMUM_VALIDATORS: usize = 4096;

#[derive(Debug, Error, PartialEq)]
pub enum CertificateError {
    #[error("BLS error: {0}")]
    BlsError(#[from] BlsError),
    #[error("solana-signer-store decode error: {0:?}")]
    DecodeError(DecodeError),
    #[error("solana-signer-store encode error: {0:?}")]
    EncodeError(EncodeError),
    #[error("Validator does not exist for given rank: {0}")]
    ValidatorDoesNotExist(u16),
}

/// A builder for creating a `CertificateMessage` by efficiently aggregating BLS signatures.
#[derive(Clone)]
pub struct VoteCertificateBuilder {
    certificate: Certificate,
    signature: SignatureProjective,
    // All certificates require at least 1 bitmap and some require 2 if they have two types of votes.
    // The order of the VoteType is defined in certificate_limits_and_vote_types.
    // We normally put fallback votes in the second bitmap.
    // The order of the VoteType is important, if you change it, you might interpret the bitmap incorrectly.
    bitmap_0: BitVec<u8, Lsb0>,
    bitmap_1: Option<BitVec<u8, Lsb0>>,
}

impl VoteCertificateBuilder {
    pub fn new(certificate_id: Certificate) -> Self {
        Self {
            certificate: certificate_id,
            signature: SignatureProjective::identity(),
            bitmap_0: BitVec::repeat(false, MAXIMUM_VALIDATORS),
            bitmap_1: None,
        }
    }

    /// Aggregates a slice of `VoteMessage`s into the builder.
    pub fn aggregate(&mut self, messages: &[VoteMessage]) -> Result<(), CertificateError> {
        let vote_types = certificate_limits_and_vote_types(self.certificate).1;
        for vote_message in messages {
            let rank = vote_message.rank as usize;
            if MAXIMUM_VALIDATORS <= rank {
                return Err(CertificateError::ValidatorDoesNotExist(vote_message.rank));
            }

            let vote_type = VoteType::get_type(&vote_message.vote);
            if vote_type == vote_types[0] {
                self.bitmap_0.set(rank, true);
            } else {
                assert_eq!(vote_type, vote_types[1]);
                self.bitmap_1
                    .get_or_insert(BitVec::repeat(false, MAXIMUM_VALIDATORS))
                    .set(rank, true);
            }
        }

        let signature_iter = messages
            .iter()
            .map(|vote_message| &vote_message.signature)
            .collect_vec();
        Ok(self.signature.aggregate_with(&signature_iter)?)
    }

    /// Builds a [`CertificateMessage`] from the builder.
    ///
    /// The built certificate considers both bitmaps for inclusion and is suitable for consensus.
    /// See [`Self::build_for_rewards`] for building certificates for rewards.
    pub fn build(mut self) -> Result<CertificateMessage, CertificateError> {
        match self.bitmap_1 {
            None => self.build_with_just_bitmap_0(),
            Some(mut bitmap_1) => {
                let last_one_0 = self.bitmap_0.last_one().map_or(0, |i| i.saturating_add(1));
                let last_one_1 = bitmap_1.last_one().map_or(0, |i| i.saturating_add(1));
                let new_length = last_one_0.max(last_one_1);
                self.bitmap_0.resize(new_length, false);
                bitmap_1.resize(new_length, false);
                let bitmap = encode_base3(&self.bitmap_0, &bitmap_1)
                    .map_err(CertificateError::EncodeError)?;
                Ok(CertificateMessage {
                    certificate: self.certificate,
                    signature: self.signature.into(),
                    bitmap,
                })
            }
        }
    }

    /// Builds a [`CertificateMessage`] from the builder that only considers the first bitmap for inclusion.
    ///
    /// This builds a certificate for rewards and as per the SIMD does not include fallback votes so does not include the second bitmap.
    pub fn build_for_rewards(self) -> Result<CertificateMessage, CertificateError> {
        self.build_with_just_bitmap_0()
    }

    fn build_with_just_bitmap_0(mut self) -> Result<CertificateMessage, CertificateError> {
        let new_len = self.bitmap_0.last_one().map_or(0, |i| i.saturating_add(1));
        self.bitmap_0.resize(new_len, false);
        let bitmap = encode_base2(&self.bitmap_0).map_err(CertificateError::EncodeError)?;
        Ok(CertificateMessage {
            certificate: self.certificate,
            signature: self.signature.into(),
            bitmap,
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_bls_signatures::{
            Keypair as BLSKeypair, PubkeyProjective as BLSPubkeyProjective,
            Signature as BLSSignature, SignatureProjective, VerifiablePubkey,
        },
        solana_hash::Hash,
        solana_signer_store::{decode, Decoded},
        solana_votor_messages::{
            consensus_message::{Certificate, CertificateType, VoteMessage},
            vote::Vote,
        },
    };

    #[test]
    fn test_normal_build() {
        let hash = Hash::new_unique();
        let certificate = Certificate::new(CertificateType::NotarizeFallback, 1, Some(hash));
        let mut builder = VoteCertificateBuilder::new(certificate);
        // Test building the certificate from Notarize and NotarizeFallback votes
        // Create Notarize on validator 1, 4, 6
        let vote = Vote::new_notarization_vote(1, hash);
        let rank_1 = [1, 4, 6];
        let messages_1 = rank_1
            .iter()
            .map(|&rank| {
                let keypair = BLSKeypair::new();
                let signature = keypair.sign(b"fake_vote_message");
                VoteMessage {
                    vote,
                    signature: signature.into(),
                    rank,
                }
            })
            .collect::<Vec<_>>();
        builder
            .aggregate(&messages_1)
            .expect("Failed to aggregate notarization votes");
        // Create NotarizeFallback on validator 2, 3, 5, 7
        let vote = Vote::new_notarization_fallback_vote(1, hash);
        let rank_2 = [2, 3, 5, 7];
        let messages_2 = rank_2
            .iter()
            .map(|&rank| {
                let keypair = BLSKeypair::new();
                let signature = keypair.sign(b"fake_vote_message_2");
                VoteMessage {
                    vote,
                    signature: signature.into(),
                    rank,
                }
            })
            .collect::<Vec<_>>();
        builder
            .aggregate(&messages_2)
            .expect("Failed to aggregate notarization fallback votes");

        let certificate_message = builder.build().expect("Failed to build certificate");
        assert_eq!(certificate_message.certificate, certificate);
        match decode(&certificate_message.bitmap, MAXIMUM_VALIDATORS)
            .expect("Failed to decode bitmap")
        {
            Decoded::Base3(bitmap1, bitmap2) => {
                assert_eq!(bitmap1.len(), 8);
                assert_eq!(bitmap2.len(), 8);
                for i in rank_1 {
                    assert!(bitmap1[i as usize]);
                }
                assert_eq!(bitmap1.count_ones(), 3);
                for i in rank_2 {
                    assert!(bitmap2[i as usize]);
                }
                assert_eq!(bitmap2.count_ones(), 4);
            }
            _ => panic!("Expected Base3 encoding"),
        }

        // Build a new certificate with only Notarize votes, we should get Base2 encoding
        let mut builder = VoteCertificateBuilder::new(certificate);
        builder
            .aggregate(&messages_1)
            .expect("Failed to aggregate notarization votes");
        let certificate_message = builder.build().expect("Failed to build certificate");
        assert_eq!(certificate_message.certificate, certificate);
        match decode(&certificate_message.bitmap, MAXIMUM_VALIDATORS)
            .expect("Failed to decode bitmap")
        {
            Decoded::Base2(bitmap1) => {
                assert_eq!(bitmap1.len(), 7);
                for i in rank_1 {
                    assert!(bitmap1[i as usize]);
                }
                assert_eq!(bitmap1.count_ones(), 3);
            }
            _ => panic!("Expected Base2 encoding"),
        }

        // Base2 encoding only applies when the first bitmap is non-empty, if we build another
        // certificate with only NotarizeFallback votes, we should still get Base3 encoding
        let mut builder = VoteCertificateBuilder::new(certificate);
        builder
            .aggregate(&messages_2)
            .expect("Failed to aggregate notarization fallback votes");
        let certificate_message = builder.build().expect("Failed to build certificate");
        assert_eq!(certificate_message.certificate, certificate);
        match decode(&certificate_message.bitmap, MAXIMUM_VALIDATORS)
            .expect("Failed to decode bitmap")
        {
            Decoded::Base3(bitmap1, bitmap2) => {
                assert_eq!(bitmap1.count_ones(), 0);
                assert_eq!(bitmap2.len(), 8);
                for i in rank_2 {
                    assert!(bitmap2[i as usize]);
                }
                assert_eq!(bitmap2.count_ones(), 4);
            }
            _ => panic!("Expected Base3 encoding"),
        }
    }

    #[test]
    fn test_builder_with_errors() {
        let hash = Hash::new_unique();
        let certificate = Certificate::new(CertificateType::NotarizeFallback, 1, Some(hash));
        let mut builder = VoteCertificateBuilder::new(certificate);

        // Test with a rank that exceeds the maximum allowed
        let vote = Vote::new_notarization_vote(1, hash);
        let vote2 = Vote::new_notarization_fallback_vote(1, hash);
        let rank_out_of_bounds = MAXIMUM_VALIDATORS.saturating_add(1); // Exceeds MAXIMUM_VALIDATORS
        let keypair = BLSKeypair::new();
        let signature = keypair.sign(b"fake_vote_message");
        let message_out_of_bounds = VoteMessage {
            vote,
            signature: signature.into(),
            rank: rank_out_of_bounds as u16,
        };
        assert_eq!(
            builder.aggregate(&[message_out_of_bounds]),
            Err(CertificateError::ValidatorDoesNotExist(
                rank_out_of_bounds as u16
            ))
        );

        // Test bls error
        let message_with_invalid_signature = VoteMessage {
            vote,
            signature: BLSSignature::default(), // Invalid signature
            rank: 1,
        };
        assert_eq!(
            builder.aggregate(&[message_with_invalid_signature]),
            Err(CertificateError::BlsError(BlsError::PointConversion))
        );

        // Test encoding error
        // Create two bitmaps with the same rank set
        let signature = keypair.sign(b"fake_vote_message_2");
        let messages_1 = vec![VoteMessage {
            vote,
            signature: signature.into(),
            rank: 1,
        }];
        let mut builder = VoteCertificateBuilder::new(certificate);
        builder
            .aggregate(&messages_1)
            .expect("Failed to aggregate notarization votes");
        let messages_2 = vec![VoteMessage {
            vote: vote2,
            signature: signature.into(),
            rank: 1, // Same rank as in messages_1
        }];
        builder
            .aggregate(&messages_2)
            .expect("Failed to aggregate notarization fallback votes");
        assert_eq!(
            builder.build(),
            Err(CertificateError::EncodeError(
                EncodeError::InvalidBitCombination
            ))
        );
    }

    #[test]
    fn test_certificate_verification_base2_encoding() {
        let slot = 10;
        let hash = Hash::new_unique();
        let certificate_id = Certificate::new(CertificateType::Notarize, slot, Some(hash));

        // 1. Setup: Create keypairs and a single vote object.
        // All validators will sign the same message, resulting in a single bitmap.
        let num_validators = 5;
        let mut keypairs = Vec::new();
        let mut vote_messages = Vec::new();
        let vote = Vote::new_notarization_vote(slot, hash);
        let serialized_vote = bincode::serialize(&vote).unwrap();

        for i in 0..num_validators {
            let keypair = BLSKeypair::new();
            let signature = keypair.sign(&serialized_vote);
            vote_messages.push(VoteMessage {
                vote,
                signature: signature.into(),
                rank: i as u16,
            });
            keypairs.push(keypair);
        }

        // 2. Generation: Aggregate votes and build the certificate. This will
        // use base2 encoding because it only contains one type of vote.
        let mut builder = VoteCertificateBuilder::new(certificate_id);
        builder
            .aggregate(&vote_messages)
            .expect("Failed to aggregate votes");
        let certificate_message = builder.build().expect("Failed to build certificate");

        // 3. Verification: Aggregate the public keys and verify the signature.
        let pubkey_refs: Vec<_> = keypairs.iter().map(|kp| &kp.public).collect();
        let aggregate_pubkey =
            BLSPubkeyProjective::aggregate(&pubkey_refs).expect("Failed to aggregate public keys");

        let verification_result =
            aggregate_pubkey.verify_signature(&certificate_message.signature, &serialized_vote);

        assert!(
            verification_result.unwrap_or(false),
            "BLS aggregate signature verification failed for base2 encoded certificate"
        );
    }

    #[test]
    fn test_certificate_verification_base3_encoding() {
        let slot = 20;
        let hash = Hash::new_unique();
        // A NotarizeFallback certificate can be composed of both Notarize and NotarizeFallback
        // votes.
        let certificate_id = Certificate::new(CertificateType::NotarizeFallback, slot, Some(hash));

        // 1. Setup: Create two groups of validators signing two different vote types.
        let mut all_vote_messages = Vec::new();
        let mut all_pubkeys = Vec::new();
        let mut all_messages = Vec::new();

        // Group 1: Signs a Notarize vote.
        let notarize_vote = Vote::new_notarization_vote(slot, hash);
        let serialized_notarize_vote = bincode::serialize(&notarize_vote).unwrap();
        for i in 0..3 {
            let keypair = BLSKeypair::new();
            let signature = keypair.sign(&serialized_notarize_vote);
            all_vote_messages.push(VoteMessage {
                vote: notarize_vote,
                signature: signature.into(),
                rank: i as u16, // Ranks 0, 1, 2
            });
            all_pubkeys.push(keypair.public);
            all_messages.push(serialized_notarize_vote.clone());
        }

        // Group 2: Signs a NotarizeFallback vote.
        let notarize_fallback_vote = Vote::new_notarization_fallback_vote(slot, hash);
        let serialized_fallback_vote = bincode::serialize(&notarize_fallback_vote).unwrap();
        for i in 3..6 {
            let keypair = BLSKeypair::new();
            let signature = keypair.sign(&serialized_fallback_vote);
            all_vote_messages.push(VoteMessage {
                vote: notarize_fallback_vote,
                signature: signature.into(),
                rank: i as u16, // Ranks 3, 4, 5
            });
            all_pubkeys.push(keypair.public);
            all_messages.push(serialized_fallback_vote.clone());
        }

        // 2. Generation: Aggregate votes. Because there are two vote types, this will use
        //    base3 encoding.
        let mut builder = VoteCertificateBuilder::new(certificate_id);
        builder
            .aggregate(&all_vote_messages)
            .expect("Failed to aggregate votes");
        let certificate_message = builder.build().expect("Failed to build certificate");

        // 3. Verification:
        let decoded_bitmap =
            decode(&certificate_message.bitmap, MAXIMUM_VALIDATORS).expect("Failed to decode");

        match decoded_bitmap {
            Decoded::Base2(_bitmap) => {
                panic!("Expected Base3 encoding, but got Base2 encoding");
            }
            Decoded::Base3(bitmap1, bitmap2) => {
                // Bitmap1 should correspond to the Notarize votes (ranks 0, 1, 2)
                assert_eq!(bitmap1.count_ones(), 3);
                assert!(bitmap1[0] && bitmap1[1] && bitmap1[2]);
                // Bitmap2 should correspond to the NotarizeFallback votes (ranks 3, 4, 5)
                assert_eq!(bitmap2.count_ones(), 3);
                assert!(bitmap2[3] && bitmap2[4] && bitmap2[5]);
            }
        }

        let pubkey_refs: Vec<_> = all_pubkeys.iter().collect();
        let message_refs: Vec<&[u8]> = all_messages.iter().map(|m| m.as_slice()).collect();

        SignatureProjective::verify_distinct_aggregated(
            &pubkey_refs,
            &certificate_message.signature,
            &message_refs,
        )
        .unwrap();
    }
}
