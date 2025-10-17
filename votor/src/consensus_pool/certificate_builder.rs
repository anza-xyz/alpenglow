use {
    crate::common::{certificate_limits_and_vote_types, VoteType},
    bitvec::prelude::*,
    solana_bls_signatures::{BlsError, SignatureProjective},
    solana_signer_store::{encode_base2, encode_base3, EncodeError},
    solana_votor_messages::consensus_message::{Certificate, CertificateMessage, VoteMessage},
    thiserror::Error,
};

/// Maximum number of validators in a certificate.
///
/// There are around 1500 validators currently. For a clean power-of-two
/// implementation, we should choose either 2048 or 4096. Choose a more
/// conservative number 4096 for now. During build() we will cut off end
/// of the bitmaps if the tail contains only zeroes, so actual bitmap
/// length will be less than or equal to this number.
const MAXIMUM_VALIDATORS: usize = 4096;

/// Different types of errors that can be returned from the [`CertificateBuilder::aggregate()`] function.
#[derive(Debug, Error)]
pub(super) enum AggregateError {
    #[error("BLS error: {0}")]
    Bls(#[from] BlsError),
    #[error("Validator does not exist for given rank: {0}")]
    ValidatorDoesNotExist(u16),
}

/// Different types of errors that can be returned from the [`CertificateBuilder::build()`] function.
#[derive(Debug, Error, PartialEq)]
pub enum BuildError {
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
    #[error("BLS error: {0}")]
    Bls(#[from] BlsError),
}

/// Different types of errors that can be returned from the [`CertificateBuilder::build_for_rewards()`] function.
#[derive(Debug, Error, PartialEq)]
pub enum BuildForRewardsError {
    #[error("Encoding failed: {0:?}")]
    Encode(EncodeError),
    #[error("rewards certs of these types are not needed")]
    InvalidCertType,
}

fn default_bitvec() -> BitVec<u8, Lsb0> {
    BitVec::repeat(false, MAXIMUM_VALIDATORS)
}

/// Build a [`CertificateMessage`] from a single bitmap.
fn build_cert_from_bitmap(
    certificate: Certificate,
    signature: SignatureProjective,
    mut bitmap: BitVec<u8, Lsb0>,
) -> Result<CertificateMessage, EncodeError> {
    let new_len = bitmap.last_one().map_or(0, |i| i.saturating_add(1));
    bitmap.resize(new_len, false);
    let bitmap = encode_base2(&bitmap)?;
    Ok(CertificateMessage {
        certificate,
        signature: signature.into(),
        bitmap,
    })
}

/// Build a [`CertificateMessage`] from two bitmaps.
fn build_cert_from_bitmaps(
    certificate: Certificate,
    signature: SignatureProjective,
    mut bitmap0: BitVec<u8, Lsb0>,
    mut bitmap1: BitVec<u8, Lsb0>,
) -> Result<CertificateMessage, BuildError> {
    let last_one_0 = bitmap0.last_one().map_or(0, |i| i.saturating_add(1));
    let last_one_1 = bitmap1.last_one().map_or(0, |i| i.saturating_add(1));
    let new_length = last_one_0.max(last_one_1);
    bitmap0.resize(new_length, false);
    bitmap1.resize(new_length, false);
    let bitmap = encode_base3(&bitmap0, &bitmap1).map_err(BuildError::Encode)?;
    Ok(CertificateMessage {
        certificate,
        signature: signature.into(),
        bitmap,
    })
}

/// Internal builder for creating [`CertificateMessage`] by using BLS signature aggregation.
#[allow(clippy::large_enum_variant)]
enum BuilderType {
    /// The produced [`CertificateMessage`] will require only one type of [`VoteMessage`].
    SingleVote {
        signature: SignatureProjective,
        bitmap: BitVec<u8, Lsb0>,
    },
    /// A [`CertificateMessage`] of type Skip will be produced.
    ///
    /// It can require two types of [`VoteMessage`]s.
    /// In order to be able to produce certificates for reward purposes, signature aggregates for the two types are tracked separately.
    Skip {
        signature0: SignatureProjective,
        bitmap0: BitVec<u8, Lsb0>,
        sig_and_bitmap1: Option<(SignatureProjective, BitVec<u8, Lsb0>)>,
    },
    /// A [`CertificateMessage`] of type NotarFallback will be produced.
    ///
    /// It can require two types of [`VoteMessage`]s.
    /// This certificate is not used for rewards so its signature can be aggregated in a single container.
    NotarFallback {
        signature: SignatureProjective,
        bitmap0: BitVec<u8, Lsb0>,
        bitmap1: Option<BitVec<u8, Lsb0>>,
    },
}

impl BuilderType {
    /// Creates a new instance of [`BuilderType`].
    fn new(certificate: &Certificate) -> Self {
        match certificate {
            Certificate::Skip(_) => Self::Skip {
                signature0: SignatureProjective::identity(),
                bitmap0: default_bitvec(),
                sig_and_bitmap1: None,
            },
            Certificate::NotarizeFallback(_, _) => Self::NotarFallback {
                signature: SignatureProjective::identity(),
                bitmap0: default_bitvec(),
                bitmap1: None,
            },
            _ => Self::SingleVote {
                signature: SignatureProjective::identity(),
                bitmap: default_bitvec(),
            },
        }
    }

    /// Aggregates new [`VoteMessage`]s into the builder.
    fn aggregate(
        &mut self,
        certificate: &Certificate,
        msgs: &[VoteMessage],
    ) -> Result<(), AggregateError> {
        let vote_types = certificate_limits_and_vote_types(certificate).1;
        assert_eq!(vote_types.len(), 2);
        match self {
            Self::Skip {
                signature0,
                bitmap0,
                sig_and_bitmap1,
            } => {
                for msg in msgs {
                    let rank = msg.rank as usize;
                    if MAXIMUM_VALIDATORS <= rank {
                        return Err(AggregateError::ValidatorDoesNotExist(msg.rank));
                    }
                    let vote_type = VoteType::get_type(&msg.vote);
                    if vote_type == vote_types[0] {
                        bitmap0.set(rank, true);
                    } else {
                        assert_eq!(vote_type, vote_types[1]);
                        sig_and_bitmap1
                            .get_or_insert((SignatureProjective::identity(), default_bitvec()))
                            .1
                            .set(rank, true);
                    }
                }
                signature0.aggregate_with(msgs.iter().filter_map(|msg| {
                    let vote_type = VoteType::get_type(&msg.vote);
                    (vote_type == vote_types[0]).then_some(&msg.signature)
                }))?;
                sig_and_bitmap1
                    .as_mut()
                    .map(|(signature, _)| {
                        signature.aggregate_with(msgs.iter().filter_map(|msg| {
                            let vote_type = VoteType::get_type(&msg.vote);
                            (vote_type == vote_types[1]).then_some(&msg.signature)
                        }))
                    })
                    .unwrap_or(Ok(()))?;
                Ok(())
            }

            Self::NotarFallback {
                signature,
                bitmap0,
                bitmap1,
            } => {
                for msg in msgs {
                    let rank = msg.rank as usize;
                    if MAXIMUM_VALIDATORS <= rank {
                        return Err(AggregateError::ValidatorDoesNotExist(msg.rank));
                    }
                    let vote_type = VoteType::get_type(&msg.vote);
                    if vote_type == vote_types[0] {
                        bitmap0.set(rank, true);
                    } else {
                        assert_eq!(vote_type, vote_types[1]);
                        bitmap1.get_or_insert(default_bitvec()).set(rank, true);
                    }
                }
                Ok(signature.aggregate_with(msgs.iter().map(|m| &m.signature))?)
            }

            Self::SingleVote { signature, bitmap } => {
                for msg in msgs {
                    let rank = msg.rank as usize;
                    if MAXIMUM_VALIDATORS <= rank {
                        return Err(AggregateError::ValidatorDoesNotExist(msg.rank));
                    }
                    let vote_type = VoteType::get_type(&msg.vote);
                    assert_eq!(vote_type, vote_types[0]);
                    bitmap.set(rank, true);
                }
                Ok(signature.aggregate_with(msgs.iter().map(|m| &m.signature))?)
            }
        }
    }

    /// Builds a [`CertificateMessage`] from the builder.
    fn build(self, certificate: Certificate) -> Result<CertificateMessage, BuildError> {
        match self {
            Self::SingleVote { signature, bitmap } => {
                build_cert_from_bitmap(certificate, signature, bitmap).map_err(BuildError::Encode)
            }
            Self::Skip {
                mut signature0,
                bitmap0,
                sig_and_bitmap1,
            } => match sig_and_bitmap1 {
                None => build_cert_from_bitmap(certificate, signature0, bitmap0)
                    .map_err(BuildError::Encode),
                Some((signature1, bitmap1)) => {
                    signature0.aggregate_with([signature1].iter())?;
                    build_cert_from_bitmaps(certificate, signature0, bitmap0, bitmap1)
                }
            },
            Self::NotarFallback {
                signature,
                bitmap0,
                bitmap1,
            } => match bitmap1 {
                None => build_cert_from_bitmap(certificate, signature, bitmap0)
                    .map_err(BuildError::Encode),
                Some(bitmap1) => build_cert_from_bitmaps(certificate, signature, bitmap0, bitmap1),
            },
        }
    }

    /// Builds a [`CertificateMessage`] for rewards purposes from the builder.
    fn build_for_rewards(
        self,
        certificate: Certificate,
    ) -> Result<CertificateMessage, BuildForRewardsError> {
        match self {
            Self::Skip {
                signature0,
                bitmap0,
                sig_and_bitmap1: _,
            } => build_cert_from_bitmap(certificate, signature0, bitmap0)
                .map_err(BuildForRewardsError::Encode),
            Self::SingleVote { signature, bitmap } => match certificate {
                Certificate::Notarize(_, _) => {
                    build_cert_from_bitmap(certificate, signature, bitmap)
                        .map_err(BuildForRewardsError::Encode)
                }
                _ => Err(BuildForRewardsError::InvalidCertType),
            },
            Self::NotarFallback { .. } => Err(BuildForRewardsError::InvalidCertType),
        }
    }
}

/// Builder for creating [`CertificateMessage`] by using BLS signature aggregation.
pub(super) struct CertificateBuilder {
    builder_type: BuilderType,
    certificate: Certificate,
}

impl CertificateBuilder {
    /// Creates a new instance of the builder.
    pub(super) fn new(certificate: Certificate) -> Self {
        let builder_type = BuilderType::new(&certificate);
        Self {
            builder_type,
            certificate,
        }
    }

    /// Aggregates new [`VoteMessage`]s into the builder.
    pub(super) fn aggregate(&mut self, msgs: &[VoteMessage]) -> Result<(), AggregateError> {
        self.builder_type.aggregate(&self.certificate, msgs)
    }

    /// Builds a [`CertificateMessage`] from the builder.
    pub(super) fn build(self) -> Result<CertificateMessage, BuildError> {
        self.builder_type.build(self.certificate)
    }

    /// Builds a [`CertificateMessage`] for rewards purposes from the builder.
    #[allow(dead_code)]
    pub(super) fn build_for_rewards(self) -> Result<CertificateMessage, BuildForRewardsError> {
        self.builder_type.build_for_rewards(self.certificate)
    }
}
