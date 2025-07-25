//! Put BLS message here so all clients can agree on the format
use serde::{Deserialize, Serialize};

use {
    crate::alpenglow::{certificate::Certificate, vote::Vote},
    bitvec::prelude::*,
    solana_bls_signatures::Signature as BLSSignature,
};

/// The seed used to derive the BLS keypair
pub const BLS_KEYPAIR_DERIVE_SEED: &[u8; 9] = b"alpenglow";

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
/// BLS vote message, we need rank to look up pubkey
pub struct VoteMessage {
    /// The vote
    pub vote: Vote,
    /// The signature
    pub signature: BLSSignature,
    /// The rank of the validator
    pub rank: u16,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// BLS vote message, we need rank to look up pubkey
pub struct CertificateMessage {
    /// The certificate
    pub certificate: Certificate,
    /// The signature
    pub signature: BLSSignature,
    /// The bitmap for validators, little endian byte order
    pub bitmap: BitVec<u8, Lsb0>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
/// BLS message data in Alpenglow
pub enum BLSMessage {
    /// Vote message, with the vote and the rank of the validator.
    Vote(VoteMessage),
    /// Certificate message
    Certificate(CertificateMessage),
}

impl BLSMessage {
    /// Create a new vote message
    pub fn new_vote(vote: Vote, signature: BLSSignature, rank: u16) -> Self {
        Self::Vote(VoteMessage {
            vote,
            signature,
            rank,
        })
    }

    /// Create a new certificate message
    pub fn new_certificate(
        certificate: Certificate,
        bitmap: BitVec<u8, Lsb0>,
        signature: BLSSignature,
    ) -> Self {
        Self::Certificate(CertificateMessage {
            certificate,
            signature,
            bitmap,
        })
    }

    /// Deserialize a BLS message from bytes
    pub fn deserialize(bls_message_in_bytes: &[u8]) -> Self {
        bincode::deserialize(bls_message_in_bytes).unwrap()
    }

    /// Serialize a BLS message to bytes
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}
