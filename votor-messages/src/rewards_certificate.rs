//! Defines aggregates used for rewards.

use {solana_bls_signatures::Signature as BLSSignature, solana_clock::Slot, solana_hash::Hash};

/// Reward certificate for the validators that voted skip.
///
/// Unlike the skip certificate which can be base-2 or base-3 encoded, this is guaranteed to be base-2 encoded.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SkipRewardCertificate {
    /// The slot the certificate is for.
    pub slot: Slot,
    /// The signature
    pub signature: BLSSignature,
    /// The bitmap for validators, see solana-signer-store for encoding format
    pub bitmap: Vec<u8>,
}

/// Reward certificate for the validators that voted notar.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NotarRewardCertificate {
    /// The slot the certificate is for.
    pub slot: Slot,
    /// The block id the certificate is for.
    pub block_id: Hash,
    /// The signature
    pub signature: BLSSignature,
    /// The bitmap for validators, see solana-signer-store for encoding format
    pub bitmap: Vec<u8>,
}

/// A reward certificate
pub enum RewardCertificate {
    /// The type is skip.
    Skip(SkipRewardCertificate),
    /// The type is notar.
    Notar(NotarRewardCertificate),
}
