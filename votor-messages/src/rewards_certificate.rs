//! Defines aggregates used for rewards.

use {
    solana_bls_signatures::Signature as BLSSignature,
    solana_clock::Slot,
    solana_hash::Hash,
    wincode::{containers::Pod, SchemaRead, SchemaWrite},
};

/// Reward certificate for the validators that voted skip.
///
/// Unlike the skip certificate which can be base-2 or base-3 encoded, this is guaranteed to be base-2 encoded.
#[derive(Clone, PartialEq, Eq, Debug, SchemaWrite, SchemaRead)]
pub struct SkipRewardCertificate {
    /// The slot the certificate is for.
    pub slot: Slot,
    /// The signature
    #[wincode(with = "Pod<BLSSignature>")]
    pub signature: BLSSignature,
    /// The bitmap for validators, see solana-signer-store for encoding format
    pub bitmap: Vec<u8>,
}

/// Reward certificate for the validators that voted notar.
#[derive(Clone, PartialEq, Eq, Debug, SchemaWrite, SchemaRead)]
pub struct NotarRewardCertificate {
    /// The slot the certificate is for.
    pub slot: Slot,
    /// The block id the certificate is for.
    #[wincode(with = "Pod<Hash>")]
    pub block_id: Hash,
    /// The signature
    #[wincode(with = "Pod<BLSSignature>")]
    pub signature: BLSSignature,
    /// The bitmap for validators, see solana-signer-store for encoding format
    pub bitmap: Vec<u8>,
}
