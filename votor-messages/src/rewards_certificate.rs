//! Defines aggregates used for vote rewards.

#[cfg(feature = "dev-context-only-utils")]
use solana_bls_signatures::keypair::Keypair as BLSKeypair;
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

impl SkipRewardCertificate {
    /// Creates a new [`SkipRewardCertificate`] for test purposes.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests() -> Self {
        let bls_keypair = BLSKeypair::new();
        let signature = bls_keypair.sign(b"hello").into();
        Self {
            slot: 1234,
            signature,
            bitmap: vec![],
        }
    }
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

impl NotarRewardCertificate {
    /// Creates a new [`NotarRewardCertificate`] for test purposes.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests() -> Self {
        let bls_keypair = BLSKeypair::new();
        let signature = bls_keypair.sign(b"hello").into();
        Self {
            slot: 1234,
            block_id: Hash::new_unique(),
            signature,
            bitmap: vec![],
        }
    }
}
