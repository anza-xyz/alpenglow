/// Size of a BLS signature in a compressed point representation
pub const BLS_SIGNATURE_COMPRESSED_SIZE: usize = 96;

/// Size of a BLS signature in an affine point representation
pub const BLS_SIGNATURE_AFFINE_SIZE: usize = 192;

/// A serialized BLS signature in a compressed point representation
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignatureCompressed(pub [u8; BLS_SIGNATURE_COMPRESSED_SIZE]);

impl Default for SignatureCompressed {
    fn default() -> Self {
        Self([0; BLS_SIGNATURE_COMPRESSED_SIZE])
    }
}

/// A serialized BLS signature in an affine point representation
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Signature(pub [u8; BLS_SIGNATURE_AFFINE_SIZE]);

impl Default for Signature {
    fn default() -> Self {
        Self([0; BLS_SIGNATURE_AFFINE_SIZE])
    }
}

/// Size of a BLS proof of possession in a compressed point representation
pub const BLS_PROOF_OF_POSSESSION_COMPRESSED_SIZE: usize = 96;

/// Size of a BLS proof of possession in an affine point representation
pub const BLS_PROOF_OF_POSSESSION_AFFINE_SIZE: usize = 192;

/// A serialized BLS signature in a compressed point representation
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProofOfPossessionCompressed(pub [u8; BLS_PROOF_OF_POSSESSION_COMPRESSED_SIZE]);

impl Default for ProofOfPossessionCompressed {
    fn default() -> Self {
        Self([0; BLS_PROOF_OF_POSSESSION_COMPRESSED_SIZE])
    }
}

/// A serialized BLS signature in an affine point representation
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProofOfPossession(pub [u8; BLS_PROOF_OF_POSSESSION_AFFINE_SIZE]);

impl Default for ProofOfPossession {
    fn default() -> Self {
        Self([0; BLS_PROOF_OF_POSSESSION_AFFINE_SIZE])
    }
}

/// Size of a BLS public key in a compressed point representation
pub const BLS_PUBLIC_KEY_COMPRESSED_SIZE: usize = 48;

/// Size of a BLS public key in an affine point representation
pub const BLS_PUBLIC_KEY_AFFINE_SIZE: usize = 96;

/// A serialized BLS public key in a compressed point representation
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PubkeyCompressed(pub [u8; BLS_PUBLIC_KEY_COMPRESSED_SIZE]);

impl Default for PubkeyCompressed {
    fn default() -> Self {
        Self([0; BLS_PUBLIC_KEY_COMPRESSED_SIZE])
    }
}

/// A serialized BLS public key in an affine point representation
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct Pubkey(pub [u8; BLS_PUBLIC_KEY_AFFINE_SIZE]);

impl Default for Pubkey {
    fn default() -> Self {
        Self([0; BLS_PUBLIC_KEY_AFFINE_SIZE])
    }
}
