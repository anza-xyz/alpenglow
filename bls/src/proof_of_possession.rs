use {
    crate::{error::BlsError, keypair::PubkeyProjective, Bls},
    blstrs::{G2Affine, G2Projective},
};

/// Size of a BLS proof of possession in a compressed point representation
pub const BLS_PROOF_OF_POSSESSION_COMPRESSED_SIZE: usize = 96;

/// Size of a BLS proof of possession in an affine point representation
pub const BLS_PROOF_OF_POSSESSION_AFFINE_SIZE: usize = 192;

/// A BLS proof of possession in a projective point representation
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProofOfPossessionProjective(pub(crate) G2Projective);
impl ProofOfPossessionProjective {
    /// Verify a proof of possession against a public key
    pub fn verify(&self, public_key: &PubkeyProjective) -> bool {
        Bls::verify_proof_of_possession(public_key, self)
    }
}

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

impl From<ProofOfPossessionProjective> for ProofOfPossession {
    fn from(proof: ProofOfPossessionProjective) -> Self {
        Self(proof.0.to_uncompressed())
    }
}

impl TryFrom<ProofOfPossession> for ProofOfPossessionProjective {
    type Error = BlsError;

    fn try_from(proof: ProofOfPossession) -> Result<Self, Self::Error> {
        let maybe_uncompressed: Option<G2Affine> = G2Affine::from_uncompressed(&proof.0).into();
        let uncompressed = maybe_uncompressed.ok_or(BlsError::PointConversion)?;
        Ok(Self(uncompressed.into()))
    }
}

impl TryFrom<&ProofOfPossession> for ProofOfPossessionProjective {
    type Error = BlsError;

    fn try_from(proof: &ProofOfPossession) -> Result<Self, Self::Error> {
        let maybe_uncompressed: Option<G2Affine> = G2Affine::from_uncompressed(&proof.0).into();
        let uncompressed = maybe_uncompressed.ok_or(BlsError::PointConversion)?;
        Ok(Self(uncompressed.into()))
    }
}
