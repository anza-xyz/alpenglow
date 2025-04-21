use {
    crate::{error::BlsError, keypair::PubkeyProjective, Bls},
    blstrs::{G2Affine, G2Projective},
};

/// Size of a BLS signature in a compressed point representation
pub const BLS_SIGNATURE_COMPRESSED_SIZE: usize = 96;

/// Size of a BLS signature in an affine point representation
pub const BLS_SIGNATURE_AFFINE_SIZE: usize = 192;

/// A BLS signature in a projective point representation
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignatureProjective(pub(crate) G2Projective);

impl SignatureProjective {
    /// Verify a signature against a message and a public key
    pub fn verify(&self, pubkey: &PubkeyProjective, message: &[u8]) -> bool {
        Bls::verify(pubkey, self, message)
    }

    /// Aggregate a list of signatures into an existing aggregate
    #[allow(clippy::arithmetic_side_effects)]
    pub fn aggregate_with<'a, I>(&mut self, signatures: I)
    where
        I: IntoIterator<Item = &'a SignatureProjective>,
    {
        self.0 = signatures.into_iter().fold(self.0, |mut acc, signature| {
            acc += &signature.0;
            acc
        });
    }

    /// Aggregate a list of public keys
    #[allow(clippy::arithmetic_side_effects)]
    pub fn aggregate<'a, I>(signatures: I) -> Result<SignatureProjective, BlsError>
    where
        I: IntoIterator<Item = &'a SignatureProjective>,
    {
        let mut iter = signatures.into_iter();
        if let Some(acc) = iter.next() {
            let aggregate_point = iter.fold(acc.0, |mut acc, signature| {
                acc += &signature.0;
                acc
            });
            Ok(Self(aggregate_point))
        } else {
            Err(BlsError::EmptyAggregation)
        }
    }
}

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

impl From<SignatureProjective> for Signature {
    fn from(proof: SignatureProjective) -> Self {
        Self(proof.0.to_uncompressed())
    }
}

impl TryFrom<Signature> for SignatureProjective {
    type Error = BlsError;

    fn try_from(proof: Signature) -> Result<Self, Self::Error> {
        let maybe_uncompressed: Option<G2Affine> = G2Affine::from_uncompressed(&proof.0).into();
        let uncompressed = maybe_uncompressed.ok_or(BlsError::PointConversion)?;
        Ok(Self(uncompressed.into()))
    }
}

impl TryFrom<&Signature> for SignatureProjective {
    type Error = BlsError;

    fn try_from(proof: &Signature) -> Result<Self, Self::Error> {
        let maybe_uncompressed: Option<G2Affine> = G2Affine::from_uncompressed(&proof.0).into();
        let uncompressed = maybe_uncompressed.ok_or(BlsError::PointConversion)?;
        Ok(Self(uncompressed.into()))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::keypair::Keypair};

    #[test]
    fn test_signature_aggregate() {
        let test_message = b"test message";
        let keypair0 = Keypair::new();
        let signature0 = keypair0.sign(test_message);

        let test_message = b"test message";
        let keypair1 = Keypair::new();
        let signature1 = keypair1.sign(test_message);

        let aggregate_signature =
            SignatureProjective::aggregate([&signature0, &signature1]).unwrap();

        let mut aggregate_signature_with = signature0;
        aggregate_signature_with.aggregate_with([&signature1]);

        assert_eq!(aggregate_signature, aggregate_signature_with);
    }
}
