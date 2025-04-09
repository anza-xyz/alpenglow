use {
    crate::{
        error::BlsError,
        keypair::{BlsPubkey, BlsSecretKey},
        signature::BlsSignature,
    },
    blstrs::{pairing, G1Affine, G2Projective},
    group::prime::PrimeCurveAffine,
};

pub mod error;
pub mod keypair;
pub mod signature;

/// Domain separation tag used for hashing messages to curve points to prevent
/// potential conflicts between different BLS implementations. This is defined
/// as the ciphersuite ID string as recommended in the standard
/// https://datatracker.ietf.org/doc/html/draft-irtf-cfrg-bls-signature-05#section-4.2.1.
pub const HASH_TO_POINT_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

pub struct Bls;
impl Bls {
    /// Sign a message using the provided secret key
    #[allow(clippy::arithmetic_side_effects)]
    pub(crate) fn sign(secret_key: &BlsSecretKey, message: &[u8]) -> BlsSignature {
        let hashed_message = Bls::hash_message_to_point(message);
        BlsSignature(hashed_message * secret_key.0)
    }

    /// Verify a signature against a message and a public key
    ///
    /// TODO: Verify by invoking pairing just once
    pub(crate) fn verify(public_key: &BlsPubkey, signature: &BlsSignature, message: &[u8]) -> bool {
        let hashed_message = Bls::hash_message_to_point(message);
        pairing(&public_key.0.into(), &hashed_message.into())
            == pairing(&G1Affine::generator(), &signature.0.into())
    }

    /// Verify a list of signatures against a message and a list of public keys
    pub fn aggregate_verify<'a, I, J>(
        public_keys: I,
        signatures: J,
        message: &[u8],
    ) -> Result<bool, BlsError>
    where
        I: IntoIterator<Item = &'a BlsPubkey>,
        J: IntoIterator<Item = &'a BlsSignature>,
    {
        let aggregate_pubkey = BlsPubkey::aggregate(public_keys)?;
        let aggregate_signature = BlsSignature::aggregate(signatures)?;

        Ok(Self::verify(
            &aggregate_pubkey,
            &aggregate_signature,
            message,
        ))
    }

    /// Hash a message to a G2 point
    pub fn hash_message_to_point(message: &[u8]) -> G2Projective {
        G2Projective::hash_to_curve(message, HASH_TO_POINT_DST, &[])
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::keypair::BlsKeypair};

    #[test]
    fn test_verify() {
        let keypair = BlsKeypair::new();
        let test_message = b"test message";
        let signature = Bls::sign(&keypair.secret, test_message);
        assert!(Bls::verify(&keypair.public, &signature, test_message));
    }

    #[test]
    fn test_aggregate_verify() {
        let test_message = b"test message";

        let keypair0 = BlsKeypair::new();
        let signature0 = Bls::sign(&keypair0.secret, test_message);
        assert!(Bls::verify(&keypair0.public, &signature0, test_message));

        let keypair1 = BlsKeypair::new();
        let signature1 = Bls::sign(&keypair1.secret, test_message);
        assert!(Bls::verify(&keypair1.public, &signature1, test_message));

        // basic case
        assert!(Bls::aggregate_verify(
            vec![&keypair0.public, &keypair1.public],
            vec![&signature0, &signature1],
            test_message,
        )
        .unwrap());

        // pre-aggregate the signatures
        let aggregate_signature = BlsSignature::aggregate([&signature0, &signature1]).unwrap();
        assert!(Bls::aggregate_verify(
            vec![&keypair0.public, &keypair1.public],
            vec![&aggregate_signature],
            test_message,
        )
        .unwrap());

        // pre-aggregate the public keys
        let aggregate_pubkey = BlsPubkey::aggregate([&keypair0.public, &keypair1.public]).unwrap();
        assert!(Bls::aggregate_verify(
            vec![&aggregate_pubkey],
            vec![&signature0, &signature1],
            test_message,
        )
        .unwrap());

        // empty set of public keys or signatures
        let err = Bls::aggregate_verify(vec![], vec![&signature0, &signature1], test_message)
            .unwrap_err();
        assert_eq!(err, BlsError::EmptyAggregation);

        let err = Bls::aggregate_verify(
            vec![&keypair0.public, &keypair1.public],
            vec![],
            test_message,
        )
        .unwrap_err();
        assert_eq!(err, BlsError::EmptyAggregation);
    }
}
