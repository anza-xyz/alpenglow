use {
    crate::{error::BlsError, signature::BlsSignature, Bls},
    blst::{blst_keygen, blst_scalar},
    blstrs::{G1Projective, Scalar},
    ff::Field,
    group::Group,
    rand::{rngs::OsRng, CryptoRng, RngCore},
    std::ptr,
};
#[cfg(feature = "solana-signer-derive")]
use {solana_signature::Signature, solana_signer::Signer, subtle::ConstantTimeEq};

pub const BLS_SECRET_KEY_SIZE: usize = 32;
pub const BLS_PUBLIC_KEY_SIZE: usize = 48;

/// A BLS secret key
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BlsSecretKey(pub Scalar);

impl BlsSecretKey {
    /// Constructs a new, random `BlsSecretKey` using a caller-provided RNG
    pub fn generate<R>(csprng: &mut R) -> Self
    where
        R: CryptoRng + RngCore,
    {
        Self(Scalar::random(csprng))
    }

    /// Constructs a new, random `BlsSecretKey` using `OsRng`
    pub fn new() -> Self {
        let mut rng = OsRng;
        Self::generate(&mut rng)
    }

    /// Derive a `BlsSecretKey` from a seed (input key material)
    pub fn derive(ikm: &[u8]) -> Result<Self, BlsError> {
        let mut scalar = blst_scalar::default();
        unsafe {
            blst_keygen(
                &mut scalar as *mut blst_scalar,
                ikm.as_ptr(),
                ikm.len(),
                ptr::null(),
                0,
            );
        }
        scalar
            .try_into()
            .map(Self)
            .map_err(|_| BlsError::FieldDecode)
    }

    /// Derive a `BlsSecretKey` from a Solana signer
    #[cfg(feature = "solana-signer-derive")]
    pub fn derive_from_signer(signer: &dyn Signer, public_seed: &[u8]) -> Result<Self, BlsError> {
        let message = [b"bls-key-derive-", public_seed].concat();
        let signature = signer
            .try_sign_message(&message)
            .map_err(|_| BlsError::KeyDerivation)?;

        // Some `Signer` implementations return the default signature, which is not suitable for
        // use as key material
        if bool::from(signature.as_ref().ct_eq(Signature::default().as_ref())) {
            return Err(BlsError::KeyDerivation);
        }

        Self::derive(signature.as_ref())
    }

    /// Sign a message using the provided secret key
    pub fn sign(&self, message: &[u8]) -> BlsSignature {
        Bls::sign(self, message)
    }
}

/// A BLS public key
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BlsPubkey(pub G1Projective);

impl BlsPubkey {
    /// Construct a corresponding `BlsPubkey` for a `BlsSecretKey`
    #[allow(clippy::arithmetic_side_effects)]
    pub fn from_secret(secret: &BlsSecretKey) -> Self {
        Self(G1Projective::generator() * secret.0)
    }

    /// Verify a signature against a message and a public key
    pub fn verify(&self, signature: &BlsSignature, message: &[u8]) -> bool {
        Bls::verify(self, signature, message)
    }

    /// Aggregate a list of public keys into an existing aggregate
    #[allow(clippy::arithmetic_side_effects)]
    pub fn aggregate_with<'a, I>(&mut self, pubkeys: I)
    where
        I: IntoIterator<Item = &'a BlsPubkey>,
    {
        self.0 = pubkeys.into_iter().fold(self.0, |mut acc, pubkey| {
            acc += &pubkey.0;
            acc
        });
    }

    /// Aggregate a list of public keys
    #[allow(clippy::arithmetic_side_effects)]
    pub fn aggregate<'a, I>(pubkeys: I) -> Result<BlsPubkey, BlsError>
    where
        I: IntoIterator<Item = &'a BlsPubkey>,
    {
        let mut iter = pubkeys.into_iter();
        if let Some(acc) = iter.next() {
            let aggregate_point = iter.fold(acc.0, |mut acc, pubkey| {
                acc += &pubkey.0;
                acc
            });
            Ok(Self(aggregate_point))
        } else {
            Err(BlsError::EmptyAggregation)
        }
    }
}

/// A BLS keypair
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BlsKeypair {
    pub secret: BlsSecretKey,
    pub public: BlsPubkey,
}

impl BlsKeypair {
    /// Constructs a new, random `BlsKeypair` using a caller-provided RNG
    pub fn generate<R>(csprng: &mut R) -> Self
    where
        R: CryptoRng + RngCore,
    {
        let secret = BlsSecretKey::generate(csprng);
        let public = BlsPubkey::from_secret(&secret);
        Self { secret, public }
    }

    /// Constructs a new, random `BlsKeypair` using `OsRng`
    pub fn new() -> Self {
        let mut rng = OsRng;
        Self::generate(&mut rng)
    }

    /// Derive a `BlsKeypair` from a seed (input key material)
    pub fn derive(ikm: &[u8]) -> Result<Self, BlsError> {
        let secret = BlsSecretKey::derive(ikm)?;
        let public = BlsPubkey::from_secret(&secret);
        Ok(Self { secret, public })
    }

    /// Derive a `BlsSecretKey` from a Solana signer
    #[cfg(feature = "solana-signer-derive")]
    pub fn derive_from_signer(signer: &dyn Signer, public_seed: &[u8]) -> Result<Self, BlsError> {
        let secret = BlsSecretKey::derive_from_signer(signer, public_seed)?;
        let public = BlsPubkey::from_secret(&secret);
        Ok(Self { secret, public })
    }

    /// Sign a message using the provided secret key
    pub fn sign(&self, message: &[u8]) -> BlsSignature {
        Bls::sign(&self.secret, message)
    }

    /// Verify a signature against a message and a public key
    pub fn verify(&self, signature: &BlsSignature, message: &[u8]) -> bool {
        Bls::verify(&self.public, signature, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keygen_derive() {
        let ikm = b"test_ikm";
        let secret = BlsSecretKey::derive(ikm).unwrap();
        let public = BlsPubkey::from_secret(&secret);
        let keypair = BlsKeypair::derive(ikm).unwrap();
        assert_eq!(keypair.secret, secret);
        assert_eq!(keypair.public, public);
    }

    #[test]
    #[cfg(feature = "solana-signer-derive")]
    fn test_keygen_derive_from_signer() {
        let solana_keypair = solana_keypair::Keypair::new();
        let secret = BlsSecretKey::derive_from_signer(&solana_keypair, b"alpenglow-vote").unwrap();
        let public = BlsPubkey::from_secret(&secret);
        let keypair = BlsKeypair::derive_from_signer(&solana_keypair, b"alpenglow-vote").unwrap();

        assert_eq!(keypair.secret, secret);
        assert_eq!(keypair.public, public);
    }
}
