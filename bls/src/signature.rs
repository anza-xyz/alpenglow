use {
    crate::{error::BlsError, keypair::BlsPubkey, Bls},
    blstrs::G2Projective,
};

pub const BLS_SIGNATURE_SIZE: usize = 96;

/// A BLS signature
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BlsSignature(pub G2Projective);

impl BlsSignature {
    /// Verify a signature against a message and a public key
    pub fn verify(&self, pubkey: &BlsPubkey, message: &[u8]) -> bool {
        Bls::verify(pubkey, self, message)
    }

    /// Aggregate a list of signatures into an existing aggregate
    #[allow(clippy::arithmetic_side_effects)]
    pub fn aggregate_with<'a, I>(&mut self, signatures: I)
    where
        I: IntoIterator<Item = &'a BlsSignature>,
    {
        self.0 = signatures.into_iter().fold(self.0, |mut acc, signature| {
            acc += &signature.0;
            acc
        });
    }

    /// Aggregate a list of public keys
    #[allow(clippy::arithmetic_side_effects)]
    pub fn aggregate<'a, I>(signatures: I) -> Result<BlsSignature, BlsError>
    where
        I: IntoIterator<Item = &'a BlsSignature>,
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

#[cfg(test)]
mod tests {
    use {super::*, crate::keypair::BlsKeypair};

    #[test]
    fn test_signature_aggregate() {
        let test_message = b"test message";
        let keypair0 = BlsKeypair::new();
        let signature0 = keypair0.sign(test_message);

        let test_message = b"test message";
        let keypair1 = BlsKeypair::new();
        let signature1 = keypair1.sign(test_message);

        let aggregate_signature = BlsSignature::aggregate([&signature0, &signature1]).unwrap();

        let mut aggregate_signature_with = signature0;
        aggregate_signature_with.aggregate_with([&signature1]);

        assert_eq!(aggregate_signature, aggregate_signature_with);
    }
}
