use {
    crate::{keypair::BlsPubkey, Bls},
    blstrs::G2Projective,
};

pub struct BlsProofOfPossession(pub G2Projective);
impl BlsProofOfPossession {
    /// Verify a proof of possession against a public key
    pub fn verify(&self, public_key: &BlsPubkey) -> bool {
        Bls::verify_proof_of_possession(public_key, self)
    }
}
