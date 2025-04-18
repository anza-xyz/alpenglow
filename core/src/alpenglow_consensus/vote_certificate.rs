use {
    super::{Stake, SUPERMAJORITY},
    solana_pubkey::Pubkey,
    solana_sdk::{clock::Slot, transaction::VersionedTransaction},
    std::{collections::HashMap, sync::Arc},
};

pub(crate) type CertificateMap = HashMap<Pubkey, Arc<VersionedTransaction>>;

//TODO(wen): split certificate according to different blockid and bankhash
pub struct VoteCertificate {
    // Must be either all notarization or finalization votes.
    // We keep separate certificates for each type
    certificate: CertificateMap,
    // Total stake of all the slots in the certificate
    stake: Stake,
    // The slot the votes in the certificate are for
    slot: Slot,
    is_complete: bool,
}

impl VoteCertificate {
    pub fn new(slot: Slot) -> Self {
        Self {
            certificate: HashMap::new(),
            stake: 0,
            slot,
            is_complete: false,
        }
    }

    pub fn add_vote(
        &mut self,
        validator_key: &Pubkey,
        transaction: Arc<VersionedTransaction>,
        validator_stake: Stake,
        total_stake: Stake,
    ) -> bool {
        // Caller needs to verify that this is the same type (Notarization, Skip) as all the other votes in the current certificate
        if self.certificate.contains_key(validator_key) {
            // Make duplicate vote fail silently, we may get votes from different resources and votes may arrive out of order.
            // This also needs to silently fail because the new skip vote might conflict with some old votes in old slots,
            // but perfectly fine for some other slots. E.g. old vote is (23, 23), (22, 24) will fail for slot 23, but it's
            // fine for slot 22 and 24.
            return false;
        }
        // TODO: verification that this vote can land
        self.certificate.insert(*validator_key, transaction);
        self.stake += validator_stake;
        self.is_complete = self.check_complete(total_stake);

        true
    }

    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    pub fn check_complete(&mut self, total_stake: Stake) -> bool {
        (self.stake as f64 / total_stake as f64) > SUPERMAJORITY
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn size(&self) -> usize {
        self.certificate.len()
    }

    // Return an iterator of CertificateMap, only return Some if the certificate is complete
    pub(crate) fn get_certificate_iter_for_complete_cert(
        &self,
    ) -> Option<std::collections::hash_map::Iter<'_, Pubkey, Arc<VersionedTransaction>>> {
        if self.is_complete {
            Some(self.certificate.iter())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use {super::*, solana_sdk::transaction::VersionedTransaction, std::sync::Arc};

    #[test]
    fn test_vote_certificate() {
        let mut vote_cert = VoteCertificate::new(1);
        let transaction = Arc::new(VersionedTransaction::default());
        let total_stake = 100;

        assert!(vote_cert.add_vote(&Pubkey::new_unique(), transaction.clone(), 10, total_stake),);
        assert_eq!(vote_cert.stake, 10);
        assert!(!vote_cert.is_complete());

        assert!(vote_cert.add_vote(&Pubkey::new_unique(), transaction.clone(), 60, total_stake),);
        assert_eq!(vote_cert.stake, 70);
        assert!(vote_cert.is_complete());
    }
}
