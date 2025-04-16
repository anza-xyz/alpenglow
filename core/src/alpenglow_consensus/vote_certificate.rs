use {
    super::{Stake, SUPERMAJORITY},
    solana_pubkey::Pubkey,
    solana_sdk::{
        clock::Slot,
        transaction::{TransactionError, VersionedTransaction},
    },
    std::{collections::HashMap, sync::Arc},
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    #[error("Transaction failed: {0}")]
    TransactionFailed(#[from] TransactionError),
}

pub(crate) struct VoteCertificateEntry {
    // The transaction that was voted on
    transaction: Arc<VersionedTransaction>,
    // The skip range for duplicate check
    skip_range: Option<(Slot, Slot)>,
}

pub(crate) type CertificateMap = HashMap<Pubkey, VoteCertificateEntry>;

impl VoteCertificateEntry {
    pub fn transaction(&self) -> Arc<VersionedTransaction> {
        self.transaction.clone()
    }

    pub fn skip_range(&self) -> Option<(Slot, Slot)> {
        self.skip_range
    }
}

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

    pub(crate) fn can_accept_new_skip_range(
        old_skip_range: Option<(Slot, Slot)>,
        new_skip_range: Option<(Slot, Slot)>,
    ) -> bool {
        info!(
            "Can accept new skip range: old_skip_range: {:?}, new_skip_range: {:?}",
            old_skip_range, new_skip_range
        );
        if let Some((old_start, old_end)) = old_skip_range {
            if let Some((new_start, new_end)) = new_skip_range {
                // Because we now use per-slot skip certificate pool, users are never allowed
                // to un-vote. If user sent (2, 5) then (3, 6), the skip cert pool for slot 2
                // will still have (2, 5) saved. The skip cert for slot 3 to 5 will replace
                // (2, 5) with (3, 6), but the user's skip is still recorded.
                // But skip votes can arrive out of order (all-to-all vs Gossip), so we need
                // a deterministic order to decide whether to replace the old skip range.
                // Note that this doesn't affect correctness, it's just optimization to reduce
                // unnecessary skip vote replacement in cert pool.
                // Because normal validators almost always extend the end, let's compare end
                // first then compare start and leave the one with the wider range so we can
                // hopefully have fewer transactions in the cert:
                // 1.new_end > old_end: (4, 8) > (3, 5), (2, 8) > (3, 5), (3, 8) > (3, 5)
                // 2.new_end == old_end, check if new_start > old_start, because hopefully
                //   larger new_start means this transaction is more recent than the old one.
                //   Reject if new_start <= old_start.  (3, 8) > (2, 8).
                // 3.new_end < old_end: reject new one
                new_end > old_end || (new_end == old_end && new_start > old_start)
            } else {
                // We should always replace skip with a skip, so should never happen that new
                // skip range is None when old one is Some.
                panic!("New skip range should never be None");
            }
        } else {
            // If both are None, this is notarization or finalization, do not accept
            // We should always replace skip with a skip, so should never happen that old is
            // None and new is Some
            assert!(new_skip_range.is_none());
            false
        }
    }

    pub fn add_vote(
        &mut self,
        validator_key: &Pubkey,
        transaction: Arc<VersionedTransaction>,
        skip_range: Option<(Slot, Slot)>,
        validator_stake: Stake,
        total_stake: Stake,
    ) -> Result<(), AddVoteError> {
        // Caller needs to verify that this is the same type (Notarization, Skip) as all the other votes in the current certificate
        if self.certificate.contains_key(validator_key)
            && !Self::can_accept_new_skip_range(
                self.certificate
                    .get(validator_key)
                    .and_then(|entry| entry.skip_range),
                skip_range,
            )
        {
            // Make duplicate vote fail silently, we may get votes from different resources and votes may arrive out of order.
            // This also needs to silently fail because the new skip vote might conflict with some old votes in old slots,
            // but perfectly fine for some other slots. E.g. old vote is (23, 23), (22, 24) will fail for slot 23, but it's
            // fine for slot 22 and 24.
            return Ok(());
        }
        // TODO: verification that this vote can land
        self.certificate.insert(
            *validator_key,
            VoteCertificateEntry {
                transaction,
                skip_range,
            },
        );
        self.stake += validator_stake;
        self.is_complete = self.check_complete(total_stake);

        Ok(())
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
    ) -> Option<std::collections::hash_map::Iter<'_, Pubkey, VoteCertificateEntry>> {
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

        assert_eq!(
            vote_cert.add_vote(
                &Pubkey::new_unique(),
                transaction.clone(),
                None,
                10,
                total_stake
            ),
            Ok(())
        );
        assert_eq!(vote_cert.stake, 10);
        assert!(!vote_cert.is_complete());

        assert_eq!(
            vote_cert.add_vote(
                &Pubkey::new_unique(),
                transaction.clone(),
                None,
                60,
                total_stake
            ),
            Ok(())
        );
        assert_eq!(vote_cert.stake, 70);
        assert!(vote_cert.is_complete());
    }

    #[test]
    fn test_can_accept_new_skip_range() {
        let old_skip_range = Some((1, 5));
        let new_skip_range = Some((2, 6));
        assert!(VoteCertificate::can_accept_new_skip_range(
            old_skip_range,
            new_skip_range
        ));
        assert!(!VoteCertificate::can_accept_new_skip_range(
            new_skip_range,
            old_skip_range
        ));

        let old_skip_range = Some((2, 5));
        let new_skip_range = Some((1, 5));
        assert!(!VoteCertificate::can_accept_new_skip_range(
            old_skip_range,
            new_skip_range
        ));
        assert!(VoteCertificate::can_accept_new_skip_range(
            new_skip_range,
            old_skip_range
        ));

        let old_skip_range = Some((1, 5));
        let new_skip_range = Some((1, 5));
        assert!(!VoteCertificate::can_accept_new_skip_range(
            old_skip_range,
            new_skip_range
        ));
    }
}
