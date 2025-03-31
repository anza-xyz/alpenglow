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

    fn can_accept_new_skip_range(
        old_skip_range: Option<(Slot, Slot)>,
        new_skip_range: Option<(Slot, Slot)>,
    ) -> bool {
        info!(
            "Can accept new skip range: old_skip_range: {:?}, new_skip_range: {:?}",
            old_skip_range, new_skip_range
        );
        if let Some((old_start, old_end)) = old_skip_range {
            if let Some((new_start, new_end)) = new_skip_range {
                // We never allow users to un-vote, so it's okay if they extend the end
                // of skip range, but we reject any new range with a smaller end.
                // We do allow new_start to be larger than old_start. There is a choice here:
                // 1. Ask that new_start to be always equal to old_start, then the clients must
                //    remember the skip range of the last Skip vote, otherwise if someone voted
                //    for (2, 5), then 3 got finalized, they can't discard anything smaller than
                //    3. If they want to skip 6, they must send (2, 6), otherwise it will not land.
                // 2. Allow new_start to be larger than old_start, then the clients can just discard
                //    anything older than their local root. This is the option we chose. Note that
                //    we use per-slot cert now, so if someone voted for (2, 5) using transaction t1,
                //    then its local root becomes 3 and it now sends (4, 7) using transaction t2,
                //    the slots 4 to 7 will save t2 in skip cert, while 2 to 3 will save t1 in skip
                //    cert. Therefore, even if we allow new_start to be larger, we don't allow users
                //    to un-vote.
                //    However, in the per-slot cert world, we can forbid new_start to be smaller
                //    than old_start. Someone may have sent (2, 5) and (3, 7), these two may
                //    arrive out of order. (2, 5) will not be able to land in cert of slot 3 to 5,
                //    but it can still land in cert of slot 2.
                new_start >= old_start && new_end > old_end
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

    // Return an iterator of CertificateMap
    pub(crate) fn get_certificate_iter(
        &self,
    ) -> std::collections::hash_map::Iter<'_, Pubkey, VoteCertificateEntry> {
        self.certificate.iter()
    }
}
