use {
    super::{
        certificate_limits_and_vote_types,
        vote_certificate::{CertificateError, VoteCertificate},
        vote_history::VoteHistory,
        vote_pool::{VoteKey, VotePool},
        vote_to_certificate_ids, Stake,
    },
    crate::alpenglow_consensus::{
        CertificateId, VoteType, MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE,
        MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES, MAX_SLOT_AGE, SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP,
        SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP, SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY,
        SAFE_TO_SKIP_THRESHOLD,
    },
    alpenglow_vote::vote::Vote,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, epoch_stakes::EpochStakes},
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
        hash::Hash,
    },
    std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    },
    thiserror::Error,
};

impl VoteType {
    pub fn get_type(vote: &Vote) -> VoteType {
        match vote {
            Vote::Notarize(_) => VoteType::Notarize,
            Vote::NotarizeFallback(_) => VoteType::NotarizeFallback,
            Vote::Skip(_) => VoteType::Skip,
            Vote::SkipFallback(_) => VoteType::SkipFallback,
            Vote::Finalize(_) => VoteType::Finalize,
        }
    }
}

pub type PoolId = (Slot, VoteType);

#[derive(Debug, Error, PartialEq)]
pub enum AddVoteError {
    //    #[error("Conflicting vote type: {0:?} vs existing {1:?} for slot: {2} pubkey: {3}")]
    //    ConflictingVoteType(VoteType, VoteType, Slot, Pubkey),
    #[error("Epoch stakes missing for epoch: {0}")]
    EpochStakesNotFound(Epoch),

    #[error("Zero stake")]
    ZeroStake,

    #[error("Unrooted slot")]
    UnrootedSlot,

    #[error("Slot in the future")]
    SlotInFuture,

    #[error("Certificate error: {0}")]
    Certificate(#[from] CertificateError),
}

#[derive(Default)]
pub struct CertificatePool<VC: VoteCertificate> {
    // Vote pools to do bean counting for votes.
    vote_pools: BTreeMap<PoolId, VotePool<VC>>,
    /// Completed certificates
    completed_certificates: BTreeMap<CertificateId, VC>,
    // Lookup table for checking conflicting vote types.
    // conflicting_vote_types: HashMap<VoteType, Vec<VoteType>>,
    /// Highest block that has a NotarizeFallback certificate, for use in producing our leader window
    highest_notarized_fallback: Option<(Slot, Hash, Hash)>,
    /// Highest slot that has a Finalized variant certificate, for use in notifying RPC
    highest_finalized_slot: Option<Slot>,
    // Cached epoch_schedule
    epoch_schedule: EpochSchedule,
    // Cached epoch_stakes_map
    epoch_stakes_map: Arc<HashMap<Epoch, EpochStakes>>,
    // The current root, no need to save anything before this slot.
    root: Slot,
    // The epoch of current root.
    root_epoch: Epoch,
}

impl<VC: VoteCertificate> CertificatePool<VC> {
    pub fn new_from_root_bank(bank: &Bank) -> Self {
        let mut pool = Self::default();

        /*    // Initialize the conflicting_vote_types map
        for (vote_type_1, vote_type_2) in CONFLICTING_VOTETYPES.iter() {
            pool.conflicting_vote_types
                .entry(*vote_type_1)
                .or_insert_with(Vec::new)
                .push(*vote_type_2);
            pool.conflicting_vote_types
                .entry(*vote_type_2)
                .or_insert_with(Vec::new)
                .push(*vote_type_1);
        }*/

        // Update the epoch_stakes_map and root
        pool.update_epoch_stakes_map(bank);
        pool.root = bank.slot();

        pool
    }

    pub fn root(&self) -> Slot {
        self.root
    }

    fn update_epoch_stakes_map(&mut self, bank: &Bank) {
        let epoch = bank.epoch();
        if self.epoch_stakes_map.is_empty() || epoch > self.root_epoch {
            self.epoch_stakes_map = Arc::new(bank.epoch_stakes_map().clone());
            self.root_epoch = epoch;
            self.epoch_schedule = bank.epoch_schedule().clone();
        }
    }

    fn new_vote_pool(vote_type: VoteType) -> VotePool<VC> {
        match vote_type {
            VoteType::NotarizeFallback => VotePool::new(MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE),
            _ => VotePool::new(MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES),
        }
    }

    fn update_vote_pool(
        &mut self,
        slot: Slot,
        vote_type: VoteType,
        bank_hash: Option<Hash>,
        block_id: Option<Hash>,
        transaction: Arc<VC::VoteTransaction>,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
    ) -> bool {
        let pool = self
            .vote_pools
            .entry((slot, vote_type))
            .or_insert_with(|| Self::new_vote_pool(vote_type));
        pool.add_vote(
            validator_vote_key,
            bank_hash,
            block_id,
            transaction,
            validator_stake,
        )
    }

    /// For a new vote `slot` , `vote_type` checks if any
    /// of the related certificates are newly complete.
    /// For each newly constructed certificate
    /// - Insert it into `self.certificates`
    /// - Potentially update `self.highest_notarized_fallback`,
    /// - Potentially update `self.highest_finalized_slot`,
    /// - If we have a new highest finalized slot, return it
    fn update_certificates(
        &mut self,
        vote: &Vote,
        block_id: Option<Hash>,
        bank_hash: Option<Hash>,
        total_stake: Stake,
    ) -> Option<Slot> {
        let slot = vote.slot();
        vote_to_certificate_ids(vote)
            .iter()
            .filter_map(|&cert_id| {
                // If the certificate is already complete, skip it
                if self.completed_certificates.contains_key(&cert_id) {
                    return None;
                }
                // Otherwise check whether the certificate is complete
                let (limit, vote_types) = certificate_limits_and_vote_types(cert_id);
                let accumulated_stake = vote_types
                    .iter()
                    .filter_map(|vote_type| {
                        Some(
                            self.vote_pools
                                .get(&(slot, *vote_type))?
                                .total_stake_by_key(bank_hash, block_id),
                        )
                    })
                    .sum::<Stake>();
                if accumulated_stake as f64 / (total_stake as f64) < limit {
                    return None;
                }
                let mut transactions = Vec::new();
                for vote_type in vote_types {
                    let Some(vote_pool) = self.vote_pools.get(&(slot, *vote_type)) else {
                        continue;
                    };
                    vote_pool.copy_out_transactions(bank_hash, block_id, &mut transactions);
                }
                // TODO: use an empty hash map of pubkeys for now since it is not clear
                // where to get the sorted list of validators yet

                // TODO: remove unwrap and properly handle unwrap
                self.completed_certificates.insert(
                    cert_id,
                    VC::new(accumulated_stake, transactions, None).unwrap(),
                );
                // TODO(ashwin): Send to blockstore so that repair can serve. Also broadcast to other nodes

                if cert_id.is_notarize_fallback()
                    && self
                        .highest_notarized_fallback
                        .map_or(true, |(s, _, _)| s < slot)
                {
                    self.highest_notarized_fallback =
                        Some((slot, block_id.unwrap(), bank_hash.unwrap()));
                }

                if cert_id.is_finalization_variant()
                    && self.highest_finalized_slot.map_or(true, |s| s < slot)
                {
                    self.highest_finalized_slot = Some(slot);
                    Some(slot)
                } else {
                    None
                }
            })
            .max()
    }

    //TODO(wen): without cert retransmit this kills our local cluster test, enable later.
    /*    fn has_conflicting_vote(
            &self,
            slot: Slot,
            vote_type: VoteType,
            validator_vote_key: &Pubkey,
        ) -> Option<VoteType> {
            let conflicting_types = self.conflicting_vote_types.get(&vote_type)?;
            for conflicting_type in conflicting_types {
                if let Some(pool) = self.vote_pools.get(&(slot, *conflicting_type)) {
                    if pool.has_prev_vote(validator_vote_key) {
                        return Some(*conflicting_type);
                    }
                }
            }
            None
        }
    */

    /// Adds the new vote the the certificate pool.
    ///
    /// If this resulted in a new highest Finalize or FastFinalize certificate,
    /// return the slot
    pub fn add_vote(
        &mut self,
        vote: &Vote,
        transaction: VC::VoteTransaction,
        validator_vote_key: &Pubkey,
    ) -> Result<Option<Slot>, AddVoteError> {
        let slot = vote.slot();
        let transaction = Arc::new(transaction);
        let epoch = self.epoch_schedule.get_epoch(slot);
        let Some(epoch_stakes) = self.epoch_stakes_map.get(&epoch) else {
            return Err(AddVoteError::EpochStakesNotFound(epoch));
        };
        let validator_stake = epoch_stakes.vote_account_stake(validator_vote_key);
        let total_stake = epoch_stakes.total_stake();

        if validator_stake == 0 {
            return Err(AddVoteError::ZeroStake);
        }

        if slot < self.root {
            return Err(AddVoteError::UnrootedSlot);
        }
        // We only allow votes
        if slot > self.root + MAX_SLOT_AGE {
            return Err(AddVoteError::SlotInFuture);
        }
        let vote_type = VoteType::get_type(vote);
        let (bank_hash, block_id) = match vote {
            Vote::Notarize(vote) => (Some(*vote.replayed_bank_hash()), Some(*vote.block_id())),
            Vote::NotarizeFallback(vote) => {
                (Some(*vote.replayed_bank_hash()), Some(*vote.block_id()))
            }
            _ => (None, None),
        };
        //TODO(wen): without cert retransmit this kills our local cluster test, enable later.
        /*        if let Some(conflicting_type) =
                    self.has_conflicting_vote(slot, vote_type, validator_vote_key)
                {
                    return Err(AddVoteError::ConflictingVoteType(
                        vote_type,
                        conflicting_type,
                        slot,
                        *validator_vote_key,
                    ));
                }
        */
        if !self.update_vote_pool(
            slot,
            vote_type,
            bank_hash,
            block_id,
            transaction.clone(),
            validator_vote_key,
            validator_stake,
        ) {
            return Ok(None);
        }
        Ok(self.update_certificates(vote, block_id, bank_hash, total_stake))
    }

    /// The highest notarized fallback slot, for use as the parent slot in leader window
    pub fn highest_notarized_fallback(&self) -> Option<(Slot, Hash, Hash)> {
        // TODO(ashwin): When updating voting loop for duplicate blocks, add parent tracker to
        // make this the true "highest branchCertified block". For now this sufficies.
        self.highest_notarized_fallback
    }

    #[cfg(test)]
    fn highest_notarized_slot(&self) -> Slot {
        // Return the max of CertificateType::Notarize and CertificateType::NotarizeFallback
        self.completed_certificates
            .iter()
            .filter_map(|(cert_id, _)| match cert_id {
                CertificateId::Notarize(s, _, _) => Some(s),
                CertificateId::NotarizeFallback(s, _, _) => Some(s),
                _ => None,
            })
            .max()
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn highest_skip_slot(&self) -> Slot {
        self.completed_certificates
            .iter()
            .filter_map(|(cert_id, _)| match cert_id {
                CertificateId::Skip(s) => Some(s),
                _ => None,
            })
            .max()
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn highest_finalized_slot(&self) -> Slot {
        self.completed_certificates
            .iter()
            .filter_map(|(cert_id, _)| match cert_id {
                CertificateId::Finalize(s) => Some(s),
                CertificateId::FinalizeFast(s, _, _) => Some(s),
                _ => None,
            })
            .max()
            .copied()
            .unwrap_or(0)
    }

    /// Checks if any block in the slot `s` is finalized
    pub fn is_finalized(&self, slot: Slot) -> bool {
        self.completed_certificates.keys().any(|cert_id| {
            matches!(cert_id, CertificateId::Finalize(s) | CertificateId::FinalizeFast(s, _, _) if *s == slot)
        })
    }

    /// Checks if any block in slot `slot` is notarized, if so return the size of the certificate
    pub fn block_notarized(&self, slot: Slot) -> Option<usize> {
        self.completed_certificates
            .iter()
            .find_map(|(cert_id, cert)| {
                matches!(cert_id, CertificateId::Notarize(s,_,_) if *s == slot)
                    .then_some(cert.vote_count())
            })
    }

    /// Checks if the any block in slot `slot` has received a `NotarizeFallback` certificate, if so return
    /// the size of the certificate
    pub fn slot_notarized_fallback(&self, slot: Slot) -> Option<usize> {
        // TODO(ashwin): when updating voting loop for duplicate blocks, add parent tracker to make this
        // a true "branchCertified". For now this sufficies as branchCertified in the sequential loop
        self.completed_certificates
            .iter()
            .find_map(|(cert_id, cert)| {
                matches!(cert_id, CertificateId::NotarizeFallback(s,_,_) if *s == slot)
                    .then_some(cert.vote_count())
            })
    }

    /// Checks if `slot` has a `Skip` certificate
    pub fn skip_certified(&self, slot: Slot) -> bool {
        self.completed_certificates
            .contains_key(&CertificateId::Skip(slot))
    }

    /// Checks if we have voted to skip `slot` or notarize some block `b' = (block_id', bank_hash')` in `slot`
    /// Additionally check that for some differernt block `b = (block_id, bank_hash)` in `slot` either:
    /// (i) At least 40% of stake has voted to notarize `b`
    /// (ii) At least 20% of stake voted to notarize `b` and at least 60% of stake voted to either notarize `b` or skip `slot`
    /// If all the above hold, return `Some(block_id, bank_hash)` for the `b`
    pub fn safe_to_notar(&self, slot: Slot, vote_history: &VoteHistory) -> Option<(Hash, Hash)> {
        let epoch_stakes = self
            .epoch_stakes_map
            .get(&self.epoch_schedule.get_epoch(slot))?;
        let total_stake = epoch_stakes.total_stake();

        if !vote_history.voted(slot) {
            return None;
        }
        let b_prime = vote_history.voted_notar(slot);

        let skip_ratio = self
            .vote_pools
            .get(&(slot, VoteType::Skip))
            .map_or(0, |pool| pool.total_stake_by_key(None, None)) as f64
            / total_stake as f64;

        let notarize_pool = self.vote_pools.get(&(slot, VoteType::Notarize))?;
        for (
            VoteKey {
                bank_hash,
                block_id,
            },
            votes,
        ) in notarize_pool.votes.iter()
        {
            let b_block_id = block_id.unwrap();
            let b_bank_hash = bank_hash.unwrap();
            if let Some((prev_block_id, prev_bank_hash)) = b_prime {
                if prev_block_id == b_block_id && prev_bank_hash == b_bank_hash {
                    continue;
                }
            }
            // Check if the block fits condition (i) 40% of stake holders voted notarize
            let notarized_ratio = votes.total_stake_by_key as f64 / total_stake as f64;
            if notarized_ratio >= SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY {
                return Some((b_block_id, b_bank_hash));
            }

            // Check if the block fits condition (ii) 20% notarized, and 60% notarized or skip
            if notarized_ratio < SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP {
                continue;
            }
            if notarized_ratio + skip_ratio >= SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP {
                return Some((b_block_id, b_bank_hash));
            }
        }
        None
    }

    /// Checks if we have already voted to notarize some block in `slot` and additionally that
    /// votedStake(s) - topNotarStake(s) >= 40% where:
    /// - votedStake(s) is the cumulative stake of all nodes who voted notarize or skip on s
    /// - topNotarStake(s) the highest of cumulative notarize stake per block in s
    pub fn safe_to_skip(&self, slot: Slot, vote_history: &VoteHistory) -> bool {
        let epoch = self.epoch_schedule.get_epoch(slot);
        let Some(epoch_stakes) = self.epoch_stakes_map.get(&epoch) else {
            return false;
        };
        let total_stake = epoch_stakes.total_stake();

        if vote_history.voted_notar(slot).is_none() {
            return false;
        }

        let Some(notarize_pool) = self.vote_pools.get(&(slot, VoteType::Notarize)) else {
            return false;
        };
        let voted_stake = notarize_pool.total_stake()
            + self
                .vote_pools
                .get(&(slot, VoteType::Skip))
                .map_or(0, |pool| pool.total_stake());
        let top_notarized_stake = notarize_pool.top_entry_stake();
        (voted_stake - top_notarized_stake) as f64 / total_stake as f64 >= SAFE_TO_SKIP_THRESHOLD
    }

    /// Determines if the leader can start based on notarization and skip certificates.
    pub fn make_start_leader_decision(
        &self,
        my_leader_slot: Slot,
        parent_slot: Slot,
        first_alpenglow_slot: Slot,
    ) -> bool {
        // TODO: for GCE tests we WFSM on 1 so slot 1 is exempt
        let needs_notarization_certificate = parent_slot >= first_alpenglow_slot && parent_slot > 1;

        if needs_notarization_certificate
            && self.block_notarized(parent_slot).is_none()
            && !self.is_finalized(parent_slot)
        {
            error!("Missing notarization certificate {parent_slot}");
            return false;
        }

        let needs_skip_certificate =
            // handles cases where we are entering the alpenglow epoch, where the first
            // slot in the epoch will pass my_leader_slot == parent_slot
            my_leader_slot != first_alpenglow_slot &&
            my_leader_slot != parent_slot + 1;

        if needs_skip_certificate {
            let begin_skip_slot = first_alpenglow_slot.max(parent_slot + 1);
            for slot in begin_skip_slot..my_leader_slot {
                if !self.skip_certified(slot) {
                    error!(
                        "Missing skip certificate for {slot}, required for skip ceritifcate \
                        from {begin_skip_slot} to build {my_leader_slot}"
                    );
                    return false;
                }
            }
        }

        true
    }

    /// Cleanup any old slots from the certificate pool
    pub fn handle_new_root(&mut self, bank: Arc<Bank>) {
        self.root = bank.slot();
        // `completed_certificates`` now only contains entries >= `slot`
        self.completed_certificates
            .retain(|cert_id, _| match cert_id {
                CertificateId::Finalize(s)
                | CertificateId::FinalizeFast(s, _, _)
                | CertificateId::Notarize(s, _, _)
                | CertificateId::NotarizeFallback(s, _, _)
                | CertificateId::Skip(s) => *s >= self.root,
            });
        self.vote_pools = self
            .vote_pools
            .split_off(&(bank.slot(), VoteType::Finalize));
        self.update_epoch_stakes_map(&bank);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::{
                transaction::AlpenglowVoteTransaction,
                vote_certificate::{BlsCertificate, LegacyVoteCertificate},
            },
            *,
        },
        solana_runtime::{
            bank::{Bank, NewBankOptions},
            bank_forks::BankForks,
            genesis_utils::{create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs},
        },
        solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey, signer::Signer},
        std::sync::{Arc, RwLock},
    };

    fn dummy_transaction<VC: VoteCertificate>() -> VC::VoteTransaction {
        VC::VoteTransaction::new_for_test()
    }

    fn create_bank(slot: Slot, parent: Arc<Bank>, pubkey: &Pubkey) -> Bank {
        Bank::new_from_parent_with_options(parent, pubkey, slot, NewBankOptions::default())
    }

    fn create_bank_forks(validator_keypairs: &[ValidatorVoteKeypairs]) -> Arc<RwLock<BankForks>> {
        let genesis = create_genesis_config_with_vote_accounts(
            1_000_000_000,
            validator_keypairs,
            vec![100; validator_keypairs.len()],
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        BankForks::new_rw_arc(bank0)
    }

    fn create_keypairs_and_pool<VC: VoteCertificate>(
    ) -> (Vec<ValidatorVoteKeypairs>, CertificatePool<VC>) {
        // Create 10 node validatorvotekeypairs vec
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let bank_forks = create_bank_forks(&validator_keypairs);
        let root_bank = bank_forks.read().unwrap().root_bank();
        (
            validator_keypairs,
            CertificatePool::new_from_root_bank(&root_bank.clone()),
        )
    }

    fn add_certificate<VC: VoteCertificate>(
        pool: &mut CertificatePool<VC>,
        validator_keypairs: &[ValidatorVoteKeypairs],
        vote: Vote,
    ) {
        for keys in validator_keypairs.iter().take(6) {
            assert!(pool
                .add_vote(
                    &vote,
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey()
                )
                .is_ok());
        }
        assert!(pool
            .add_vote(
                &vote,
                dummy_transaction::<VC>(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        match vote {
            Vote::Notarize(vote) => assert_eq!(pool.highest_notarized_slot(), vote.slot()),
            Vote::NotarizeFallback(vote) => assert_eq!(pool.highest_notarized_slot(), vote.slot()),
            Vote::Skip(vote) => assert_eq!(pool.highest_skip_slot(), vote.slot()),
            Vote::SkipFallback(vote) => assert_eq!(pool.highest_skip_slot(), vote.slot()),
            Vote::Finalize(vote) => assert_eq!(pool.highest_finalized_slot(), vote.slot()),
        }
    }

    fn add_skip_vote_range<VC: VoteCertificate>(
        pool: &mut CertificatePool<VC>,
        start: Slot,
        end: Slot,
        pubkey: Pubkey,
    ) {
        for slot in start..=end {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(slot),
                    dummy_transaction::<VC>(),
                    &pubkey,
                )
                .is_ok());
        }
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_notarization_missing() {
        test_make_decision_leader_does_not_start_if_notarization_missing_with_type::<
            LegacyVoteCertificate,
        >();
        test_make_decision_leader_does_not_start_if_notarization_missing_with_type::<BlsCertificate>(
        );
    }

    fn test_make_decision_leader_does_not_start_if_notarization_missing_with_type<
        VC: VoteCertificate,
    >() {
        let (_, pool) = create_keypairs_and_pool::<VC>();

        // No notarization set, pool is default
        let parent_slot = 2;
        let my_leader_slot = 3;
        let first_alpenglow_slot = 0;
        let decision =
            pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot);
        assert!(
            !decision,
            "Leader should not be allowed to start without notarization"
        );
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_1() {
        test_make_decision_first_alpenglow_slot_edge_case_1_with_type::<LegacyVoteCertificate>();
        test_make_decision_first_alpenglow_slot_edge_case_1_with_type::<BlsCertificate>();
    }

    fn test_make_decision_first_alpenglow_slot_edge_case_1_with_type<VC: VoteCertificate>() {
        let (_, pool) = create_keypairs_and_pool::<VC>();

        // If parent_slot == 0, you don't need a notarization certificate
        // Because leader_slot == parent_slot + 1, you don't need a skip certificate
        let parent_slot = 0;
        let my_leader_slot = 1;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_2() {
        test_make_decision_first_alpenglow_slot_edge_case_2_with_type::<LegacyVoteCertificate>();
        test_make_decision_first_alpenglow_slot_edge_case_2_with_type::<BlsCertificate>();
    }

    fn test_make_decision_first_alpenglow_slot_edge_case_2_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // If parent_slot < first_alpenglow_slot, and parent_slot > 0
        // no notarization certificate is required, but a skip
        // certificate will be
        let parent_slot = 1;
        let my_leader_slot = 3;
        let first_alpenglow_slot = 2;

        assert!(!pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
        ));

        add_certificate::<VC>(
            &mut pool,
            &validator_keypairs,
            Vote::new_skip_vote(first_alpenglow_slot),
        );

        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_3() {
        test_make_decision_first_alpenglow_slot_edge_case_3_with_type::<LegacyVoteCertificate>();
        test_make_decision_first_alpenglow_slot_edge_case_3_with_type::<BlsCertificate>();
    }

    fn test_make_decision_first_alpenglow_slot_edge_case_3_with_type<VC: VoteCertificate>() {
        let (_, pool) = create_keypairs_and_pool::<VC>();
        // If parent_slot == first_alpenglow_slot, and
        // first_alpenglow_slot > 0, you need a notarization certificate
        let parent_slot = 2;
        let my_leader_slot = 3;
        let first_alpenglow_slot = 2;
        assert!(!pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
        ));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_4() {
        test_make_decision_first_alpenglow_slot_edge_case_4_with_type::<LegacyVoteCertificate>();
        test_make_decision_first_alpenglow_slot_edge_case_4_with_type::<BlsCertificate>();
    }

    fn test_make_decision_first_alpenglow_slot_edge_case_4_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // If parent_slot < first_alpenglow_slot, and parent_slot == 0,
        // no notarization certificate is required, but a skip certificate will
        // be
        let parent_slot = 0;
        let my_leader_slot = 2;
        let first_alpenglow_slot = 1;

        assert!(!pool.make_start_leader_decision(
            my_leader_slot,
            parent_slot,
            first_alpenglow_slot,
        ));

        add_certificate::<VC>(
            &mut pool,
            &validator_keypairs,
            Vote::new_skip_vote(first_alpenglow_slot),
        );
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_5() {
        test_make_decision_first_alpenglow_slot_edge_case_5_with_type::<LegacyVoteCertificate>();
        test_make_decision_first_alpenglow_slot_edge_case_5_with_type::<BlsCertificate>();
    }

    fn test_make_decision_first_alpenglow_slot_edge_case_5_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // Valid skip certificate for 1-9 exists
        for slot in 1..=9 {
            add_certificate::<VC>(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }

        // Parent slot is equal to 0, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 0;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_6() {
        test_make_decision_first_alpenglow_slot_edge_case_6_with_type::<LegacyVoteCertificate>();
        test_make_decision_first_alpenglow_slot_edge_case_6_with_type::<BlsCertificate>();
    }

    fn test_make_decision_first_alpenglow_slot_edge_case_6_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // Valid skip certificate for 1-9 exists
        for slot in 1..=9 {
            add_certificate::<VC>(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }
        // Parent slot is less than first_alpenglow_slot, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 4;
        let first_alpenglow_slot = 5;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_skip_certificate_missing() {
        test_make_decision_leader_does_not_start_if_skip_certificate_missing_with_type::<
            LegacyVoteCertificate,
        >();
        test_make_decision_leader_does_not_start_if_skip_certificate_missing_with_type::<
            BlsCertificate,
        >();
    }

    fn test_make_decision_leader_does_not_start_if_skip_certificate_missing_with_type<
        VC: VoteCertificate,
    >() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        let bank_forks = create_bank_forks(&validator_keypairs);
        let my_pubkey = validator_keypairs[0].vote_keypair.pubkey();

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        bank.freeze();
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        add_certificate::<VC>(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // No skip certificate for 6-10
        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        let decision =
            pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot);
        assert!(
            !decision,
            "Leader should not be allowed to start if a skip certificate is missing"
        );
    }

    #[test]
    fn test_make_decision_leader_starts_when_no_skip_required() {
        test_make_decision_leader_starts_when_no_skip_required_with_type::<LegacyVoteCertificate>();
        test_make_decision_leader_starts_when_no_skip_required_with_type::<BlsCertificate>();
    }

    fn test_make_decision_leader_starts_when_no_skip_required_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // Notarize slot 5
        add_certificate::<VC>(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Leader slot is just +1 from notarized slot (no skip needed)
        let my_leader_slot = 6;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_leader_starts_if_notarized_and_skips_valid() {
        test_make_decision_leader_starts_if_notarized_and_skips_valid_with_type::<
            LegacyVoteCertificate,
        >();
        test_make_decision_leader_starts_if_notarized_and_skips_valid_with_type::<BlsCertificate>();
    }

    fn test_make_decision_leader_starts_if_notarized_and_skips_valid_with_type<
        VC: VoteCertificate,
    >() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // Notarize slot 5
        add_certificate::<VC>(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Valid skip certificate for 6-9 exists
        for slot in 6..=9 {
            add_certificate::<VC>(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_make_decision_leader_starts_if_skip_range_superset() {
        test_make_decision_leader_starts_if_skip_range_superset_with_type::<LegacyVoteCertificate>(
        );
        test_make_decision_leader_starts_if_skip_range_superset_with_type::<BlsCertificate>();
    }

    fn test_make_decision_leader_starts_if_skip_range_superset_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // Notarize slot 5
        add_certificate::<VC>(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Valid skip certificate for 4-9 exists
        // Should start leader block even if the beginning of the range is from
        // before your last notarized slot
        for slot in 4..=9 {
            add_certificate::<VC>(
                &mut pool,
                &validator_keypairs,
                Vote::new_skip_fallback_vote(slot),
            );
        }

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot,));
    }

    #[test]
    fn test_add_vote_new_finalize_certificate() {
        test_add_vote_new_finalize_certificate_with_type::<LegacyVoteCertificate>();
        test_add_vote_new_finalize_certificate_with_type::<BlsCertificate>();
    }

    fn test_add_vote_new_finalize_certificate_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction::<VC>(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_finalized_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction::<VC>(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_finalized_slot(), 0);
        for keys in validator_keypairs.iter().take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_finalization_vote(5),
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_finalized_slot(), 0);
        assert!(pool
            .add_vote(
                &Vote::new_finalization_vote(5),
                dummy_transaction::<VC>(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_finalized_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_notarize_certificate() {
        test_add_vote_new_notarize_certificate_with_type::<LegacyVoteCertificate>();
        test_add_vote_new_notarize_certificate_with_type::<BlsCertificate>();
    }

    fn test_add_vote_new_notarize_certificate_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction::<VC>(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction::<VC>(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 0);

        for keys in validator_keypairs.iter().take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_notarized_slot(), 0);
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction::<VC>(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_notarize_fallback_certificate() {
        test_add_vote_new_notarize_fallback_certificate_with_type::<LegacyVoteCertificate>();
        test_add_vote_new_notarize_fallback_certificate_with_type::<BlsCertificate>();
    }

    fn test_add_vote_new_notarize_fallback_certificate_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        // 10% voted for notarize_fallback 5
        let pubkey = validator_keypairs[4].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_notarization_fallback_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction::<VC>(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 0);
        // 20% voted for notarize 5, 20% voted for notarize_fallback 5
        for keys in validator_keypairs.iter().take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        for keys in validator_keypairs.iter().skip(2).take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_notarized_slot(), 0);
        // Another 10% voted for notarize_fallback 5, we are over the threshold
        assert!(pool
            .add_vote(
                &Vote::new_notarization_vote(5, Hash::default(), Hash::default(),),
                dummy_transaction::<VC>(),
                &validator_keypairs[5].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_notarized_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_skip_certificate() {
        test_add_vote_new_skip_certificate_with_type::<LegacyVoteCertificate>();
        test_add_vote_new_skip_certificate_with_type::<BlsCertificate>();
    }
    fn test_add_vote_new_skip_certificate_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(&Vote::new_skip_vote(5), dummy_transaction::<VC>(), &pubkey)
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(&Vote::new_skip_vote(5), dummy_transaction::<VC>(), &pubkey,)
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        for keys in validator_keypairs.iter().take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(5),
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_skip_slot(), 0);
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(5),
                dummy_transaction::<VC>(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 5);
    }

    #[test]
    fn test_add_vote_new_skip_fallback_certificate() {
        test_add_vote_new_skip_fallback_certificate_with_type::<LegacyVoteCertificate>();
        test_add_vote_new_skip_fallback_certificate_with_type::<BlsCertificate>();
    }

    fn test_add_vote_new_skip_fallback_certificate_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        // 10% voted for skip_fallback 5
        let pubkey = validator_keypairs[5].vote_keypair.pubkey();
        assert!(pool
            .add_vote(
                &Vote::new_skip_fallback_vote(5),
                dummy_transaction::<VC>(),
                &pubkey
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_vote(
                &Vote::new_skip_fallback_vote(5),
                dummy_transaction::<VC>(),
                &pubkey,
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 0);
        // 20% voted for skip 5, 20% voted for skip_fallback 5
        for keys in validator_keypairs.iter().take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(5),
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        for keys in validator_keypairs.iter().skip(2).take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_fallback_vote(5),
                    dummy_transaction::<VC>(),
                    &keys.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(pool.highest_skip_slot(), 0);
        // Another 10% voted for skip_fallback 5, we are over the threshold
        assert!(pool
            .add_vote(
                &Vote::new_skip_fallback_vote(5),
                dummy_transaction::<VC>(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 5);
    }

    #[test]
    fn test_add_vote_zero_stake() {
        test_add_vote_zero_stake_with_type::<LegacyVoteCertificate>();
        test_add_vote_zero_stake_with_type::<BlsCertificate>();
    }

    fn test_add_vote_zero_stake_with_type<VC: VoteCertificate>() {
        let (_, mut pool) = create_keypairs_and_pool::<VC>();

        assert_eq!(
            pool.add_vote(
                &Vote::new_skip_vote(5),
                dummy_transaction::<VC>(),
                &Pubkey::new_unique()
            ),
            Err(AddVoteError::ZeroStake)
        );
    }

    fn assert_single_certificate_range<VC: VoteCertificate>(
        pool: &CertificatePool<VC>,
        exp_range_start: Slot,
        exp_range_end: Slot,
    ) {
        for i in exp_range_start..=exp_range_end {
            assert!(pool.skip_certified(i));
        }
    }

    #[test]
    fn test_consecutive_slots() {
        test_consecutive_slots_with_type::<LegacyVoteCertificate>();
        test_consecutive_slots_with_type::<BlsCertificate>();
    }

    fn test_consecutive_slots_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        add_certificate::<VC>(&mut pool, &validator_keypairs, Vote::new_skip_vote(15));
        assert_eq!(pool.highest_skip_slot(), 15);

        for (i, keypairs) in validator_keypairs.iter().enumerate() {
            let slot = i as u64 + 16;
            // These should not extend the skip range
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(slot),
                    dummy_transaction::<VC>(),
                    &keypairs.vote_keypair.pubkey()
                )
                .is_ok());
        }

        assert_single_certificate_range::<VC>(&pool, 15, 15);
    }

    #[test]
    fn test_multi_skip_cert() {
        test_multi_skip_cert_with_type::<LegacyVoteCertificate>();
        test_multi_skip_cert_with_type::<BlsCertificate>();
    }

    fn test_multi_skip_cert_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // We have 10 validators, 40% voted for (5, 15)
        for pubkeys in validator_keypairs.iter().take(4) {
            add_skip_vote_range::<VC>(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        // 30% voted for (5, 8)
        for pubkeys in validator_keypairs.iter().skip(4).take(3) {
            add_skip_vote_range::<VC>(&mut pool, 5, 8, pubkeys.vote_keypair.pubkey());
        }
        // The rest voted for (11, 15)
        for pubkeys in validator_keypairs.iter().skip(7) {
            add_skip_vote_range::<VC>(&mut pool, 11, 15, pubkeys.vote_keypair.pubkey());
        }
        // Test slots from 5 to 15, (5, 8) and (11, 15) should be certified, the others aren't
        for slot in 5..=15 {
            if slot > 8 && slot < 11 {
                assert!(!pool.skip_certified(slot));
            } else {
                assert!(pool.skip_certified(slot));
            }
        }
    }

    #[test]
    fn test_add_multiple_votes() {
        test_add_multiple_votes_with_type::<LegacyVoteCertificate>();
        test_add_multiple_votes_with_type::<BlsCertificate>();
    }

    fn test_add_multiple_votes_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();

        // 10 validators, half vote for (5, 15), the other (20, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range::<VC>(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        for pubkeys in validator_keypairs.iter().skip(5) {
            add_skip_vote_range::<VC>(&mut pool, 20, 30, pubkeys.vote_keypair.pubkey());
        }
        assert_eq!(pool.highest_skip_slot(), 0);

        // Now the first half vote for (5, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range::<VC>(&mut pool, 5, 30, pubkeys.vote_keypair.pubkey());
        }
        assert_single_certificate_range::<VC>(&pool, 20, 30);
    }

    #[test]
    fn test_add_multiple_disjoint_votes() {
        test_add_multiple_disjoint_votes_with_type::<LegacyVoteCertificate>();
        test_add_multiple_disjoint_votes_with_type::<BlsCertificate>();
    }

    fn test_add_multiple_disjoint_votes_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        // 50% of the validators vote for (1, 10)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range::<VC>(&mut pool, 1, 10, pubkeys.vote_keypair.pubkey());
        }
        // 10% vote for (2, 2)
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(2),
                dummy_transaction::<VC>(),
                &validator_keypairs[6].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 2);

        assert_single_certificate_range::<VC>(&pool, 2, 2);
        // 10% vote for (4, 4)
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(4),
                dummy_transaction::<VC>(),
                &validator_keypairs[7].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 4);

        assert_single_certificate_range::<VC>(&pool, 2, 2);
        assert_single_certificate_range::<VC>(&pool, 4, 4);
        // 10% vote for (3, 3)
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(3),
                dummy_transaction::<VC>(),
                &validator_keypairs[8].vote_keypair.pubkey(),
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 4);
        assert_single_certificate_range::<VC>(&pool, 2, 4);
        assert!(pool.skip_certified(3));
        // Let the last 10% vote for (3, 10) now
        add_skip_vote_range::<VC>(
            &mut pool,
            3,
            10,
            validator_keypairs[8].vote_keypair.pubkey(),
        );
        assert_eq!(pool.highest_skip_slot(), 10);
        assert_single_certificate_range::<VC>(&pool, 2, 10);
        assert!(pool.skip_certified(7));
    }

    #[test]
    fn test_update_existing_singleton_vote() {
        test_update_existing_singleton_vote_with_type::<LegacyVoteCertificate>();
        test_update_existing_singleton_vote_with_type::<BlsCertificate>();
    }

    fn test_update_existing_singleton_vote_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        // 50% voted on (1, 6)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range::<VC>(&mut pool, 1, 6, pubkeys.vote_keypair.pubkey());
        }
        // Range expansion on a singleton vote should be ok
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(1),
                dummy_transaction::<VC>(),
                &validator_keypairs[6].vote_keypair.pubkey()
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 1);
        add_skip_vote_range::<VC>(&mut pool, 1, 6, validator_keypairs[6].vote_keypair.pubkey());
        assert_eq!(pool.highest_skip_slot(), 6);
        assert_single_certificate_range::<VC>(&pool, 1, 6);
    }

    #[test]
    fn test_update_existing_vote() {
        test_update_existing_vote_with_type::<LegacyVoteCertificate>();
        test_update_existing_vote_with_type::<BlsCertificate>();
    }

    fn test_update_existing_vote_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        // 50% voted for (10, 25)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range::<VC>(&mut pool, 10, 25, pubkeys.vote_keypair.pubkey());
        }
        let pubkey = validator_keypairs[6].vote_keypair.pubkey();

        add_skip_vote_range::<VC>(&mut pool, 10, 20, pubkey);
        assert_eq!(pool.highest_skip_slot(), 20);
        assert_single_certificate_range::<VC>(&pool, 10, 20);

        // AlreadyExists, silently fail
        assert!(pool
            .add_vote(&Vote::new_skip_vote(20), dummy_transaction::<VC>(), &pubkey)
            .is_ok());
    }

    #[test]
    fn test_threshold_not_reached() {
        test_threshold_not_reached_with_type::<LegacyVoteCertificate>();
        test_threshold_not_reached_with_type::<BlsCertificate>();
    }

    fn test_threshold_not_reached_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        // half voted (5, 15) and the other half voted (20, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range::<VC>(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        for pubkeys in validator_keypairs.iter().skip(5) {
            add_skip_vote_range::<VC>(&mut pool, 20, 30, pubkeys.vote_keypair.pubkey());
        }
        for slot in 5..31 {
            assert!(!pool.skip_certified(slot));
        }
    }

    #[test]
    fn test_update_and_skip_range_certify() {
        test_update_and_skip_range_certify_with_type::<LegacyVoteCertificate>();
        test_update_and_skip_range_certify_with_type::<BlsCertificate>();
    }

    fn test_update_and_skip_range_certify_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        // half voted (5, 15) and the other half voted (10, 30)
        for pubkeys in validator_keypairs.iter().take(5) {
            add_skip_vote_range::<VC>(&mut pool, 5, 15, pubkeys.vote_keypair.pubkey());
        }
        for pubkeys in validator_keypairs.iter().skip(5) {
            add_skip_vote_range::<VC>(&mut pool, 10, 30, pubkeys.vote_keypair.pubkey());
        }
        for slot in 5..10 {
            assert!(!pool.skip_certified(slot));
        }
        for slot in 16..31 {
            assert!(!pool.skip_certified(slot));
        }
        assert_single_certificate_range::<VC>(&pool, 10, 15);
    }

    #[test]
    fn test_safe_to_notar() {
        test_safe_to_notar_with_type::<LegacyVoteCertificate>();
        test_safe_to_notar_with_type::<BlsCertificate>();
    }

    fn test_safe_to_notar_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        let my_pubkey = validator_keypairs[0].vote_keypair.pubkey();
        let mut vote_history = VoteHistory::default();

        // Create bank 2
        let slot = 2;
        let block_id = Hash::new_unique();
        let bank_hash = Hash::new_unique();

        // With no votes, this should fail.
        assert!(pool.safe_to_notar(slot, &vote_history).is_none());

        // Add a skip from myself.
        assert!(pool
            .add_vote(
                &Vote::new_skip_vote(2),
                dummy_transaction::<VC>(),
                &my_pubkey,
            )
            .is_ok());
        vote_history.add_vote(Vote::new_skip_vote(2));
        // 40% notarized, should succeed
        for keypairs in validator_keypairs.iter().skip(1).take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(2, block_id, bank_hash),
                    dummy_transaction::<VC>(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(
            pool.safe_to_notar(slot, &vote_history),
            Some((block_id, bank_hash))
        );

        // Create bank 3
        let slot = 3;
        let block_id = Hash::new_unique();
        let bank_hash = Hash::new_unique();

        // Add 20% notarize, but no vote from myself, should fail
        for keypairs in validator_keypairs.iter().skip(1).take(2) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(3, block_id, bank_hash),
                    dummy_transaction::<VC>(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(pool.safe_to_notar(slot, &vote_history).is_none());

        // Add a notarize from myself for some other block, but still not enough notar or skip, should fail.
        let vote = Vote::new_notarization_vote(3, Hash::new_unique(), Hash::new_unique());
        assert!(pool
            .add_vote(&vote, dummy_transaction::<VC>(), &my_pubkey,)
            .is_ok());
        vote_history.add_vote(vote);
        assert!(pool.safe_to_notar(slot, &vote_history).is_none());

        // Now add 40% skip, should succeed
        for keypairs in validator_keypairs.iter().skip(3).take(4) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(3),
                    dummy_transaction::<VC>(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert_eq!(
            pool.safe_to_notar(slot, &vote_history),
            Some((block_id, bank_hash))
        );
    }

    #[test]
    fn test_safe_to_skip() {
        test_safe_to_skip_with_type::<LegacyVoteCertificate>();
        test_safe_to_skip_with_type::<BlsCertificate>();
    }

    fn test_safe_to_skip_with_type<VC: VoteCertificate>() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool::<VC>();
        let my_pubkey = validator_keypairs[0].vote_keypair.pubkey();
        let slot = 2;
        let mut vote_history = VoteHistory::default();
        // No vote from myself, should fail.
        assert!(!pool.safe_to_skip(slot, &vote_history));

        // Add a notarize from myself.
        let block_id = Hash::new_unique();
        let block_hash = Hash::new_unique();
        let vote = Vote::new_notarization_vote(2, block_id, block_hash);
        assert!(pool
            .add_vote(&vote, dummy_transaction::<VC>(), &my_pubkey,)
            .is_ok());
        vote_history.add_vote(vote);
        // Should still fail because there are no other votes.
        assert!(!pool.safe_to_skip(slot, &vote_history));
        // Add 50% skip, should succeed
        for keypairs in validator_keypairs.iter().skip(1).take(5) {
            assert!(pool
                .add_vote(
                    &Vote::new_skip_vote(2),
                    dummy_transaction::<VC>(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(pool.safe_to_skip(slot, &vote_history));
        // Add 10% more notarize, still safe to skip any more because total voted increased.
        for keypairs in validator_keypairs.iter().skip(6).take(1) {
            assert!(pool
                .add_vote(
                    &Vote::new_notarization_vote(2, block_id, block_hash),
                    dummy_transaction::<VC>(),
                    &keypairs.vote_keypair.pubkey(),
                )
                .is_ok());
        }
        assert!(pool.safe_to_skip(slot, &vote_history));
    }

    /*
    fn create_new_vote(vote_type: VoteType, slot: Slot) -> Vote {
        match vote_type {
            VoteType::Notarize => {
                Vote::new_notarization_vote(slot, Hash::default(), Hash::default())
            }
            VoteType::NotarizeFallback => {
                Vote::new_notarization_fallback_vote(slot, Hash::default(), Hash::default())
            }
            VoteType::Skip => Vote::new_skip_vote(slot),
            VoteType::SkipFallback => Vote::new_skip_fallback_vote(slot),
            VoteType::Finalize => Vote::new_finalization_vote(slot),
        }
    }


        fn test_reject_conflicting_vote(
            pool: &mut CertificatePool,
            pubkey: &Pubkey,
            vote_type_1: VoteType,
            vote_type_2: VoteType,
            slot: Slot,
        ) {
            let vote_1 = create_new_vote(vote_type_1, slot);
            let vote_2 = create_new_vote(vote_type_2, slot);
            assert!(pool.add_vote(&vote_1, dummy_transaction(), pubkey).is_ok());
            assert!(pool.add_vote(&vote_2, dummy_transaction(), pubkey).is_err());
        }

        #[test]
        fn test_reject_conflicting_votes() {
            let (validator_keypairs, mut pool) = create_keypairs_and_pool();
            let mut slot = 2;
            for (vote_type_1, vote_type_2) in CONFLICTING_VOTETYPES.iter() {
                let pubkey = validator_keypairs[0].vote_keypair.pubkey();
                test_reject_conflicting_vote(&mut pool, &pubkey, *vote_type_1, *vote_type_2, slot);
                test_reject_conflicting_vote(&mut pool, &pubkey, *vote_type_2, *vote_type_1, slot + 1);
                slot += 2;
            }
        }
    */

    #[test]
    fn test_handle_new_root() {
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let bank_forks = create_bank_forks(&validator_keypairs);
        let root_bank = bank_forks.read().unwrap().root_bank();
        let mut pool: CertificatePool<LegacyVoteCertificate> =
            CertificatePool::new_from_root_bank(&root_bank.clone());
        assert_eq!(pool.root(), 0);

        let new_bank = Arc::new(create_bank(2, root_bank, &Pubkey::new_unique()));
        pool.handle_new_root(new_bank.clone());
        assert_eq!(pool.root(), 2);
        let new_bank = Arc::new(create_bank(3, new_bank, &Pubkey::new_unique()));
        pool.handle_new_root(new_bank);
        assert_eq!(pool.root(), 3);
    }
}
