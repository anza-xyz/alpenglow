use {
    crate::{
        certificate_limits_and_vote_types,
        certificate_pool::{
            parent_ready_tracker::ParentReadyTracker,
            stats::CertificatePoolStats,
            vote_certificate::{CertificateError, VoteCertificate},
            vote_pool::{DuplicateBlockVotePool, SimpleVotePool, VotePool, VotePoolType},
        },
        conflicting_types,
        event::VotorEvent,
        vote_to_certificate_ids, Block, CertificateId, Stake, VoteType,
        MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE, MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES,
        MAX_SLOT_AGE, SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP,
        SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP, SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY,
        SAFE_TO_SKIP_THRESHOLD,
    },
    crossbeam_channel::Sender,
    solana_ledger::blockstore::Blockstore,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, epoch_stakes::EpochStakes},
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
        hash::Hash,
    },
    solana_vote::alpenglow::{
        bls_message::{BLSMessage, CertificateMessage, VoteMessage},
        vote::Vote,
    },
    std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    },
    thiserror::Error,
};

pub mod parent_ready_tracker;
mod stats;
mod vote_certificate;
mod vote_pool;

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
    #[error("Conflicting vote type: {0:?} vs existing {1:?} for slot: {2} pubkey: {3}")]
    ConflictingVoteType(VoteType, VoteType, Slot, Pubkey),

    #[error("Epoch stakes missing for epoch: {0}")]
    EpochStakesNotFound(Epoch),

    #[error("Unrooted slot")]
    UnrootedSlot,

    #[error("Slot in the future")]
    SlotInFuture,

    #[error("Certificate error: {0}")]
    Certificate(#[from] CertificateError),

    #[error("Certificate sender error")]
    CertificateSenderError,

    #[error("Voting Service sender disconnected")]
    VotingServiceSenderDisconnected,

    #[error("Voting Service queue full")]
    VotingServiceQueueFull,

    #[error("Invalid rank: {0}")]
    InvalidRank(u16),
}

#[derive(Default)]
pub struct CertificatePool {
    // Vote pools to do bean counting for votes.
    vote_pools: BTreeMap<PoolId, VotePoolType>,
    /// Completed certificates
    completed_certificates: BTreeMap<CertificateId, Arc<CertificateMessage>>,
    /// Tracks slots which have reached the parent ready condition:
    /// - They have a potential parent block with a NotarizeFallback certificate
    /// - All slots from the parent have a Skip certificate
    pub parent_ready_tracker: ParentReadyTracker,
    /// Highest block that has a NotarizeFallback certificate, for use in producing our leader window
    highest_notarized_fallback: Option<(Slot, Hash)>,
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
    /// The certificate sender, if set, newly created certificates will be sent here
    certificate_sender: Option<Sender<(CertificateId, CertificateMessage)>>,
    /// Stats for the certificate pool
    stats: CertificatePoolStats,
}

impl CertificatePool {
    pub fn new_from_root_bank(
        my_pubkey: Pubkey,
        bank: &Bank,
        certificate_sender: Option<Sender<(CertificateId, CertificateMessage)>>,
    ) -> Self {
        // To account for genesis and snapshots we allow default block id until
        // block id can be serialized  as part of the snapshot
        let root_block = (bank.slot(), bank.block_id().unwrap_or_default());
        let parent_ready_tracker = ParentReadyTracker::new(my_pubkey, root_block);

        let mut pool = Self {
            vote_pools: BTreeMap::new(),
            completed_certificates: BTreeMap::new(),
            highest_notarized_fallback: None,
            highest_finalized_slot: None,
            epoch_schedule: EpochSchedule::default(),
            epoch_stakes_map: Arc::new(HashMap::new()),
            root: bank.slot(),
            root_epoch: Epoch::default(),
            certificate_sender,
            parent_ready_tracker,
            stats: CertificatePoolStats::new(),
        };

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

    fn new_vote_pool(vote_type: VoteType) -> VotePoolType {
        match vote_type {
            VoteType::NotarizeFallback => VotePoolType::DuplicateBlockVotePool(
                DuplicateBlockVotePool::new(MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE),
            ),
            VoteType::Notarize => VotePoolType::DuplicateBlockVotePool(
                DuplicateBlockVotePool::new(MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES),
            ),
            _ => VotePoolType::SimpleVotePool(SimpleVotePool::new()),
        }
    }

    fn update_vote_pool(
        &mut self,
        slot: Slot,
        vote_type: VoteType,
        block_id: Option<Hash>,
        transaction: &VoteMessage,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
    ) -> bool {
        let pool = self
            .vote_pools
            .entry((slot, vote_type))
            .or_insert_with(|| Self::new_vote_pool(vote_type));
        match pool {
            VotePoolType::SimpleVotePool(pool) => {
                pool.add_vote(validator_vote_key, validator_stake, transaction)
            }
            VotePoolType::DuplicateBlockVotePool(pool) => pool.add_vote(
                validator_vote_key,
                block_id.expect("Duplicate block pool expects a voted block key"),
                transaction,
                validator_stake,
            ),
        }
    }

    /// For a new vote `slot` , `vote_type` checks if any
    /// of the related certificates are newly complete.
    /// For each newly constructed certificate
    /// - Insert it into `self.certificates`
    /// - Potentially update `self.highest_notarized_fallback`,
    /// - If it is a `is_critical` certificate, send via the certificate sender
    /// - Potentially update `self.highest_finalized_slot`,
    /// - If we have a new highest finalized slot, return it
    /// - update any newly created events
    fn update_certificates(
        &mut self,
        vote: &Vote,
        block_id: Option<Hash>,
        events: &mut Vec<VotorEvent>,
        total_stake: Stake,
    ) -> Result<Vec<Arc<CertificateMessage>>, AddVoteError> {
        let slot = vote.slot();
        let mut new_certificates_to_send = Vec::new();
        for cert_id in vote_to_certificate_ids(vote) {
            // If the certificate is already complete, skip it
            if self.completed_certificates.contains_key(&cert_id) {
                continue;
            }
            // Otherwise check whether the certificate is complete
            let (limit, vote_types) = certificate_limits_and_vote_types(cert_id);
            let accumulated_stake = vote_types
                .iter()
                .filter_map(|vote_type| {
                    Some(match self.vote_pools
                        .get(&(slot, *vote_type))? {
                            VotePoolType::SimpleVotePool(pool) => pool.total_stake(),
                            VotePoolType::DuplicateBlockVotePool(pool) => pool.total_stake_by_block_id(block_id.as_ref().expect("Duplicate block pool for {vote_type:?} expects a voted block key for certificate {cert_id:?}")),
                        })
                })
                .sum::<Stake>();
            if accumulated_stake as f64 / (total_stake as f64) < limit {
                continue;
            }
            let mut vote_certificate = VoteCertificate::new(cert_id);
            vote_types.iter().for_each(|vote_type| {
                if let Some(vote_pool) = self.vote_pools.get(&(slot, *vote_type)) {
                match vote_pool {
                    VotePoolType::SimpleVotePool(pool) => pool.add_to_certificate(&mut vote_certificate),
                    VotePoolType::DuplicateBlockVotePool(pool) => pool.add_to_certificate(block_id.as_ref().expect("Duplicate block pool for {vote_type:?} expects a voted block key for certificate {cert_id:?}"), &mut vote_certificate),
                };
            }
            });
            let new_cert = Arc::new(vote_certificate.certificate());
            self.send_and_insert_certificate(cert_id, new_cert.clone(), events)?;
            self.stats
                .incr_cert_type(new_cert.certificate.certificate_type, true);
            new_certificates_to_send.push(new_cert);
        }
        Ok(new_certificates_to_send)
    }

    fn send_and_insert_certificate(
        &mut self,
        cert_id: CertificateId,
        vote_certificate: Arc<CertificateMessage>,
        events: &mut Vec<VotorEvent>,
    ) -> Result<(), AddVoteError> {
        if let Some(sender) = &self.certificate_sender {
            if cert_id.is_critical() {
                if let Err(e) = sender.try_send((cert_id, (*vote_certificate).clone())) {
                    error!("Unable to send certificate {cert_id:?}: {e:?}");
                    return Err(AddVoteError::CertificateSenderError);
                }
            }
        }
        self.insert_certificate(cert_id, vote_certificate, events);
        Ok(())
    }

    fn has_conflicting_vote(
        &self,
        slot: Slot,
        vote_type: VoteType,
        validator_vote_key: &Pubkey,
        block_id: &Option<Hash>,
    ) -> Option<VoteType> {
        for conflicting_type in conflicting_types(vote_type) {
            if let Some(pool) = self.vote_pools.get(&(slot, *conflicting_type)) {
                let is_conflicting = match pool {
                    // In a simple vote pool, just check if the validator previously voted at all. If so, that's a conflict
                    VotePoolType::SimpleVotePool(pool) => {
                        pool.has_prev_validator_vote(validator_vote_key)
                    }
                    // In a duplicate block vote pool, because some conflicts between things like Notarize and NotarizeFallback
                    // for different blocks are allowed, we need a more specific check.
                    // TODO: This can be made much cleaner/safer if VoteType carried the bank hash, block id so we
                    // could check which exact VoteType(blockid, bankhash) was the source of the conflict.
                    VotePoolType::DuplicateBlockVotePool(pool) => {
                        if let Some(block_id) = &block_id {
                            // Reject votes for the same block with a conflicting type, i.e.
                            // a NotarizeFallback vote for the same block as a Notarize vote.
                            pool.has_prev_validator_vote_for_block(validator_vote_key, block_id)
                        } else {
                            pool.has_prev_validator_vote(validator_vote_key)
                        }
                    }
                };
                if is_conflicting {
                    return Some(*conflicting_type);
                }
            }
        }
        None
    }

    fn insert_certificate(
        &mut self,
        cert_id: CertificateId,
        cert: Arc<CertificateMessage>,
        events: &mut Vec<VotorEvent>,
    ) {
        self.completed_certificates.insert(cert_id, cert);
        match cert_id {
            CertificateId::NotarizeFallback(slot, block_id) => {
                self.parent_ready_tracker
                    .add_new_notar_fallback((slot, block_id), events);
                if self
                    .highest_notarized_fallback
                    .map_or(true, |(s, _)| s < slot)
                {
                    self.highest_notarized_fallback = Some((slot, block_id));
                }
            }
            CertificateId::Skip(slot) => self.parent_ready_tracker.add_new_skip(slot, events),
            CertificateId::Notarize(slot, block_id) => {
                events.push(VotorEvent::BlockNotarized((slot, block_id)));
                if self.is_finalized(slot) {
                    events.push(VotorEvent::Finalized((slot, block_id)));
                }
            }
            CertificateId::Finalize(slot) => {
                if let Some(block) = self.get_notarized_block(slot) {
                    events.push(VotorEvent::Finalized(block));
                }
                if self.highest_finalized_slot.map_or(true, |s| s < slot) {
                    self.highest_finalized_slot = Some(slot);
                }
            }
            CertificateId::FinalizeFast(slot, block_id) => {
                events.push(VotorEvent::Finalized((slot, block_id)));
                if self.highest_finalized_slot.map_or(true, |s| s < slot) {
                    self.highest_finalized_slot = Some(slot);
                }
            }
        }
    }

    fn get_key_and_stakes(
        &self,
        slot: Slot,
        rank: u16,
    ) -> Result<(Pubkey, Stake, Stake), AddVoteError> {
        let epoch = self.epoch_schedule.get_epoch(slot);
        let epoch_stakes = self
            .epoch_stakes_map
            .get(&epoch)
            .ok_or(AddVoteError::EpochStakesNotFound(epoch))?;
        let Some((vote_key, _)) = epoch_stakes
            .bls_pubkey_to_rank_map()
            .get_pubkey(rank as usize)
        else {
            return Err(AddVoteError::InvalidRank(rank));
        };
        let stake = epoch_stakes.vote_account_stake(vote_key);
        if stake == 0 {
            // Since we have a valid rank, this should never happen, there is no rank for zero stake.
            panic!("Validator stake is zero for pubkey: {vote_key}");
        }
        Ok((*vote_key, stake, epoch_stakes.total_stake()))
    }

    /// Adds the new vote the the certificate pool. If a new certificate is created
    /// as a result of this, send it via the `self.certificate_sender`
    ///
    /// Any new votor events that are a result of adding this new vote will be added
    /// to `events`.
    ///
    /// If this resulted in a new highest Finalize or FastFinalize certificate,
    /// return the slot
    pub fn add_message(
        &mut self,
        my_vote_pubkey: &Pubkey,
        message: &BLSMessage,
        events: &mut Vec<VotorEvent>,
    ) -> Result<(Option<Slot>, Vec<Arc<CertificateMessage>>), AddVoteError> {
        let current_highest_finalized_slot = self.highest_finalized_slot;
        let new_certficates_to_send = match message {
            BLSMessage::Vote(vote_message) => {
                self.add_vote(my_vote_pubkey, vote_message, events)?
            }
            BLSMessage::Certificate(certificate_message) => {
                self.add_certificate(certificate_message, events)?
            }
        };
        // If we have a new highest finalized slot, return it
        let new_finalized_slot = if self.highest_finalized_slot > current_highest_finalized_slot {
            self.highest_finalized_slot
        } else {
            None
        };
        Ok((new_finalized_slot, new_certficates_to_send))
    }

    fn add_vote(
        &mut self,
        my_vote_pubkey: &Pubkey,
        vote_message: &VoteMessage,
        events: &mut Vec<VotorEvent>,
    ) -> Result<Vec<Arc<CertificateMessage>>, AddVoteError> {
        let vote = &vote_message.vote;
        let rank = vote_message.rank;
        let slot = vote.slot();
        let (validator_vote_key, validator_stake, total_stake) =
            self.get_key_and_stakes(slot, rank)?;

        // Since we have a valid rank, this should never happen, there is no rank for zero stake.
        assert_ne!(
            validator_stake, 0,
            "Validator stake is zero for pubkey: {validator_vote_key}"
        );

        self.stats.incoming_votes = self.stats.incoming_votes.saturating_add(1);
        if slot < self.root {
            self.stats.out_of_range_votes = self.stats.out_of_range_votes.saturating_add(1);
            return Err(AddVoteError::UnrootedSlot);
        }
        // We only allow votes
        if slot > self.root.saturating_add(MAX_SLOT_AGE) {
            self.stats.out_of_range_votes = self.stats.out_of_range_votes.saturating_add(1);
            return Err(AddVoteError::SlotInFuture);
        }

        let block_id = vote.block_id().map(|block_id| {
            if !matches!(vote, Vote::Notarize(_) | Vote::NotarizeFallback(_)) {
                panic!("expected Notarize or NotarizeFallback vote");
            }
            *block_id
        });
        let vote_type = VoteType::get_type(vote);
        if let Some(conflicting_type) =
            self.has_conflicting_vote(slot, vote_type, &validator_vote_key, &block_id)
        {
            self.stats.conflicting_votes = self.stats.conflicting_votes.saturating_add(1);
            return Err(AddVoteError::ConflictingVoteType(
                vote_type,
                conflicting_type,
                slot,
                validator_vote_key,
            ));
        }
        if !self.update_vote_pool(
            slot,
            vote_type,
            block_id,
            vote_message,
            &validator_vote_key,
            validator_stake,
        ) {
            self.stats.exist_votes = self.stats.exist_votes.saturating_add(1);
            return Ok(vec![]);
        }
        // Check if this new vote generated a safe to notar or safe to skip
        // TODO: we should just handle this when adding the vote rather than
        // calling out here again. Also deal with duplicate events, don't notify
        // everytime.
        if self.safe_to_skip(my_vote_pubkey, slot) {
            events.push(VotorEvent::SafeToSkip(slot));
            self.stats.event_safe_to_skip = self.stats.event_safe_to_skip.saturating_add(1);
        }
        for block_id in self.safe_to_notar(my_vote_pubkey, slot) {
            events.push(VotorEvent::SafeToNotar((slot, block_id)));
            self.stats.event_safe_to_notarize = self.stats.event_safe_to_notarize.saturating_add(1);
        }

        self.stats.incr_ingested_vote_type(vote_type);

        self.update_certificates(vote, block_id, events, total_stake)
    }

    fn add_certificate(
        &mut self,
        certificate_message: &CertificateMessage,
        events: &mut Vec<VotorEvent>,
    ) -> Result<Vec<Arc<CertificateMessage>>, AddVoteError> {
        let certificate = &certificate_message.certificate;
        let certificate_id = CertificateId::from(certificate);
        self.stats.incoming_certs = self.stats.incoming_certs.saturating_add(1);
        if certificate.slot < self.root {
            self.stats.out_of_range_certs = self.stats.out_of_range_certs.saturating_add(1);
            return Err(AddVoteError::UnrootedSlot);
        }
        if self.completed_certificates.contains_key(&certificate_id) {
            self.stats.exist_certs = self.stats.exist_certs.saturating_add(1);
            return Ok(vec![]);
        }
        let new_certificate = Arc::new(certificate_message.clone());
        self.send_and_insert_certificate(certificate_id, new_certificate.clone(), events)?;

        self.stats
            .incr_cert_type(certificate.certificate_type, false);

        Ok(vec![new_certificate])
    }

    /// The highest notarized fallback slot, for use as the parent slot in leader window
    pub fn highest_notarized_fallback(&self) -> Option<(Slot, Hash)> {
        self.highest_notarized_fallback
    }

    /// Get the notarized block in `slot`
    pub fn get_notarized_block(&self, slot: Slot) -> Option<Block> {
        self.completed_certificates
            .iter()
            .find_map(|(cert_id, _)| match cert_id {
                CertificateId::Notarize(s, block_id) if slot == *s => Some((*s, *block_id)),
                _ => None,
            })
    }

    #[cfg(test)]
    fn highest_notarized_slot(&self) -> Slot {
        // Return the max of CertificateType::Notarize and CertificateType::NotarizeFallback
        self.completed_certificates
            .iter()
            .filter_map(|(cert_id, _)| match cert_id {
                CertificateId::Notarize(s, _) => Some(s),
                CertificateId::NotarizeFallback(s, _) => Some(s),
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

    pub fn highest_finalized_slot(&self) -> Slot {
        self.completed_certificates
            .iter()
            .filter_map(|(cert_id, _)| match cert_id {
                CertificateId::Finalize(s) => Some(s),
                CertificateId::FinalizeFast(s, _) => Some(s),
                _ => None,
            })
            .max()
            .copied()
            .unwrap_or(0)
    }

    pub fn highest_fast_finalized_block(&self) -> Option<Block> {
        self.completed_certificates
            .iter()
            .filter_map(|(cert_id, _)| match cert_id {
                CertificateId::FinalizeFast(s, bid) => Some((*s, *bid)),
                _ => None,
            })
            .max()
    }

    /// Checks if any block in the slot `s` is finalized
    pub fn is_finalized(&self, slot: Slot) -> bool {
        self.completed_certificates.keys().any(|cert_id| {
            matches!(cert_id, CertificateId::Finalize(s) | CertificateId::FinalizeFast(s, _) if *s == slot)
        })
    }

    /// Check if the specific block `(block_id)` in slot `s` is notarized
    pub fn is_notarized(&self, slot: Slot, block_id: Hash) -> bool {
        self.completed_certificates
            .contains_key(&CertificateId::Notarize(slot, block_id))
    }

    /// Checks if the any block in slot `slot` has received a `NotarizeFallback` certificate, if so return
    /// the size of the certificate
    #[cfg(test)]
    pub fn slot_has_notarized_fallback(&self, slot: Slot) -> bool {
        self.completed_certificates.iter().any(
            |(cert_id, _)| matches!(cert_id, CertificateId::NotarizeFallback(s,_) if *s == slot),
        )
    }

    /// Checks if `slot` has a `Skip` certificate
    pub fn skip_certified(&self, slot: Slot) -> bool {
        self.completed_certificates
            .contains_key(&CertificateId::Skip(slot))
    }

    /// Checks if we have voted to skip `slot` or notarize some block `b' = block_id'` in `slot`
    /// Additionally check that for some different block `b = block_id` in `slot` either:
    /// (i) At least 40% of stake has voted to notarize `b`
    /// (ii) At least 20% of stake voted to notarize `b` and at least 60% of stake voted to either notarize `b` or skip `slot`
    /// and we have not already cast a notarize fallback for this `b`
    /// If all the above hold, return `Some(block_id)` for the `b`
    pub fn safe_to_notar(&self, my_vote_pubkey: &Pubkey, slot: Slot) -> Vec<Hash> {
        let Some(epoch_stakes) = self
            .epoch_stakes_map
            .get(&self.epoch_schedule.get_epoch(slot))
        else {
            return vec![];
        };
        let total_stake = epoch_stakes.total_stake();

        let skip_ratio = self
            .vote_pools
            .get(&(slot, VoteType::Skip))
            .map_or(0, |pool| pool.total_stake()) as f64
            / total_stake as f64;

        let voted_skip = self
            .vote_pools
            .get(&(slot, VoteType::Skip))
            .is_some_and(|pool| pool.has_prev_validator_vote(my_vote_pubkey));

        let Some(notarize_pool) = self.vote_pools.get(&(slot, VoteType::Notarize)) else {
            return vec![];
        };
        let notarize_pool = notarize_pool.unwrap_duplicate_block_vote_pool(
            "Notarize vote pool should be a DuplicateBlockVotePool",
        );
        let my_prev_voted_block_id = notarize_pool.get_prev_voted_block_id(my_vote_pubkey);

        let mut safe_to_notar = vec![];
        for (block_id, votes) in notarize_pool.votes.iter() {
            if !voted_skip
                && my_prev_voted_block_id
                    .as_ref()
                    .is_none_or(|prev_block_id| *prev_block_id == *block_id)
            {
                // We either have not voted for the slot or we voted notarize on this block.
                // Not eligble for safe to notar
                continue;
            }

            let notarized_ratio = votes.total_stake_by_key as f64 / total_stake as f64;
            let qualifies =
                // Check if the block fits condition (i) 40% of stake holders voted notarize
                notarized_ratio >= SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY
                // Check if the block fits condition (ii) 20% notarized, and 60% notarized or skip
                || (notarized_ratio >= SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP
                    && notarized_ratio + skip_ratio >= SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP);

            if qualifies {
                safe_to_notar.push(*block_id);
            }
        }
        safe_to_notar
    }

    /// Checks if we have already voted to notarize some block in `slot` and additionally that
    /// votedStake(s) - topNotarStake(s) >= 40% where:
    /// - votedStake(s) is the cumulative stake of all nodes who voted notarize or skip on s
    /// - topNotarStake(s) the highest of cumulative notarize stake per block in s
    pub fn safe_to_skip(&self, my_vote_pubkey: &Pubkey, slot: Slot) -> bool {
        let epoch = self.epoch_schedule.get_epoch(slot);
        let Some(epoch_stakes) = self.epoch_stakes_map.get(&epoch) else {
            return false;
        };
        let total_stake = epoch_stakes.total_stake();

        let Some(notarize_pool) = self.vote_pools.get(&(slot, VoteType::Notarize)) else {
            return false;
        };
        let notarize_pool = notarize_pool.unwrap_duplicate_block_vote_pool(
            "Notarize vote pool should be a DuplicateBlockVotePool",
        );
        if !notarize_pool.has_prev_validator_vote(my_vote_pubkey) {
            return false;
        }
        let voted_stake = notarize_pool.total_stake().saturating_add(
            self.vote_pools
                .get(&(slot, VoteType::Skip))
                .map_or(0, |pool| pool.total_stake()),
        );
        let top_notarized_stake = notarize_pool.top_entry_stake();
        (voted_stake.saturating_sub(top_notarized_stake)) as f64 / total_stake as f64
            >= SAFE_TO_SKIP_THRESHOLD
    }

    #[cfg(test)]
    fn make_start_leader_decision(
        &self,
        my_leader_slot: Slot,
        parent_slot: Slot,
        first_alpenglow_slot: Slot,
    ) -> bool {
        // TODO: for GCE tests we WFSM on 1 so slot 1 is exempt
        let needs_notarization_certificate = parent_slot >= first_alpenglow_slot && parent_slot > 1;

        if needs_notarization_certificate
            && !self.slot_has_notarized_fallback(parent_slot)
            && !self.is_finalized(parent_slot)
        {
            error!("Missing notarization certificate {parent_slot}");
            return false;
        }

        let needs_skip_certificate =
            // handles cases where we are entering the alpenglow epoch, where the first
            // slot in the epoch will pass my_leader_slot == parent_slot
            my_leader_slot != first_alpenglow_slot &&
            my_leader_slot != parent_slot.saturating_add(1);

        if needs_skip_certificate {
            let begin_skip_slot = first_alpenglow_slot.max(parent_slot.saturating_add(1));
            for slot in begin_skip_slot..my_leader_slot {
                if !self.skip_certified(slot) {
                    error!(
                        "Missing skip certificate for {slot}, required for skip certificate \
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
        let new_root = bank.slot();
        self.root = new_root;
        // `completed_certificates`` now only contains entries >= `slot`
        self.completed_certificates
            .retain(|cert_id, _| match cert_id {
                CertificateId::Finalize(s)
                | CertificateId::FinalizeFast(s, _)
                | CertificateId::Notarize(s, _)
                | CertificateId::NotarizeFallback(s, _)
                | CertificateId::Skip(s) => *s >= self.root,
            });
        self.vote_pools = self.vote_pools.split_off(&(new_root, VoteType::Finalize));
        self.parent_ready_tracker.set_root(new_root);
        self.update_epoch_stakes_map(&bank);
    }

    /// Updates the pubkey used for logging purposes only.
    /// This avoids the need to recreate the entire certificate pool since it's
    /// not distinguished by the pubkey.
    pub fn update_pubkey(&mut self, new_pubkey: Pubkey) {
        self.parent_ready_tracker.update_pubkey(new_pubkey);
    }

    pub fn maybe_report(&mut self) {
        self.stats.maybe_report();
    }
}

pub fn load_from_blockstore(
    my_pubkey: &Pubkey,
    root_bank: &Bank,
    blockstore: &Blockstore,
    certificate_sender: Option<Sender<(CertificateId, CertificateMessage)>>,
    events: &mut Vec<VotorEvent>,
) -> CertificatePool {
    let mut cert_pool =
        CertificatePool::new_from_root_bank(*my_pubkey, root_bank, certificate_sender);
    for (slot, slot_cert) in blockstore
        .slot_certificates_iterator(root_bank.slot())
        .unwrap()
    {
        let certs = slot_cert
            .notarize_fallback_certificates
            .into_iter()
            .map(|(block_id, cert)| {
                let cert_id = CertificateId::NotarizeFallback(slot, block_id);
                (cert_id, cert)
            })
            .chain(slot_cert.skip_certificate.map(|cert| {
                let cert_id = CertificateId::Skip(slot);
                (cert_id, cert)
            }));

        for (cert_id, cert) in certs {
            trace!("{my_pubkey}: loading certificate {cert_id:?} from blockstore into certificate pool");
            cert_pool.insert_certificate(cert_id, cert.into(), events);
        }
    }
    cert_pool
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bitvec::prelude::*,
        itertools::Itertools,
        solana_bls_signatures::{keypair::Keypair as BLSKeypair, Signature as BLSSignature},
        solana_runtime::{
            bank::{Bank, NewBankOptions},
            bank_forks::BankForks,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts_no_program,
                ValidatorVoteKeypairs,
            },
        },
        solana_sdk::{clock::Slot, pubkey::Pubkey, signer::Signer},
        solana_vote::alpenglow::{
            bls_message::{VoteMessage, BLS_KEYPAIR_DERIVE_SEED},
            certificate::{Certificate, CertificateType},
        },
        std::sync::{Arc, RwLock},
        test_case::test_case,
    };

    fn dummy_transaction(
        keypairs: &[ValidatorVoteKeypairs],
        vote: &Vote,
        rank: usize,
    ) -> BLSMessage {
        let bls_keypair =
            BLSKeypair::derive_from_signer(&keypairs[rank].vote_keypair, BLS_KEYPAIR_DERIVE_SEED)
                .unwrap();
        let signature: BLSSignature = bls_keypair
            .sign(bincode::serialize(vote).unwrap().as_slice())
            .into();
        BLSMessage::new_vote(*vote, signature, rank as u16)
    }

    fn create_bank(slot: Slot, parent: Arc<Bank>, pubkey: &Pubkey) -> Bank {
        Bank::new_from_parent_with_options(parent, pubkey, slot, NewBankOptions::default())
    }

    fn create_bank_forks(validator_keypairs: &[ValidatorVoteKeypairs]) -> Arc<RwLock<BankForks>> {
        let genesis = create_genesis_config_with_alpenglow_vote_accounts_no_program(
            1_000_000_000,
            validator_keypairs,
            vec![100; validator_keypairs.len()],
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        BankForks::new_rw_arc(bank0)
    }

    fn create_keypairs_and_pool() -> (Vec<ValidatorVoteKeypairs>, CertificatePool) {
        // Create 10 node validatorvotekeypairs vec
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let bank_forks = create_bank_forks(&validator_keypairs);
        let root_bank = bank_forks.read().unwrap().root_bank();
        (
            validator_keypairs,
            CertificatePool::new_from_root_bank(Pubkey::new_unique(), &root_bank.clone(), None),
        )
    }

    #[cfg(test)]
    fn add_certificate(
        pool: &mut CertificatePool,
        validator_keypairs: &[ValidatorVoteKeypairs],
        vote: Vote,
    ) {
        for rank in 0..6 {
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(validator_keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(validator_keypairs, &vote, 6),
                &mut vec![]
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

    fn add_skip_vote_range(
        pool: &mut CertificatePool,
        start: Slot,
        end: Slot,
        keypairs: &[ValidatorVoteKeypairs],
        rank: usize,
    ) {
        for slot in start..=end {
            let vote = Vote::new_skip_vote(slot);
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_notarization_missing() {
        let (_, pool) = create_keypairs_and_pool();

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
        let (_, pool) = create_keypairs_and_pool();

        // If parent_slot == 0, you don't need a notarization certificate
        // Because leader_slot == parent_slot + 1, you don't need a skip certificate
        let parent_slot = 0;
        let my_leader_slot = 1;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_2() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

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

        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_skip_vote(first_alpenglow_slot),
        );

        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_3() {
        let (_, pool) = create_keypairs_and_pool();
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
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

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

        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_skip_vote(first_alpenglow_slot),
        );
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_5() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Valid skip certificate for 1-9 exists
        for slot in 1..=9 {
            add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }

        // Parent slot is equal to 0, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 0;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_first_alpenglow_slot_edge_case_6() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Valid skip certificate for 1-9 exists
        for slot in 1..=9 {
            add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }
        // Parent slot is less than first_alpenglow_slot, so no notarization certificate required
        let my_leader_slot = 10;
        let parent_slot = 4;
        let first_alpenglow_slot = 5;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_leader_does_not_start_if_skip_certificate_missing() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        let bank_forks = create_bank_forks(&validator_keypairs);
        let my_pubkey = validator_keypairs[0].vote_keypair.pubkey();

        // Create bank 5
        let bank = create_bank(5, bank_forks.read().unwrap().get(0).unwrap(), &my_pubkey);
        bank.freeze();
        bank_forks.write().unwrap().insert(bank);

        // Notarize slot 5
        add_certificate(
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
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Notarize slot 5
        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Leader slot is just +1 from notarized slot (no skip needed)
        let my_leader_slot = 6;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_leader_starts_if_notarized_and_skips_valid() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Notarize slot 5
        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Valid skip certificate for 6-9 exists
        for slot in 6..=9 {
            add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(slot));
        }

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test]
    fn test_make_decision_leader_starts_if_skip_range_superset() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // Notarize slot 5
        add_certificate(
            &mut pool,
            &validator_keypairs,
            Vote::new_notarization_vote(5, Hash::default(), Hash::default()),
        );
        assert_eq!(pool.highest_notarized_slot(), 5);

        // Valid skip certificate for 4-9 exists
        // Should start leader block even if the beginning of the range is from
        // before your last notarized slot
        for slot in 4..=9 {
            add_certificate(
                &mut pool,
                &validator_keypairs,
                Vote::new_skip_fallback_vote(slot),
            );
        }

        let my_leader_slot = 10;
        let parent_slot = 5;
        let first_alpenglow_slot = 0;
        assert!(pool.make_start_leader_decision(my_leader_slot, parent_slot, first_alpenglow_slot));
    }

    #[test_case(Vote::new_finalization_vote(5), vec![CertificateType::Finalize])]
    #[test_case(Vote::new_notarization_vote(6, Hash::new_unique(), Hash::new_unique()), vec![CertificateType::Notarize, CertificateType::NotarizeFallback])]
    #[test_case(Vote::new_notarization_fallback_vote(7, Hash::new_unique(), Hash::new_unique()), vec![CertificateType::NotarizeFallback])]
    #[test_case(Vote::new_skip_vote(8), vec![CertificateType::Skip])]
    #[test_case(Vote::new_skip_fallback_vote(9), vec![CertificateType::Skip])]
    fn test_add_vote_and_create_new_certificate_with_types(
        vote: Vote,
        expected_certificate_types: Vec<CertificateType>,
    ) {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let my_validator_ix = 5;
        let highest_slot_fn = match &vote {
            Vote::Finalize(_) => |pool: &CertificatePool| pool.highest_finalized_slot(),
            Vote::Notarize(_) => |pool: &CertificatePool| pool.highest_notarized_slot(),
            Vote::NotarizeFallback(_) => |pool: &CertificatePool| pool.highest_notarized_slot(),
            Vote::Skip(_) => |pool: &CertificatePool| pool.highest_skip_slot(),
            Vote::SkipFallback(_) => |pool: &CertificatePool| pool.highest_skip_slot(),
        };
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, my_validator_ix),
                &mut vec![]
            )
            .is_ok());
        let slot = vote.slot();
        assert!(highest_slot_fn(&pool) < slot);
        // Same key voting again shouldn't make a certificate
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, my_validator_ix),
                &mut vec![]
            )
            .is_ok());
        assert!(highest_slot_fn(&pool) < slot);
        for rank in 0..4 {
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }
        assert!(highest_slot_fn(&pool) < slot);
        let new_validator_ix = 6;
        let (new_finalized_slot, certs_to_send) = pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, new_validator_ix),
                &mut vec![],
            )
            .unwrap();
        if vote.is_finalize() {
            assert_eq!(new_finalized_slot, Some(slot));
        } else {
            assert!(new_finalized_slot.is_none());
        }
        // Assert certs_to_send contains the expected certificate types
        for cert_type in expected_certificate_types {
            assert!(certs_to_send.iter().any(|cert| {
                cert.certificate.certificate_type == cert_type && cert.certificate.slot == slot
            }));
        }
        assert_eq!(highest_slot_fn(&pool), slot);
        // Now add the same certificate again, this should silently exit.
        for cert in certs_to_send {
            let (new_finalized_slot, certs_to_send) = pool
                .add_message(
                    &Pubkey::new_unique(),
                    &BLSMessage::Certificate((*cert).clone()),
                    &mut vec![],
                )
                .unwrap();
            assert!(new_finalized_slot.is_none());
            assert_eq!(certs_to_send, []);
        }
    }

    #[test_case(CertificateType::Finalize, Vote::new_finalization_vote(5))]
    #[test_case(
        CertificateType::FinalizeFast,
        Vote::new_notarization_vote(6, Hash::new_unique(), Hash::new_unique())
    )]
    #[test_case(
        CertificateType::Notarize,
        Vote::new_notarization_vote(6, Hash::new_unique(), Hash::new_unique())
    )]
    #[test_case(
        CertificateType::NotarizeFallback,
        Vote::new_notarization_fallback_vote(7, Hash::new_unique(), Hash::new_unique())
    )]
    #[test_case(CertificateType::Skip, Vote::new_skip_vote(8))]
    fn test_add_certificate_with_types(certificate_type: CertificateType, vote: Vote) {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let certificate = Certificate {
            slot: vote.slot(),
            certificate_type,
            block_id: vote.block_id().copied(),
            replayed_bank_hash: vote.replayed_bank_hash().copied(),
        };
        let certificate_message = CertificateMessage {
            certificate: certificate.clone(),
            signature: BLSSignature::default(),
            bitmap: BitVec::new(),
        };
        let bls_message = BLSMessage::Certificate(certificate_message.clone());
        // Add the certificate to the pool
        let (new_finalized_slot, certs_to_send) = pool
            .add_message(&Pubkey::new_unique(), &bls_message, &mut vec![])
            .unwrap();
        // Because this is the first certificate of this type, it should be sent out.
        if certificate_type == CertificateType::Finalize
            || certificate_type == CertificateType::FinalizeFast
        {
            assert_eq!(new_finalized_slot, Some(certificate.slot));
        } else {
            assert!(new_finalized_slot.is_none());
        }
        assert_eq!(certs_to_send.len(), 1);
        assert_eq!(*certs_to_send[0], certificate_message);

        // Adding the cert again will not trigger another send
        let (new_finalized_slot, certs_to_send) = pool
            .add_message(&Pubkey::new_unique(), &bls_message, &mut vec![])
            .unwrap();
        assert!(new_finalized_slot.is_none());
        assert_eq!(certs_to_send, []);

        // Now add the vote from everyone else, this will not trigger a certificate send
        for rank in 0..validator_keypairs.len() {
            let (_, certs_to_send) = pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, rank),
                    &mut vec![],
                )
                .unwrap();
            assert!(!certs_to_send
                .iter()
                .any(|cert| { cert.certificate.certificate_type == certificate_type }));
        }
    }

    #[test]
    fn test_add_vote_zero_stake() {
        let (_, mut pool) = create_keypairs_and_pool();
        assert_eq!(
            pool.add_message(
                &Pubkey::new_unique(),
                &BLSMessage::Vote(VoteMessage {
                    vote: Vote::new_skip_vote(5),
                    rank: 100,
                    signature: BLSSignature::default(),
                }),
                &mut vec![]
            ),
            Err(AddVoteError::InvalidRank(100))
        );
    }

    fn assert_single_certificate_range(
        pool: &CertificatePool,
        exp_range_start: Slot,
        exp_range_end: Slot,
    ) {
        for i in exp_range_start..=exp_range_end {
            assert!(pool.skip_certified(i));
        }
    }

    #[test]
    fn test_consecutive_slots() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        add_certificate(&mut pool, &validator_keypairs, Vote::new_skip_vote(15));
        assert_eq!(pool.highest_skip_slot(), 15);

        for i in 0..validator_keypairs.len() {
            let slot = (i as u64).saturating_add(16);
            let vote = Vote::new_skip_vote(slot);
            // These should not extend the skip range
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, i),
                    &mut vec![]
                )
                .is_ok());
        }

        assert_single_certificate_range(&pool, 15, 15);
    }

    #[test]
    fn test_multi_skip_cert() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // We have 10 validators, 40% voted for (5, 15)
        for rank in 0..4 {
            add_skip_vote_range(&mut pool, 5, 15, &validator_keypairs, rank);
        }
        // 30% voted for (5, 8)
        for rank in 4..7 {
            add_skip_vote_range(&mut pool, 5, 8, &validator_keypairs, rank);
        }
        // The rest voted for (11, 15)
        for rank in 7..10 {
            add_skip_vote_range(&mut pool, 11, 15, &validator_keypairs, rank);
        }
        // Test slots from 5 to 15, [5, 8] and [11, 15] should be certified, the others aren't
        for slot in 5..9 {
            assert!(pool.skip_certified(slot));
        }
        for slot in 9..11 {
            assert!(!pool.skip_certified(slot));
        }
        for slot in 11..=15 {
            assert!(pool.skip_certified(slot));
        }
    }

    #[test]
    fn test_add_multiple_votes() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();

        // 10 validators, half vote for (5, 15), the other (20, 30)
        for rank in 0..5 {
            add_skip_vote_range(&mut pool, 5, 15, &validator_keypairs, rank);
        }
        for rank in 5..10 {
            add_skip_vote_range(&mut pool, 20, 30, &validator_keypairs, rank);
        }
        assert_eq!(pool.highest_skip_slot(), 0);

        // Now the first half vote for (5, 30)
        for rank in 0..5 {
            add_skip_vote_range(&mut pool, 5, 30, &validator_keypairs, rank);
        }
        assert_single_certificate_range(&pool, 20, 30);
    }

    #[test]
    fn test_add_multiple_disjoint_votes() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 50% of the validators vote for (1, 10)
        for rank in 0..5 {
            add_skip_vote_range(&mut pool, 1, 10, &validator_keypairs, rank);
        }
        // 10% vote for skip 2
        let vote = Vote::new_skip_vote(2);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 6),
                &mut vec![]
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 2);

        assert_single_certificate_range(&pool, 2, 2);
        // 10% vote for skip 4
        let vote = Vote::new_skip_vote(4);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 7),
                &mut vec![]
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 4);

        assert_single_certificate_range(&pool, 2, 2);
        assert_single_certificate_range(&pool, 4, 4);
        // 10% vote for skip 3
        let vote = Vote::new_skip_vote(3);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 8),
                &mut vec![]
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 4);
        assert_single_certificate_range(&pool, 2, 4);
        assert!(pool.skip_certified(3));
        // Let the last 10% vote for (3, 10) now
        add_skip_vote_range(&mut pool, 3, 10, &validator_keypairs, 8);
        assert_eq!(pool.highest_skip_slot(), 10);
        assert_single_certificate_range(&pool, 2, 10);
        assert!(pool.skip_certified(7));
    }

    #[test]
    fn test_update_existing_singleton_vote() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 50% voted on (1, 6)
        for rank in 0..5 {
            add_skip_vote_range(&mut pool, 1, 6, &validator_keypairs, rank);
        }
        // Range expansion on a singleton vote should be ok
        let vote = Vote::new_skip_vote(1);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 6),
                &mut vec![]
            )
            .is_ok());
        assert_eq!(pool.highest_skip_slot(), 1);
        add_skip_vote_range(&mut pool, 1, 6, &validator_keypairs, 6);
        assert_eq!(pool.highest_skip_slot(), 6);
        assert_single_certificate_range(&pool, 1, 6);
    }

    #[test]
    fn test_update_existing_vote() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // 50% voted for (10, 25)
        for rank in 0..5 {
            add_skip_vote_range(&mut pool, 10, 25, &validator_keypairs, rank);
        }

        add_skip_vote_range(&mut pool, 10, 20, &validator_keypairs, 6);
        assert_eq!(pool.highest_skip_slot(), 20);
        assert_single_certificate_range(&pool, 10, 20);

        // AlreadyExists, silently fail
        let vote = Vote::new_skip_vote(20);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 6),
                &mut vec![]
            )
            .is_ok());
    }

    #[test]
    fn test_threshold_not_reached() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // half voted (5, 15) and the other half voted (20, 30)
        for rank in 0..5 {
            add_skip_vote_range(&mut pool, 5, 15, &validator_keypairs, rank);
        }
        for rank in 5..10 {
            add_skip_vote_range(&mut pool, 20, 30, &validator_keypairs, rank);
        }
        for slot in 5..31 {
            assert!(!pool.skip_certified(slot));
        }
    }

    #[test]
    fn test_update_and_skip_range_certify() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        // half voted (5, 15) and the other half voted (10, 30)
        for rank in 0..5 {
            add_skip_vote_range(&mut pool, 5, 15, &validator_keypairs, rank);
        }
        for rank in 5..10 {
            add_skip_vote_range(&mut pool, 10, 30, &validator_keypairs, rank);
        }
        for slot in 5..10 {
            assert!(!pool.skip_certified(slot));
        }
        for slot in 16..31 {
            assert!(!pool.skip_certified(slot));
        }
        assert_single_certificate_range(&pool, 10, 15);
    }

    #[test]
    fn test_safe_to_notar() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let (my_vote_key, _, _) = pool.get_key_and_stakes(0, 0).unwrap();

        // Create bank 2
        let slot = 2;
        let block_id = Hash::new_unique();

        // With no votes, this should fail.
        assert!(pool.safe_to_notar(&my_vote_key, slot).is_empty());

        // Add a skip from myself.
        let vote = Vote::new_skip_vote(2);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 0),
                &mut vec![]
            )
            .is_ok());
        // 40% notarized, should succeed
        for rank in 1..5 {
            let vote = Vote::new_notarization_vote(2, block_id, Hash::default());
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }
        assert_eq!(pool.safe_to_notar(&my_vote_key, slot), vec![block_id]);

        // Create bank 3
        let slot = 3;
        let block_id = Hash::new_unique();

        // Add 20% notarize, but no vote from myself, should fail
        for rank in 1..3 {
            let vote = Vote::new_notarization_vote(3, block_id, Hash::default());
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }
        assert!(pool.safe_to_notar(&my_vote_key, slot).is_empty());

        // Add a notarize from myself for some other block, but still not enough notar or skip, should fail.
        let vote = Vote::new_notarization_vote(3, Hash::new_unique(), Hash::default());
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 0),
                &mut vec![]
            )
            .is_ok());
        assert!(pool.safe_to_notar(&my_vote_key, slot).is_empty());

        // Now add 40% skip, should succeed
        for rank in 3..7 {
            let vote = Vote::new_skip_vote(3);
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }
        assert_eq!(pool.safe_to_notar(&my_vote_key, slot), vec![block_id]);

        // Add 20% notarization for another block, we should notify on both
        let duplicate_block_id = Hash::new_unique();
        for rank in 7..9 {
            let vote = Vote::new_notarization_vote(3, duplicate_block_id, Hash::default());
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }

        assert_eq!(
            pool.safe_to_notar(&my_vote_key, slot)
                .into_iter()
                .sorted()
                .collect::<Vec<_>>(),
            vec![block_id, duplicate_block_id,]
                .into_iter()
                .sorted()
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_safe_to_skip() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let (my_vote_key, _, _) = pool.get_key_and_stakes(0, 0).unwrap();
        let slot = 2;
        // No vote from myself, should fail.
        assert!(!pool.safe_to_skip(&my_vote_key, slot));

        // Add a notarize from myself.
        let block_id = Hash::new_unique();
        let bank_hash = Hash::default();
        let vote = Vote::new_notarization_vote(2, block_id, bank_hash);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 0),
                &mut vec![]
            )
            .is_ok());
        // Should still fail because there are no other votes.
        assert!(!pool.safe_to_skip(&my_vote_key, slot));
        // Add 50% skip, should succeed
        for rank in 1..6 {
            let vote = Vote::new_skip_vote(2);
            assert!(pool
                .add_message(
                    &Pubkey::new_unique(),
                    &dummy_transaction(&validator_keypairs, &vote, rank),
                    &mut vec![]
                )
                .is_ok());
        }
        assert!(pool.safe_to_skip(&my_vote_key, slot));
        // Add 10% more notarize, still safe to skip any more because total voted increased.
        let vote = Vote::new_notarization_vote(2, block_id, bank_hash);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 6),
                &mut vec![]
            )
            .is_ok());
        assert!(pool.safe_to_skip(&my_vote_key, slot));
    }

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
        validator_keypairs: &[ValidatorVoteKeypairs],
        vote_type_1: VoteType,
        vote_type_2: VoteType,
        slot: Slot,
    ) {
        let vote_1 = create_new_vote(vote_type_1, slot);
        let vote_2 = create_new_vote(vote_type_2, slot);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(validator_keypairs, &vote_1, 0),
                &mut vec![]
            )
            .is_ok());
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(validator_keypairs, &vote_2, 0),
                &mut vec![]
            )
            .is_err());
    }

    #[test]
    fn test_reject_conflicting_votes_with_type() {
        let (validator_keypairs, mut pool) = create_keypairs_and_pool();
        let mut slot = 2;
        for vote_type_1 in [
            VoteType::Finalize,
            VoteType::Notarize,
            VoteType::NotarizeFallback,
            VoteType::Skip,
            VoteType::SkipFallback,
        ] {
            let conflicting_vote_types = conflicting_types(vote_type_1);
            for vote_type_2 in conflicting_vote_types {
                test_reject_conflicting_vote(
                    &mut pool,
                    &validator_keypairs,
                    vote_type_1,
                    *vote_type_2,
                    slot,
                );
            }
            slot = slot.saturating_add(4);
        }
    }

    #[test]
    fn test_handle_new_root() {
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let bank_forks = create_bank_forks(&validator_keypairs);
        let root_bank = bank_forks.read().unwrap().root_bank();
        let mut pool: CertificatePool =
            CertificatePool::new_from_root_bank(Pubkey::new_unique(), &root_bank.clone(), None);
        assert_eq!(pool.root(), 0);

        let new_bank = Arc::new(create_bank(2, root_bank, &Pubkey::new_unique()));
        pool.handle_new_root(new_bank.clone());
        assert_eq!(pool.root(), 2);
        let new_bank = Arc::new(create_bank(3, new_bank, &Pubkey::new_unique()));
        pool.handle_new_root(new_bank);
        assert_eq!(pool.root(), 3);
        // Send a vote on slot 1, it should be rejected
        let vote = Vote::new_skip_vote(1);
        assert!(pool
            .add_message(
                &Pubkey::new_unique(),
                &dummy_transaction(&validator_keypairs, &vote, 0),
                &mut vec![]
            )
            .is_err());
        // Send a cert on slot 2, it should be rejected
        let cert = BLSMessage::Certificate(CertificateMessage {
            certificate: Certificate {
                slot: 2,
                certificate_type: CertificateType::Notarize,
                block_id: Some(Hash::new_unique()),
                replayed_bank_hash: Some(Hash::new_unique()),
            },
            signature: BLSSignature::default(),
            bitmap: BitVec::new(),
        });
        assert!(pool
            .add_message(&Pubkey::new_unique(), &cert, &mut vec![])
            .is_err());
    }
}
