use {
    crate::{bank::Bank, validated_block_finalization::ValidatedBlockFinalizationCert},
    agave_votor_messages::{
        consensus_message::{Certificate, ConsensusMessage},
        fraction::Fraction,
        migration::{GENESIS_VOTE_THRESHOLD, MigrationStatus},
    },
    crossbeam_channel::Sender,
    log::{info, warn},
    solana_clock::{DEFAULT_MS_PER_SLOT, Slot},
    solana_entry::block_component::{
        BlockFooterV1, BlockMarkerV1, GenesisCertificate, VersionedBlockFooter,
        VersionedBlockHeader, VersionedBlockMarker, VersionedUpdateParent,
    },
    std::{num::NonZeroU64, sync::Arc},
    thiserror::Error,
    vote_reward::calculate_and_pay_voting_reward,
};

pub(crate) mod vote_reward;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BlockComponentProcessorError {
    #[error("Missing block footer")]
    MissingBlockFooter,
    #[error("Missing parent marker (neither a header nor an update parent was present)")]
    MissingParentMarker,
    #[error("Multiple block footers detected")]
    MultipleBlockFooters,
    #[error("Multiple block headers detected")]
    MultipleBlockHeaders,
    #[error("Multiple update parents detected")]
    MultipleUpdateParents,
    #[error("BlockComponent detected pre-migration")]
    BlockComponentPreMigration,
    #[error("GenesisCertificate marker detected when GenesisCertificate is already populated")]
    GenesisCertificateAlreadyPopulated,
    #[error("GenesisCertificate marker detected when the cluster has Alpenglow enabled at slot 0")]
    GenesisCertificateInAlpenglowCluster,
    #[error("GenesisCertificate marker detected on a block which is not a child of genesis")]
    GenesisCertificateOnNonChild,
    #[error("GenesisCertificate was invalid and failed to verify")]
    GenesisCertificateFailedVerification,
    #[error("Finalization certificate was invalid and failed to verify")]
    InvalidFinalizationCertificate,
    #[error("Nanosecond clock out of bounds")]
    NanosecondClockOutOfBounds,
    #[error("Spurious update parent")]
    SpuriousUpdateParent,
    #[error("Abandoned bank")]
    AbandonedBank(VersionedUpdateParent),
}

#[derive(Default)]
pub struct BlockComponentProcessor {
    has_header: bool,
    has_footer: bool,
    update_parent: Option<VersionedUpdateParent>,
}

impl BlockComponentProcessor {
    pub fn on_final(
        &self,
        migration_status: &MigrationStatus,
        slot: Slot,
    ) -> Result<(), BlockComponentProcessorError> {
        // Only require block markers (header/footer) for slots where they should be present.
        if !migration_status.should_allow_block_markers(slot) {
            return Ok(());
        }

        // If we encounter an UpdateParent when fast leader handover is disabled, error.
        if !migration_status.should_allow_fast_leader_handover(slot) && self.update_parent.is_some()
        {
            return Err(BlockComponentProcessorError::SpuriousUpdateParent);
        }

        // Post-migration: both header and footer are required.
        if !self.has_footer {
            return Err(BlockComponentProcessorError::MissingBlockFooter);
        }

        if !self.has_header && self.update_parent.is_none() {
            return Err(BlockComponentProcessorError::MissingParentMarker);
        }

        Ok(())
    }

    pub fn on_entry_batch(
        &mut self,
        migration_status: &MigrationStatus,
    ) -> Result<(), BlockComponentProcessorError> {
        if !migration_status.is_alpenglow_enabled() {
            return Ok(());
        }

        // We must have either a header or an update parent prior to processing entry batches.
        if !self.has_header && self.update_parent.is_none() {
            return Err(BlockComponentProcessorError::MissingParentMarker);
        }

        Ok(())
    }

    pub fn on_marker(
        &mut self,
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        marker: &VersionedBlockMarker,
        finalization_cert_sender: Option<&Sender<Vec<ConsensusMessage>>>,
        migration_status: &MigrationStatus,
    ) -> Result<(), BlockComponentProcessorError> {
        // Pre-migration: blocks with block components should be marked as dead.
        if !migration_status.is_alpenglow_enabled() {
            return Err(BlockComponentProcessorError::BlockComponentPreMigration);
        }

        let VersionedBlockMarker::V1(marker) = marker;

        match marker {
            BlockMarkerV1::BlockFooter(footer) => {
                self.on_footer(bank, parent_bank, footer.inner(), finalization_cert_sender)
            }
            BlockMarkerV1::BlockHeader(header) => self.on_header(header.inner()),
            BlockMarkerV1::UpdateParent(update_parent) => {
                self.on_update_parent(update_parent.inner())
            }
            // We process GenesisCertificate messages elsewhere, so no callback needed here.
            BlockMarkerV1::GenesisCertificate(_) => Ok(()),
        }
    }

    pub fn on_genesis_certificate(
        &self,
        bank: Arc<Bank>,
        genesis_cert: GenesisCertificate,
        migration_status: &MigrationStatus,
    ) -> Result<(), BlockComponentProcessorError> {
        if bank.parent_slot() == 0 {
            return Err(BlockComponentProcessorError::GenesisCertificateInAlpenglowCluster);
        }

        let parent_block_id = bank
            .parent_block_id()
            .expect("Block id is populated for all slots > 0");
        if (bank.parent_slot(), parent_block_id) != (genesis_cert.slot, genesis_cert.block_id) {
            return Err(BlockComponentProcessorError::GenesisCertificateOnNonChild);
        }

        if bank.get_alpenglow_genesis_certificate().is_some() {
            return Err(BlockComponentProcessorError::GenesisCertificateAlreadyPopulated);
        }

        let genesis_cert = Certificate::from(genesis_cert);
        Self::verify_genesis_certificate(&bank, &genesis_cert)?;
        bank.set_alpenglow_genesis_certificate(&genesis_cert);

        if migration_status.is_alpenglow_enabled() {
            // We participated in the migration, nothing to do.
            return Ok(());
        }

        // We ingested the first alpenglow block while replaying, so store migration state to
        // let replay transition us to ReadyToEnable.
        warn!(
            "{}: Alpenglow genesis marker processed during replay of {}. Transitioning Alpenglow \
             to ReadyToEnable",
            migration_status.my_pubkey(),
            bank.slot()
        );
        migration_status.set_genesis_block(
            genesis_cert
                .cert_type
                .to_block()
                .expect("Genesis cert must correspond to a block"),
        );
        migration_status.set_genesis_certificate(Arc::new(genesis_cert));
        assert!(migration_status.is_ready_to_enable());

        Ok(())
    }

    fn verify_genesis_certificate(
        bank: &Bank,
        cert: &Certificate,
    ) -> Result<(), BlockComponentProcessorError> {
        let slot = cert.cert_type.slot();
        let (genesis_stake, total_stake) = bank.verify_certificate(cert).map_err(|_| {
            warn!(
                "Failed to verify genesis certificate for {slot} in bank slot {}",
                bank.slot()
            );
            BlockComponentProcessorError::GenesisCertificateFailedVerification
        })?;
        let total_stake = NonZeroU64::new(total_stake)
            .ok_or(BlockComponentProcessorError::GenesisCertificateFailedVerification)?;
        let genesis_percent = Fraction::new(genesis_stake, total_stake);
        if genesis_percent < GENESIS_VOTE_THRESHOLD {
            warn!(
                "Received a genesis certificate for {slot} in bank slot {} with {genesis_percent} \
                 stake < {GENESIS_VOTE_THRESHOLD}",
                bank.slot()
            );
            return Err(BlockComponentProcessorError::GenesisCertificateFailedVerification);
        }

        Ok(())
    }

    fn on_footer(
        &mut self,
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        footer: &VersionedBlockFooter,
        finalization_cert_sender: Option<&Sender<Vec<ConsensusMessage>>>,
    ) -> Result<(), BlockComponentProcessorError> {
        if !self.has_header && self.update_parent.is_none() {
            return Err(BlockComponentProcessorError::MissingParentMarker);
        }

        if self.has_footer {
            return Err(BlockComponentProcessorError::MultipleBlockFooters);
        }

        let VersionedBlockFooter::V1(footer) = footer;

        Self::enforce_nanosecond_clock_bounds(bank.clone(), parent_bank, footer)?;
        Self::update_bank_with_footer(bank.clone(), footer);

        if let Some(final_cert) = footer.final_cert.clone() {
            let validated = ValidatedBlockFinalizationCert::try_from_footer(final_cert, &bank)
                .map_err(|e| {
                    warn!(
                        "Failed to validate finalization certificate for bank {}: {e}",
                        bank.slot()
                    );
                    BlockComponentProcessorError::InvalidFinalizationCertificate
                })?;

            if let Some(sender) = finalization_cert_sender {
                let (finalize_cert, notarize_cert) = validated.into_certificates();
                let mut certs = Vec::with_capacity(if notarize_cert.is_some() { 2 } else { 1 });
                if let Some(notarize_cert) = notarize_cert {
                    certs.push(ConsensusMessage::Certificate(notarize_cert));
                }
                certs.push(ConsensusMessage::Certificate(finalize_cert));
                if sender.send(certs).is_err() {
                    info!("Finalization certificate sender disconnected");
                }
            }
        }

        self.has_footer = true;
        Ok(())
    }

    fn on_header(
        &mut self,
        _header: &VersionedBlockHeader,
    ) -> Result<(), BlockComponentProcessorError> {
        if self.has_header {
            return Err(BlockComponentProcessorError::MultipleBlockHeaders);
        }

        if self.update_parent.is_some() {
            return Err(BlockComponentProcessorError::SpuriousUpdateParent);
        }

        self.has_header = true;
        Ok(())
    }

    fn on_update_parent(
        &mut self,
        update_parent: &VersionedUpdateParent,
    ) -> Result<(), BlockComponentProcessorError> {
        if self.update_parent.is_some() {
            return Err(BlockComponentProcessorError::MultipleUpdateParents);
        }

        self.update_parent = Some(update_parent.clone());

        if self.has_header {
            Err(BlockComponentProcessorError::AbandonedBank(
                update_parent.clone(),
            ))
        } else {
            Ok(())
        }
    }

    fn enforce_nanosecond_clock_bounds(
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        footer: &BlockFooterV1,
    ) -> Result<(), BlockComponentProcessorError> {
        // If nanosecond clock hasn't been populated, don't enforce bounds yet.
        let Some(parent_time_nanos) = parent_bank.get_nanosecond_clock() else {
            return Ok(());
        };

        let parent_slot = parent_bank.slot();
        let current_time_nanos =
            i64::try_from(footer.block_producer_time_nanos).unwrap_or(i64::MAX);
        let current_slot = bank.slot();

        let (lower_bound_nanos, upper_bound_nanos) =
            Self::nanosecond_time_bounds(parent_slot, parent_time_nanos, current_slot);

        if lower_bound_nanos <= current_time_nanos && current_time_nanos <= upper_bound_nanos {
            Ok(())
        } else {
            Err(BlockComponentProcessorError::NanosecondClockOutOfBounds)
        }
    }

    /// Given the parent slot, parent time, and slot, calculate the inclusive
    /// bounds for the block producer timestamp.
    pub fn nanosecond_time_bounds(
        parent_slot: Slot,
        parent_time_nanos: i64,
        slot: Slot,
    ) -> (i64, i64) {
        let default_ns_per_slot = i64::try_from(DEFAULT_MS_PER_SLOT)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000_000);
        let diff_slots = i64::try_from(slot.saturating_sub(parent_slot)).unwrap_or(i64::MAX);

        let min_working_bank_time = parent_time_nanos.saturating_add(1);
        let max_working_bank_time = parent_time_nanos.saturating_add(
            diff_slots
                .saturating_mul(2)
                .saturating_mul(default_ns_per_slot),
        );

        (min_working_bank_time, max_working_bank_time)
    }

    pub fn update_bank_with_footer(bank: Arc<Bank>, footer: &BlockFooterV1) {
        // Update clock sysvar from footer timestamp.
        let unix_timestamp_nanos =
            i64::try_from(footer.block_producer_time_nanos).unwrap_or(i64::MAX);
        bank.update_clock_from_footer(unix_timestamp_nanos);
        calculate_and_pay_voting_reward(&bank, None).unwrap();
        // Record expected bank hash from footer for later verification when the bank is frozen.
        bank.set_expected_bank_hash(footer.bank_hash);
    }
}
