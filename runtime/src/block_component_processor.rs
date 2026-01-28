use {
    crate::{
        bank::Bank,
        validated_block_finalization::ValidatedBlockFinalizationCert,
        validated_reward_certificate::{Error as ValidatedRewardCertError, ValidatedRewardCert},
    },
    crossbeam_channel::Sender,
    log::*,
    solana_clock::{Slot, DEFAULT_MS_PER_SLOT},
    solana_entry::block_component::{
        BlockFooterV1, BlockMarkerV1, GenesisCertificate, VersionedBlockFooter,
        VersionedBlockHeader, VersionedBlockMarker, VersionedUpdateParent,
    },
    solana_pubkey::Pubkey,
    solana_votor_messages::{
        consensus_message::{Certificate, ConsensusMessage},
        fraction::Fraction,
        migration::{MigrationStatus, GENESIS_VOTE_THRESHOLD},
    },
    std::{num::NonZeroU64, sync::Arc},
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BlockComponentProcessorError {
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
    #[error("FinalizationCertificate was invalid or failed to verify")]
    InvalidFinalizationCertificate,
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
    #[error("Nanosecond clock out of bounds")]
    NanosecondClockOutOfBounds,
    #[error("Spurious update parent")]
    SpuriousUpdateParent,
    #[error("Abandoned bank")]
    AbandonedBank(VersionedUpdateParent),
    #[error("invalid reward certs {0}")]
    InvalidRewardCerts(#[from] ValidatedRewardCertError),
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
        // Only require block markers (header/footer) for slots where they should be present
        if !migration_status.should_allow_block_markers(slot) {
            return Ok(());
        }

        // If we encounter an UpdateParent when fast leader handover is disabled, error.
        if !migration_status.should_allow_fast_leader_handover(slot) && self.update_parent.is_some()
        {
            return Err(BlockComponentProcessorError::SpuriousUpdateParent);
        }

        // Post-migration: both header and footer are required
        if !self.has_footer {
            return Err(BlockComponentProcessorError::MissingBlockFooter);
        }

        if !self.has_header && self.update_parent.is_none() {
            return Err(BlockComponentProcessorError::MissingParentMarker);
        }

        Ok(())
    }

    /// Process an entry batch.
    ///
    /// Validates that a parent marker (header or update parent) has been
    /// processed before any entry batches.
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

    /// Process a block marker:
    /// - Pre migration, no block markers are allowed
    /// - During the migration only header and genesis certificate are allowed:
    ///     - This is in case our node was slow in observing the completion of the migration
    ///     - By seeing the first alpenglow block, we can advance the migration phase
    /// - Once the migration is complete all markers are allowed
    pub fn on_marker(
        &mut self,
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        marker: VersionedBlockMarker,
        finalization_cert_sender: Option<&Sender<ConsensusMessage>>,
        migration_status: &MigrationStatus,
    ) -> Result<(), BlockComponentProcessorError> {
        let slot = bank.slot();
        let VersionedBlockMarker::V1(marker) = marker;

        let markers_fully_enabled = migration_status.should_allow_block_markers(slot);
        let in_migration = migration_status.is_in_migration();

        match marker {
            // Header and genesis cert can be processed either:
            // - once migration is fully enabled, or
            // - while we're still in the migration phase (to let us advance it)
            BlockMarkerV1::BlockHeader(header) if markers_fully_enabled || in_migration => {
                self.on_header(header.inner())
            }
            BlockMarkerV1::GenesisCertificate(genesis_cert)
                if markers_fully_enabled || in_migration =>
            {
                self.on_genesis_certificate(bank, genesis_cert.into_inner(), migration_status)
            }

            // Everything else is only valid once migration is complete
            BlockMarkerV1::BlockFooter(footer) => self.on_footer(
                bank,
                parent_bank,
                footer.into_inner(),
                finalization_cert_sender,
            ),

            BlockMarkerV1::UpdateParent(update_parent) => {
                self.on_update_parent(update_parent.inner())
            }

            // Any other combination means we saw a marker too early
            _ => Err(BlockComponentProcessorError::BlockComponentPreMigration),
        }
    }

    pub fn on_genesis_certificate(
        &self,
        bank: Arc<Bank>,
        genesis_cert: GenesisCertificate,
        migration_status: &MigrationStatus,
    ) -> Result<(), BlockComponentProcessorError> {
        // Genesis Certificate is only allowed for direct child of genesis
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
            // We participated in the migration, nothing to do
            return Ok(());
        }

        // We missed the migration however we ingested the first alpenglow block.
        // This is either a result of startup replay, or in some weird cases steady state replay after a network partition.
        // Either way we ingest the genesis block details moving us to `ReadyToEnable`.
        // Since this is a direct child of genesis, and we are replaying, we know we have frozen the genesis block.
        // Then `load_frozen_forks` or `replay_stage` will take care of the rest.
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
        debug_assert!(cert.cert_type.is_genesis());

        let cert_slot = cert.cert_type.slot();
        let (genesis_stake, total_stake) = bank.verify_certificate(cert).map_err(|_| {
            warn!(
                "Failed to verify genesis certificate for slot {cert_slot} in bank slot {}",
                bank.slot()
            );
            BlockComponentProcessorError::GenesisCertificateFailedVerification
        })?;

        let genesis_percent = Fraction::new(genesis_stake, NonZeroU64::new(total_stake).unwrap());
        if genesis_percent < GENESIS_VOTE_THRESHOLD {
            warn!(
                "Received a genesis certificate for slot {cert_slot} in bank slot {} with \
                 {genesis_percent} stake < {GENESIS_VOTE_THRESHOLD}",
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
        footer: VersionedBlockFooter,
        finalization_cert_sender: Option<&Sender<ConsensusMessage>>,
    ) -> Result<(), BlockComponentProcessorError> {
        if !self.has_header && self.update_parent.is_none() {
            return Err(BlockComponentProcessorError::MissingParentMarker);
        }

        if self.has_footer {
            return Err(BlockComponentProcessorError::MultipleBlockFooters);
        }

        let VersionedBlockFooter::V1(footer) = footer;

        Self::enforce_nanosecond_clock_bounds(&bank, &parent_bank, &footer)?;

        let reward_slot_and_validators = match ValidatedRewardCert::try_new(
            &bank,
            &footer.skip_reward_cert,
            &footer.notar_reward_cert,
        ) {
            Ok(c) => Some(c.into_parts()),
            Err(ValidatedRewardCertError::Empty) => None,
            Err(e) => return Err(e.into()),
        };
        Self::update_bank_with_footer(&bank, &footer, reward_slot_and_validators);

        // Verify finalization certificate and send to consensus pool
        if let Some(final_cert) = footer.final_cert {
            let validated = ValidatedBlockFinalizationCert::try_from_footer(final_cert, &bank)
                .map_err(|e| {
                    warn!("Failed to validate finalization certificate: {e}");
                    BlockComponentProcessorError::InvalidFinalizationCertificate
                })?;

            if let Some(sender) = finalization_cert_sender {
                let (finalize_cert, notarize_cert) = validated.into_certificates();
                if let Some(notarize_cert) = notarize_cert {
                    let _ = sender
                        .send(ConsensusMessage::from(notarize_cert))
                        .inspect_err(|_| info!("ConsensusMessage sender disconnected"));
                }
                let _ = sender
                    .send(ConsensusMessage::from(finalize_cert))
                    .inspect_err(|_| info!("ConsensusMessage sender disconnected"));
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
        bank: &Bank,
        parent_bank: &Bank,
        footer: &BlockFooterV1,
    ) -> Result<(), BlockComponentProcessorError> {
        // Get parent time from nanosecond clock account
        // If nanosecond clock hasn't been populated, don't enforce the bounds; note that the
        // nanosecond clock is populated as soon as Alpenglow migration is complete.
        let Some(parent_time_nanos) = parent_bank.get_nanosecond_clock() else {
            return Ok(());
        };

        let parent_slot = parent_bank.slot();
        let current_time_nanos = footer.block_producer_time_nanos as i64;
        let current_slot = bank.slot();

        let (lower_bound_nanos, upper_bound_nanos) =
            Self::nanosecond_time_bounds(parent_slot, parent_time_nanos, current_slot);

        let is_valid =
            lower_bound_nanos <= current_time_nanos && current_time_nanos <= upper_bound_nanos;

        match is_valid {
            true => Ok(()),
            false => Err(BlockComponentProcessorError::NanosecondClockOutOfBounds),
        }
    }

    /// Given the parent slot, parent time, and slot, calculate the lower and upper
    /// bounds for the block producer time. We return (lower_bound, upper_bound), where both bounds
    /// are inclusive. I.e., the working bank time is valid if
    /// lower_bound <= working_bank_time <= upper_bound.
    ///
    /// Refer to https://github.com/solana-foundation/solana-improvement-documents/pull/363 for
    /// details on the bounds calculation.
    pub fn nanosecond_time_bounds(
        parent_slot: Slot,
        parent_time_nanos: i64,
        slot: Slot,
    ) -> (i64, i64) {
        let default_ns_per_slot = DEFAULT_MS_PER_SLOT * 1_000_000;
        let diff_slots = slot.saturating_sub(parent_slot);

        let min_working_bank_time = parent_time_nanos.saturating_add(1);
        let max_working_bank_time =
            parent_time_nanos.saturating_add((2 * diff_slots * default_ns_per_slot) as i64);

        (min_working_bank_time, max_working_bank_time)
    }

    pub fn update_bank_with_footer(
        bank: &Bank,
        footer: &BlockFooterV1,
        _reward_slot_and_validators: Option<(Slot, Vec<Pubkey>)>,
    ) {
        // Update clock sysvar
        bank.update_clock_from_footer(footer.block_producer_time_nanos as i64);

        // Record expected bank hash from footer for later verification when the bank is frozen.
        bank.set_expected_bank_hash(footer.bank_hash);

        // TODO: rewards
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            bank::Bank,
            genesis_utils::{
                create_genesis_config, create_genesis_config_with_alpenglow_vote_accounts,
                ValidatorVoteKeypairs,
            },
            validated_block_finalization::BlockFinalizationCertError,
        },
        bitvec::prelude::*,
        solana_bls_signatures::SignatureProjective,
        solana_entry::block_component::{
            BlockFooterV1, BlockHeaderV1, FinalCertificate, UpdateParentV1, VersionedUpdateParent,
            VotesAggregate,
        },
        solana_program::{hash::Hash, pubkey::Pubkey},
        solana_signer_store::encode_base2,
        solana_votor_messages::{consensus_message::CertificateType, vote::Vote},
        std::sync::Arc,
    };

    fn create_test_bank() -> Arc<Bank> {
        let genesis_config_info = create_genesis_config(10_000);
        Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config))
    }

    fn create_child_bank(parent: &Arc<Bank>, slot: u64) -> Arc<Bank> {
        Arc::new(Bank::new_from_parent(
            parent.clone(),
            &Pubkey::new_unique(),
            slot,
        ))
    }

    #[test]
    fn test_missing_header_error_on_entry_batch() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor::default();

        // Try to process entry batch without header - should fail
        let result = processor.on_entry_batch(&migration_status);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::MissingParentMarker)
        );
    }

    #[test]
    fn test_missing_footer_error_on_slot_full() {
        let migration_status = MigrationStatus::post_migration_status();
        let processor = BlockComponentProcessor {
            has_header: true,
            ..BlockComponentProcessor::default()
        };

        // Try to mark slot as full without footer - should fail
        let result = processor.on_final(&migration_status, 1);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::MissingBlockFooter)
        );
    }

    #[test]
    fn test_multiple_headers_error() {
        let mut processor = BlockComponentProcessor::default();
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        // First header should succeed
        assert!(processor.on_header(&header).is_ok());

        // Second header should fail
        let result = processor.on_header(&header);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::MultipleBlockHeaders)
        );
    }

    #[test]
    fn test_multiple_footers_error() {
        let mut processor = BlockComponentProcessor {
            has_header: true,
            ..Default::default()
        };

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Calculate valid timestamp based on parent's time
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer_time_nanos = parent_time_nanos + 400_000_000; // parent + 400ms

        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });

        // First footer should succeed
        assert!(processor
            .on_footer(bank.clone(), parent.clone(), footer.clone(), None)
            .is_ok());

        // Second footer should fail
        let result = processor.on_footer(bank, parent, footer, None);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::MultipleBlockFooters)
        );
    }

    #[test]
    fn test_on_footer_sets_timestamp() {
        let mut processor = BlockComponentProcessor {
            has_header: true,
            ..Default::default()
        };

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Calculate valid timestamp based on parent's time
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer_time_nanos = parent_time_nanos + 200_000_000; // parent + 200ms
        let expected_time_secs = footer_time_nanos / 1_000_000_000;

        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });

        processor
            .on_footer(bank.clone(), parent, footer, None)
            .unwrap();

        assert!(processor.has_footer);

        // Verify clock sysvar was updated with correct timestamp (nanos converted to seconds)
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);
    }

    #[test]
    fn test_on_header_sets_flag() {
        let mut processor = BlockComponentProcessor::default();
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        processor.on_header(&header).unwrap();
        assert!(processor.has_header);
    }

    #[test]
    fn test_on_marker_processes_header() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor::default();
        let marker = VersionedBlockMarker::new_block_header(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        processor
            .on_marker(bank, parent, marker, None, &migration_status)
            .unwrap();
        assert!(processor.has_header);
    }

    #[test]
    fn test_on_marker_processes_footer() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor {
            has_header: true,
            ..Default::default()
        };

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Calculate valid timestamp based on parent's time
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer_time_nanos = parent_time_nanos + 300_000_000; // parent + 300ms
        let expected_time_secs = footer_time_nanos / 1_000_000_000;

        let marker = VersionedBlockMarker::new_block_footer(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });

        processor
            .on_marker(bank.clone(), parent, marker, None, &migration_status)
            .unwrap();
        assert!(processor.has_footer);

        // Verify clock sysvar was updated
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);
    }

    #[test]
    fn test_complete_workflow_success() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Calculate valid timestamp based on parent's time
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer_time_nanos = parent_time_nanos + 100_000_000; // parent + 100ms
        let expected_time_secs = footer_time_nanos / 1_000_000_000;

        // Process header
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });
        processor.on_header(&header).unwrap();

        // Process some entry batches (not full yet)
        assert!(processor.on_entry_batch(&migration_status).is_ok());

        // Process footer with valid timestamp
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });
        processor
            .on_footer(bank.clone(), parent.clone(), footer, None)
            .unwrap();

        // Verify clock sysvar was updated
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);

        // Entry batch after footer should still succeed
        let result = processor.on_entry_batch(&migration_status);
        assert!(result.is_ok());
    }

    #[test]
    fn test_block_marker_detected_pre_migration() {
        let migration_status = MigrationStatus::default();
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Try to process a block header marker pre-migration - should fail
        let marker = VersionedBlockMarker::new_block_header(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        let result = processor.on_marker(bank, parent, marker, None, &migration_status);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::BlockComponentPreMigration)
        );
    }

    #[test]
    fn test_entry_batch_pre_migration_succeeds() {
        let migration_status = MigrationStatus::default();
        let mut processor = BlockComponentProcessor::default();

        // Processing entry batches pre-migration (without markers) should succeed
        let result = processor.on_entry_batch(&migration_status);
        assert!(result.is_ok());

        // Even with slot full
        let result = processor.on_entry_batch(&migration_status);
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_workflow_post_migration() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Process header marker
        let header_marker = VersionedBlockMarker::new_block_header(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });
        processor
            .on_marker(
                bank.clone(),
                parent.clone(),
                header_marker,
                None,
                &migration_status,
            )
            .unwrap();

        // Process entry batches
        assert!(processor.on_entry_batch(&migration_status).is_ok());

        // Calculate valid timestamp based on parent's time
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer_time_nanos = parent_time_nanos + 500_000_000; // parent + 500ms
        let expected_time_secs = footer_time_nanos / 1_000_000_000;

        // Process footer marker
        let footer_marker = VersionedBlockMarker::new_block_footer(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });
        processor
            .on_marker(bank.clone(), parent, footer_marker, None, &migration_status)
            .unwrap();

        // Verify clock sysvar was updated
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);

        // Entry batch after footer should still succeed
        let result = processor.on_entry_batch(&migration_status);
        assert!(result.is_ok());
    }

    #[test]
    fn test_footer_without_header_errors() {
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1_000_000_000,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });

        // Try to process footer without header - should fail
        let result = processor.on_footer(bank, parent, footer, None);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::MissingParentMarker)
        );
    }

    #[test]
    fn test_marker_with_footer_at_slot_full() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Process header first
        processor.has_header = true;

        // Calculate valid timestamp based on parent's time
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer_time_nanos = parent_time_nanos + 600_000_000; // parent + 600ms
        let expected_time_secs = footer_time_nanos / 1_000_000_000;

        // Process footer marker
        let footer_marker = VersionedBlockMarker::new_block_footer(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });

        // Should succeed - footer is processed
        let result =
            processor.on_marker(bank.clone(), parent, footer_marker, None, &migration_status);
        assert!(result.is_ok());
        assert!(processor.has_footer);

        // Verify clock sysvar was updated
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);
    }

    #[test]
    fn test_entry_batch_with_header_not_full_succeeds() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor {
            has_header: true,
            ..Default::default()
        };

        // Process entry batch with header but not full - should succeed even without footer
        let result = processor.on_entry_batch(&migration_status);
        assert!(result.is_ok());
    }

    #[test]
    fn test_footer_sets_epoch_start_timestamp_on_epoch_change() {
        let mut processor = BlockComponentProcessor {
            has_header: true,
            ..Default::default()
        };

        // Create genesis bank
        let genesis_config_info = create_genesis_config(10_000);
        let genesis_bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        // Get epoch schedule to find first slot of next epoch
        let epoch_schedule = genesis_bank.epoch_schedule();
        let first_slot_in_epoch_1 = epoch_schedule.get_first_slot_in_epoch(1);

        // Create parent bank at last slot of epoch 0
        let mut parent = genesis_bank.clone();
        for slot in 1..first_slot_in_epoch_1 {
            parent = create_child_bank(&parent, slot);
        }

        // Create bank at first slot of epoch 1
        let bank = create_child_bank(&parent, first_slot_in_epoch_1);

        // Verify we're in epoch 1
        assert_eq!(bank.epoch(), 1);

        // Calculate valid timestamp based on parent's time
        let parent_slot = parent.slot();
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let current_slot = bank.slot();

        // Use a timestamp in the middle of the valid range
        let (lower_bound, upper_bound) = BlockComponentProcessor::nanosecond_time_bounds(
            parent_slot,
            parent_time_nanos,
            current_slot,
        );
        let footer_time_nanos = (lower_bound + upper_bound) / 2;
        let expected_time_secs = footer_time_nanos / 1_000_000_000;

        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });

        processor
            .on_footer(bank.clone(), parent, footer, None)
            .unwrap();

        // Verify clock sysvar was updated
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);

        // Verify epoch_start_timestamp was set correctly for the new epoch
        assert_eq!(bank.clock().epoch_start_timestamp, expected_time_secs);
    }

    // Helper function to test clock bounds enforcement
    fn test_clock_bounds_helper(
        slot_gap: u64,
        timestamp_fn: impl FnOnce(i64, i64, i64) -> i64,
        should_pass: bool,
    ) {
        let mut processor = BlockComponentProcessor {
            has_header: true,
            ..Default::default()
        };

        let parent = create_test_bank();
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);

        // Set up clock on parent so validation doesn't skip bounds checking
        parent.update_clock_from_footer(parent_time_nanos);

        let bank = create_child_bank(&parent, slot_gap);

        let (lower_bound, upper_bound) =
            BlockComponentProcessor::nanosecond_time_bounds(0, parent_time_nanos, slot_gap);

        let footer_time_nanos = timestamp_fn(parent_time_nanos, lower_bound, upper_bound);

        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });

        let result = processor.on_footer(bank, parent, footer, None);
        if should_pass {
            assert!(result.is_ok());
        } else {
            assert_eq!(
                result,
                Err(BlockComponentProcessorError::NanosecondClockOutOfBounds)
            );
        }
    }

    #[test]
    fn test_clock_bounds_at_minimum() {
        test_clock_bounds_helper(1, |_, lower, _| lower, true);
    }

    #[test]
    fn test_clock_bounds_at_maximum() {
        test_clock_bounds_helper(1, |_, _, upper| upper, true);
    }

    #[test]
    fn test_clock_bounds_below_minimum() {
        test_clock_bounds_helper(1, |_, lower, _| lower - 1, false);
    }

    #[test]
    fn test_clock_bounds_above_maximum() {
        test_clock_bounds_helper(1, |_, _, upper| upper + 1, false);
    }

    #[test]
    fn test_clock_bounds_multi_slot_gap() {
        // For 5 slots: upper_bound = parent_time + 2 * 5 * 400ms = parent_time + 4000ms
        // Use 2 seconds which is within bounds
        test_clock_bounds_helper(5, |_, lower, _| lower + 2_000_000_000, true);
    }

    #[test]
    fn test_clock_bounds_multi_slot_gap_exceeds() {
        // Exceed by 1 second beyond the upper bound
        test_clock_bounds_helper(5, |_, _, upper| upper + 1_000_000_000, false);
    }

    #[test]
    fn test_clock_bounds_timestamp_equals_parent() {
        // Timestamp equal to parent time (should fail, must be strictly greater)
        test_clock_bounds_helper(1, |parent_time, _, _| parent_time, false);
    }

    // Helper function to test nanosecond_time_bounds calculation
    fn test_nanosecond_time_bounds_helper(
        parent_slot: u64,
        parent_time_nanos: i64,
        working_slot: u64,
        expected_lower: i64,
        expected_upper: i64,
    ) {
        let (lower, upper) = BlockComponentProcessor::nanosecond_time_bounds(
            parent_slot,
            parent_time_nanos,
            working_slot,
        );

        assert_eq!(lower, expected_lower);
        assert_eq!(upper, expected_upper);
    }

    #[test]
    fn test_nanosecond_time_bounds_calculation() {
        // Test the nanosecond_time_bounds function directly
        // diff_slots = 15 - 10 = 5
        // lower = parent_time + 1
        // upper = parent_time + 2 * 5 * 400_000_000 = parent_time + 4_000_000_000
        let parent_time = 1_000_000_000_000; // 1000 seconds in nanos
        test_nanosecond_time_bounds_helper(
            10,
            parent_time,
            15,
            parent_time + 1,
            parent_time + 4_000_000_000,
        );
    }

    #[test]
    fn test_nanosecond_time_bounds_same_slot() {
        // Test with same slot (diff = 0)
        // diff_slots = 0
        // lower = parent_time + 1
        // upper = parent_time + 2 * 0 * 400_000_000 = parent_time
        // Note: In this case, lower > upper, so no timestamp would be valid
        // This is expected since we shouldn't have the same slot for parent and working bank
        let parent_time = 1_000_000_000_000;
        test_nanosecond_time_bounds_helper(10, parent_time, 10, parent_time + 1, parent_time);
    }

    #[test]
    fn test_update_parent_as_first_marker() {
        let mut processor = BlockComponentProcessor::default();
        let update_parent = VersionedUpdateParent::V1(UpdateParentV1 {
            new_parent_slot: 0,
            new_parent_block_id: Hash::default(),
        });

        assert!(processor.on_update_parent(&update_parent).is_ok());
        assert!(processor.update_parent.is_some());
    }

    #[test]
    fn test_update_parent_after_header_abandoned_bank() {
        let mut processor = BlockComponentProcessor::default();
        processor
            .on_header(&VersionedBlockHeader::V1(BlockHeaderV1 {
                parent_slot: 0,
                parent_block_id: Hash::default(),
            }))
            .unwrap();

        let update_parent = VersionedUpdateParent::V1(UpdateParentV1 {
            new_parent_slot: 0,
            new_parent_block_id: Hash::default(),
        });

        assert!(matches!(
            processor.on_update_parent(&update_parent),
            Err(BlockComponentProcessorError::AbandonedBank(_))
        ));
    }

    #[test]
    fn test_multiple_update_parents_error() {
        let mut processor = BlockComponentProcessor::default();
        let update_parent = VersionedUpdateParent::V1(UpdateParentV1 {
            new_parent_slot: 0,
            new_parent_block_id: Hash::default(),
        });

        // First should succeed
        processor.on_update_parent(&update_parent).unwrap();

        // Second should fail
        assert_eq!(
            processor.on_update_parent(&update_parent),
            Err(BlockComponentProcessorError::MultipleUpdateParents)
        );
    }

    #[test]
    fn test_header_after_update_parent_error() {
        let mut processor = BlockComponentProcessor::default();
        processor
            .on_update_parent(&VersionedUpdateParent::V1(UpdateParentV1 {
                new_parent_slot: 0,
                new_parent_block_id: Hash::default(),
            }))
            .unwrap();

        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        assert_eq!(
            processor.on_header(&header),
            Err(BlockComponentProcessorError::SpuriousUpdateParent)
        );
    }

    #[test]
    #[ignore] // TODO(ksn): Enable when fast leader handover is enabled in MigrationPhase::should_allow_fast_leader_handover
    fn test_workflow_with_update_parent() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        processor
            .on_update_parent(&VersionedUpdateParent::V1(UpdateParentV1 {
                new_parent_slot: 0,
                new_parent_block_id: Hash::default(),
            }))
            .unwrap();

        assert!(processor.on_entry_batch(&migration_status).is_ok());

        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: (parent_time_nanos + 100_000_000) as u64,
            block_user_agent: vec![],
            final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });
        processor.on_footer(bank, parent, footer, None).unwrap();

        assert!(processor.on_final(&migration_status, 1).is_ok());
    }

    /// Creates a bank with BLS-enabled validators for testing certificate verification.
    /// Returns (bank, validator_keypairs) where bank has validators with BLS keys.
    fn create_bank_with_bls_validators(
        num_validators: usize,
        stakes: Vec<u64>,
    ) -> (Arc<Bank>, Vec<ValidatorVoteKeypairs>) {
        assert_eq!(num_validators, stakes.len());
        let validator_keypairs: Vec<ValidatorVoteKeypairs> = (0..num_validators)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect();

        let genesis_config_info = create_genesis_config_with_alpenglow_vote_accounts(
            10_000_000,
            &validator_keypairs,
            stakes,
        );

        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));
        (bank, validator_keypairs)
    }

    /// Build a certificate by manually aggregating BLS signatures and encoding bitmap.
    /// `signing_ranks` specifies which validator ranks are signing.
    fn build_certificate_manual(
        cert_type: CertificateType,
        vote: Vote,
        signing_ranks: &[usize],
        validator_keypairs: &[ValidatorVoteKeypairs],
    ) -> Certificate {
        let serialized_vote = bincode::serialize(&vote).unwrap();

        // Aggregate signatures
        let mut signature = SignatureProjective::identity();
        for &rank in signing_ranks {
            let sig = validator_keypairs[rank].bls_keypair.sign(&serialized_vote);
            signature.aggregate_with(std::iter::once(&sig)).unwrap();
        }

        // Build bitmap
        let max_rank = signing_ranks.iter().copied().max().unwrap_or(0);
        let mut bitvec = BitVec::<u8, Lsb0>::repeat(false, max_rank.saturating_add(1));
        for &rank in signing_ranks {
            bitvec.set(rank, true);
        }
        let bitmap = encode_base2(&bitvec).expect("Failed to encode bitmap");

        Certificate {
            cert_type,
            signature: signature.into(),
            bitmap,
        }
    }

    #[test]
    fn test_verify_final_cert_valid() {
        // Create 10 validators with descending stakes (1000, 900, 800, ...)
        // Total stake = 5500
        let num_validators = 10;
        let stakes: Vec<u64> = (0..num_validators)
            .map(|i| (1000u64).saturating_sub((i as u64).saturating_mul(100)))
            .collect();
        let (bank, validator_keypairs) = create_bank_with_bls_validators(num_validators, stakes);

        let slot = bank.slot();
        let block_id = Hash::new_unique();

        // Test 1: Fast finalize (requires 80% stake = 4400)
        // Top 6 validators = 1000+900+800+700+600+500 = 4500 (>= 80%)
        {
            let cert_type = CertificateType::FinalizeFast(slot, block_id);
            let vote = Vote::new_notarization_vote(slot, block_id);
            let signing_ranks: Vec<usize> = (0..6).collect();
            let fast_finalize_cert =
                build_certificate_manual(cert_type, vote, &signing_ranks, &validator_keypairs);

            let final_cert = FinalCertificate {
                slot,
                block_id,
                final_aggregate: VotesAggregate::from_certificate(&fast_finalize_cert),
                notar_aggregate: None,
            };

            let result = ValidatedBlockFinalizationCert::try_from_footer(final_cert, &bank);
            assert!(
                result.is_ok(),
                "Valid fast finalize certificate should pass verification: {result:?}"
            );
        }

        // Test 2: Slow finalize (requires 60% stake = 3300 for both certs)
        // Top 4 validators = 1000+900+800+700 = 3400 (>= 60%)
        {
            let notarize_cert_type = CertificateType::Notarize(slot, block_id);
            let notarize_vote = Vote::new_notarization_vote(slot, block_id);
            let notarize_signing_ranks: Vec<usize> = (0..4).collect();
            let notarize_cert = build_certificate_manual(
                notarize_cert_type,
                notarize_vote,
                &notarize_signing_ranks,
                &validator_keypairs,
            );

            let finalize_cert_type = CertificateType::Finalize(slot);
            let finalize_vote = Vote::new_finalization_vote(slot);
            let finalize_signing_ranks: Vec<usize> = (0..4).collect();
            let finalize_cert = build_certificate_manual(
                finalize_cert_type,
                finalize_vote,
                &finalize_signing_ranks,
                &validator_keypairs,
            );

            let final_cert = FinalCertificate {
                slot,
                block_id,
                final_aggregate: VotesAggregate::from_certificate(&finalize_cert),
                notar_aggregate: Some(VotesAggregate::from_certificate(&notarize_cert)),
            };

            let result = ValidatedBlockFinalizationCert::try_from_footer(final_cert, &bank);
            assert!(
                result.is_ok(),
                "Valid slow finalize certificate should pass verification: {result:?}"
            );
        }
    }

    #[test]
    fn test_verify_final_cert_insufficient_stake() {
        // Create 10 validators with descending stakes (1000, 900, 800, ...)
        // Total stake = 5500
        let num_validators = 10;
        let stakes: Vec<u64> = (0..num_validators)
            .map(|i| (1000u64).saturating_sub((i as u64).saturating_mul(100)))
            .collect();
        let (bank, validator_keypairs) = create_bank_with_bls_validators(num_validators, stakes);

        let slot = bank.slot();
        let block_id = Hash::new_unique();

        // Fast finalize with insufficient stake (requires 80% = 4400)
        // Top 5 validators = 1000+900+800+700+600 = 4000 (< 80%)
        {
            let cert_type = CertificateType::FinalizeFast(slot, block_id);
            let vote = Vote::new_notarization_vote(slot, block_id);
            let signing_ranks: Vec<usize> = (0..5).collect();
            let fast_finalize_cert =
                build_certificate_manual(cert_type, vote, &signing_ranks, &validator_keypairs);

            let final_cert = FinalCertificate {
                slot,
                block_id,
                final_aggregate: VotesAggregate::from_certificate(&fast_finalize_cert),
                notar_aggregate: None,
            };

            let result = ValidatedBlockFinalizationCert::try_from_footer(final_cert, &bank);
            assert!(
                matches!(
                    result,
                    Err(BlockFinalizationCertError::InsufficientStake { .. })
                ),
                "Fast finalize with insufficient stake should fail verification"
            );
        }

        // Slow finalize with insufficient notarize stake (requires 60% = 3300)
        // Top 3 validators = 1000+900+800 = 2700 (< 60%)
        {
            let notarize_cert_type = CertificateType::Notarize(slot, block_id);
            let notarize_vote = Vote::new_notarization_vote(slot, block_id);
            let notarize_signing_ranks: Vec<usize> = (0..3).collect();
            let notarize_cert = build_certificate_manual(
                notarize_cert_type,
                notarize_vote,
                &notarize_signing_ranks,
                &validator_keypairs,
            );

            // Finalize cert has enough stake
            let finalize_cert_type = CertificateType::Finalize(slot);
            let finalize_vote = Vote::new_finalization_vote(slot);
            let finalize_signing_ranks: Vec<usize> = (0..4).collect();
            let finalize_cert = build_certificate_manual(
                finalize_cert_type,
                finalize_vote,
                &finalize_signing_ranks,
                &validator_keypairs,
            );

            let final_cert = FinalCertificate {
                slot,
                block_id,
                final_aggregate: VotesAggregate::from_certificate(&finalize_cert),
                notar_aggregate: Some(VotesAggregate::from_certificate(&notarize_cert)),
            };

            let result = ValidatedBlockFinalizationCert::try_from_footer(final_cert, &bank);
            assert!(
                matches!(
                    result,
                    Err(BlockFinalizationCertError::InsufficientStake { .. })
                ),
                "Slow finalize with insufficient notarize stake should fail verification"
            );
        }

        // Slow finalize with insufficient finalize stake (requires 60% = 3300)
        // Notarize has enough stake, but finalize doesn't
        {
            // Notarize cert has enough stake
            let notarize_cert_type = CertificateType::Notarize(slot, block_id);
            let notarize_vote = Vote::new_notarization_vote(slot, block_id);
            let notarize_signing_ranks: Vec<usize> = (0..4).collect();
            let notarize_cert = build_certificate_manual(
                notarize_cert_type,
                notarize_vote,
                &notarize_signing_ranks,
                &validator_keypairs,
            );

            // Finalize cert has insufficient stake
            // Top 3 validators = 1000+900+800 = 2700 (< 60%)
            let finalize_cert_type = CertificateType::Finalize(slot);
            let finalize_vote = Vote::new_finalization_vote(slot);
            let finalize_signing_ranks: Vec<usize> = (0..3).collect();
            let finalize_cert = build_certificate_manual(
                finalize_cert_type,
                finalize_vote,
                &finalize_signing_ranks,
                &validator_keypairs,
            );

            let final_cert = FinalCertificate {
                slot,
                block_id,
                final_aggregate: VotesAggregate::from_certificate(&finalize_cert),
                notar_aggregate: Some(VotesAggregate::from_certificate(&notarize_cert)),
            };

            let result = ValidatedBlockFinalizationCert::try_from_footer(final_cert, &bank);
            assert!(
                matches!(
                    result,
                    Err(BlockFinalizationCertError::InsufficientStake { .. })
                ),
                "Slow finalize with insufficient finalize stake should fail verification"
            );
        }
    }
}
