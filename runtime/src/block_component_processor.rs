use {
    crate::bank::Bank,
    solana_clock::{Slot, DEFAULT_MS_PER_SLOT},
    solana_entry::block_component::{
        BlockFooterV1, BlockMarkerV1, VersionedBlockFooter, VersionedBlockHeader,
        VersionedBlockMarker,
    },
    solana_votor_messages::migration::MigrationStatus,
    std::sync::Arc,
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BlockComponentProcessorError {
    #[error("Missing block footer")]
    MissingBlockFooter,
    #[error("Missing block header")]
    MissingBlockHeader,
    #[error("Multiple block footers detected")]
    MultipleBlockFooters,
    #[error("Multiple block headers detected")]
    MultipleBlockHeaders,
    #[error("BlockComponent detected pre-migration")]
    BlockComponentPreMigration,
    #[error("Nanosecond clock out of bounds")]
    NanosecondClockOutOfBounds,
}

#[derive(Default)]
pub struct BlockComponentProcessor {
    has_header: bool,
    has_footer: bool,
}

impl BlockComponentProcessor {
    fn on_final(&self) -> Result<(), BlockComponentProcessorError> {
        // Post-migration: both header and footer are required
        if !self.has_footer {
            return Err(BlockComponentProcessorError::MissingBlockFooter);
        }

        if !self.has_header {
            return Err(BlockComponentProcessorError::MissingBlockHeader);
        }

        Ok(())
    }

    pub fn on_entry_batch(
        &mut self,
        migration_status: &MigrationStatus,
        is_final: bool,
    ) -> Result<(), BlockComponentProcessorError> {
        if !migration_status.is_alpenglow_enabled() {
            return Ok(());
        }

        // The block header must be the first component of each block.
        if !self.has_header {
            return Err(BlockComponentProcessorError::MissingBlockHeader);
        }

        if is_final {
            self.on_final()
        } else {
            Ok(())
        }
    }

    pub fn on_marker(
        &mut self,
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        marker: &VersionedBlockMarker,
        migration_status: &MigrationStatus,
        is_final: bool,
    ) -> Result<(), BlockComponentProcessorError> {
        // Pre-migration: blocks with block components should be marked as dead
        if !migration_status.is_alpenglow_enabled() {
            return Err(BlockComponentProcessorError::BlockComponentPreMigration);
        }

        // Here onwards, Alpenglow is enabled
        let marker = match marker {
            VersionedBlockMarker::V1(marker) | VersionedBlockMarker::Current(marker) => marker,
        };

        match marker {
            BlockMarkerV1::BlockFooter(footer) => self.on_footer(bank, parent_bank, footer),
            BlockMarkerV1::BlockHeader(header) => self.on_header(header),
            // We process UpdateParent messages on shred ingest, so no callback needed here
            BlockMarkerV1::UpdateParent(_) => Ok(()),
            // TODO(ashwin): update genesis certificate account / ticks
            BlockMarkerV1::GenesisCertificate(_) => Ok(()),
        }?;

        if is_final {
            self.on_final()
        } else {
            Ok(())
        }
    }

    fn on_footer(
        &mut self,
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        footer: &VersionedBlockFooter,
    ) -> Result<(), BlockComponentProcessorError> {
        // The block header must be the first component of each block.
        if !self.has_header {
            return Err(BlockComponentProcessorError::MissingBlockHeader);
        }

        if self.has_footer {
            return Err(BlockComponentProcessorError::MultipleBlockFooters);
        }

        let footer = match footer {
            VersionedBlockFooter::V1(footer) | VersionedBlockFooter::Current(footer) => footer,
        };

        Self::enforce_nanosecond_clock_bounds(bank.clone(), parent_bank.clone(), footer)?;
        Self::update_bank_with_footer(bank, footer);

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

        self.has_header = true;
        Ok(())
    }

    fn enforce_nanosecond_clock_bounds(
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
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

    pub fn update_bank_with_footer(bank: Arc<Bank>, footer: &BlockFooterV1) {
        // Update clock sysvar
        bank.update_clock_from_footer(footer.block_producer_time_nanos as i64);

        // TODO: rewards
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{bank::Bank, genesis_utils::create_genesis_config},
        solana_entry::block_component::{BlockFooterV1, BlockHeaderV1},
        solana_program::{hash::Hash, pubkey::Pubkey},
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
        let result = processor.on_entry_batch(&migration_status, false);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::MissingBlockHeader)
        );
    }

    #[test]
    fn test_missing_footer_error_on_slot_full() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor {
            has_header: true,
            ..BlockComponentProcessor::default()
        };

        // Try to mark slot as full without footer - should fail
        let result = processor.on_entry_batch(&migration_status, true);
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
        });

        // First footer should succeed
        assert!(processor
            .on_footer(bank.clone(), parent.clone(), &footer)
            .is_ok());

        // Second footer should fail
        let result = processor.on_footer(bank, parent, &footer);
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
        });

        processor.on_footer(bank.clone(), parent, &footer).unwrap();

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
        let marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockHeader(
            VersionedBlockHeader::V1(BlockHeaderV1 {
                parent_slot: 0,
                parent_block_id: Hash::default(),
            }),
        ));

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        processor
            .on_marker(bank, parent, &marker, &migration_status, false)
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

        let marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockFooter(
            VersionedBlockFooter::V1(BlockFooterV1 {
                bank_hash: Hash::new_unique(),
                block_producer_time_nanos: footer_time_nanos as u64,
                block_user_agent: vec![],
            }),
        ));

        processor
            .on_marker(bank.clone(), parent, &marker, &migration_status, false)
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
        assert!(processor.on_entry_batch(&migration_status, false).is_ok());

        // Process footer with valid timestamp
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time_nanos as u64,
            block_user_agent: vec![],
        });
        processor
            .on_footer(bank.clone(), parent.clone(), &footer)
            .unwrap();

        // Verify clock sysvar was updated
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);

        // Process final entry batch (slot is full) - should succeed
        let result = processor.on_entry_batch(&migration_status, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_block_marker_detected_pre_migration() {
        let migration_status = MigrationStatus::default();
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Try to process a block header marker pre-migration - should fail
        let marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockHeader(
            VersionedBlockHeader::V1(BlockHeaderV1 {
                parent_slot: 0,
                parent_block_id: Hash::default(),
            }),
        ));

        let result = processor.on_marker(bank, parent, &marker, &migration_status, false);
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
        let result = processor.on_entry_batch(&migration_status, false);
        assert!(result.is_ok());

        // Even with slot full
        let result = processor.on_entry_batch(&migration_status, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_workflow_post_migration() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut processor = BlockComponentProcessor::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Process header marker
        let header_marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockHeader(
            VersionedBlockHeader::V1(BlockHeaderV1 {
                parent_slot: 0,
                parent_block_id: Hash::default(),
            }),
        ));
        processor
            .on_marker(
                bank.clone(),
                parent.clone(),
                &header_marker,
                &migration_status,
                false,
            )
            .unwrap();

        // Process entry batches
        assert!(processor.on_entry_batch(&migration_status, false).is_ok());

        // Calculate valid timestamp based on parent's time
        let parent_time_nanos = parent.clock().unix_timestamp.saturating_mul(1_000_000_000);
        let footer_time_nanos = parent_time_nanos + 500_000_000; // parent + 500ms
        let expected_time_secs = footer_time_nanos / 1_000_000_000;

        // Process footer marker
        let footer_marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockFooter(
            VersionedBlockFooter::V1(BlockFooterV1 {
                bank_hash: Hash::new_unique(),
                block_producer_time_nanos: footer_time_nanos as u64,
                block_user_agent: vec![],
            }),
        ));
        processor
            .on_marker(
                bank.clone(),
                parent,
                &footer_marker,
                &migration_status,
                false,
            )
            .unwrap();

        // Verify clock sysvar was updated
        assert_eq!(bank.clock().unix_timestamp, expected_time_secs);

        // Process final entry batch with slot_full=true - should succeed
        let result = processor.on_entry_batch(&migration_status, true);
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
        });

        // Try to process footer without header - should fail
        let result = processor.on_footer(bank, parent, &footer);
        assert_eq!(
            result,
            Err(BlockComponentProcessorError::MissingBlockHeader)
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

        // Process footer marker with slot_full=true
        let footer_marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockFooter(
            VersionedBlockFooter::V1(BlockFooterV1 {
                bank_hash: Hash::new_unique(),
                block_producer_time_nanos: footer_time_nanos as u64,
                block_user_agent: vec![],
            }),
        ));

        // Should succeed - footer is processed and slot_full validation passes
        let result = processor.on_marker(
            bank.clone(),
            parent,
            &footer_marker,
            &migration_status,
            true,
        );
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
        let result = processor.on_entry_batch(&migration_status, false);
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
        });

        processor.on_footer(bank.clone(), parent, &footer).unwrap();

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
        });

        let result = processor.on_footer(bank, parent, &footer);
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
}
