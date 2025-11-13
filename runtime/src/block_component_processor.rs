use {
    crate::bank::Bank,
    agave_votor_messages::migration::MigrationStatus,
    solana_clock::{Slot, DEFAULT_MS_PER_SLOT},
    solana_entry::block_component::{
        BlockFooterV1, BlockMarkerV1, VersionedBlockFooter, VersionedBlockHeader,
        VersionedBlockMarker,
    },
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
        // Post-migration: both header and footer are required.
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
        // Pre-migration: blocks with block components should be marked as dead.
        if !migration_status.is_alpenglow_enabled() {
            return Err(BlockComponentProcessorError::BlockComponentPreMigration);
        }

        let VersionedBlockMarker::V1(marker) = marker;

        match marker {
            BlockMarkerV1::BlockFooter(footer) => {
                self.on_footer(bank, parent_bank, footer.inner())
            }
            BlockMarkerV1::BlockHeader(header) => self.on_header(header.inner()),
            // We process UpdateParent messages on shred ingest, so no callback needed here.
            BlockMarkerV1::UpdateParent(_) => Ok(()),
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

        let VersionedBlockFooter::V1(footer) = footer;

        Self::enforce_nanosecond_clock_bounds(bank.clone(), parent_bank, footer)?;
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
        // If nanosecond clock hasn't been populated, don't enforce bounds yet.
        let Some(parent_time_nanos) = parent_bank.get_nanosecond_clock() else {
            return Ok(());
        };

        let parent_slot = parent_bank.slot();
        let current_time_nanos = i64::try_from(footer.block_producer_time_nanos).unwrap_or(i64::MAX);
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
        let max_working_bank_time = parent_time_nanos
            .saturating_add(diff_slots.saturating_mul(2).saturating_mul(default_ns_per_slot));

        (min_working_bank_time, max_working_bank_time)
    }

    pub fn update_bank_with_footer(bank: Arc<Bank>, footer: &BlockFooterV1) {
        // Update clock sysvar from footer timestamp.
        let unix_timestamp_nanos = i64::try_from(footer.block_producer_time_nanos).unwrap_or(i64::MAX);
        bank.update_clock_from_footer(unix_timestamp_nanos);

        // TODO: rewards
    }
}
