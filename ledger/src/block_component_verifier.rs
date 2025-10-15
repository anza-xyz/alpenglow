use {
    crate::blockstore_processor::BlockstoreProcessorError,
    solana_clock::UnixTimestamp,
    solana_entry::block_component::{
        BlockMarkerV1, VersionedBlockFooter, VersionedBlockHeader, VersionedBlockMarker,
    },
    solana_runtime::bank::Bank,
    std::{result, sync::Arc, time::Duration},
};

/// Time the leader has for producing and sending the block.
pub(crate) const DELTA_BLOCK: Duration = Duration::from_millis(400);

/// Clock multiplier for timeout bounds
const CLOCK_TIMEOUT_MULTIPLIER: u32 = 2;

#[derive(Default)]
pub struct BlockComponentVerifier {
    parent_alpenglow_timestamp: UnixTimestamp,
    current_alpenglow_timestamp: Option<UnixTimestamp>,
    has_header: bool,
}

impl BlockComponentVerifier {
    pub fn new(parent_alpenglow_timestamp: UnixTimestamp) -> Self {
        Self {
            parent_alpenglow_timestamp,
            ..Default::default()
        }
    }

    fn has_footer(&self) -> bool {
        self.current_alpenglow_timestamp.is_some()
    }

    fn check_alpenglow_clock_bounds(
        &self,
        bank: Arc<Bank>,
    ) -> result::Result<(), BlockstoreProcessorError> {
        let (current_slot, parent_slot) = (bank.slot(), bank.parent_slot());
        let (current_time, parent_time) = (
            // Guaranteed to exist, since we've already verified that has_footer() is true
            self.current_alpenglow_timestamp.unwrap(),
            self.parent_alpenglow_timestamp,
        );

        let diff_slots = current_slot.checked_sub(parent_slot).unwrap();

        let max_diff_time = DELTA_BLOCK
            .checked_mul(CLOCK_TIMEOUT_MULTIPLIER)
            .unwrap()
            .checked_mul(diff_slots as u32)
            .unwrap();
        let latest_acceptable_current_time = parent_time + (max_diff_time.as_nanos() as i64);

        if parent_time < current_time && current_time <= latest_acceptable_current_time {
            Ok(())
        } else {
            Err(BlockstoreProcessorError::AlpenglowClockBoundsExceeded)
        }
    }

    pub fn finish(&self, bank: Arc<Bank>) -> result::Result<(), BlockstoreProcessorError> {
        if !self.has_footer() {
            return Err(BlockstoreProcessorError::MissingBlockFooter);
        }

        if !self.has_header {
            return Err(BlockstoreProcessorError::MissingBlockHeader);
        }

        self.check_alpenglow_clock_bounds(bank)
    }

    pub fn on_marker(
        &mut self,
        marker: &VersionedBlockMarker,
    ) -> result::Result<(), BlockstoreProcessorError> {
        let marker = match marker {
            VersionedBlockMarker::V1(marker) | VersionedBlockMarker::Current(marker) => marker,
        };

        match marker {
            BlockMarkerV1::BlockFooter(footer) => self.on_footer(footer),
            BlockMarkerV1::BlockHeader(header) => self.on_header(header),
            // We process UpdateParent messages on shred ingest, so no callback needed here
            BlockMarkerV1::UpdateParent(_) => Ok(()),
        }
    }

    fn on_footer(
        &mut self,
        footer: &VersionedBlockFooter,
    ) -> result::Result<(), BlockstoreProcessorError> {
        if self.has_footer() {
            return Err(BlockstoreProcessorError::MultipleBlockFooters);
        }

        let footer = match footer {
            VersionedBlockFooter::V1(footer) | VersionedBlockFooter::Current(footer) => footer,
        };

        // TODO(ksn): should we just make alpenglow timestamps u64?
        self.current_alpenglow_timestamp = Some(footer.block_producer_time_nanos as i64);

        Ok(())
    }

    fn on_header(
        &mut self,
        _header: &VersionedBlockHeader,
    ) -> result::Result<(), BlockstoreProcessorError> {
        if self.has_header {
            return Err(BlockstoreProcessorError::MultipleBlockHeaders);
        }

        self.has_header = true;
        Ok(())
    }
}
