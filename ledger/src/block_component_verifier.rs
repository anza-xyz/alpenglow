use {
    crate::blockstore_processor::BlockstoreProcessorError,
    solana_entry::block_component::{BlockMarkerV1, VersionedBlockMarker},
    std::result,
};

#[derive(Default)]
pub struct BlockComponentVerifier {
    has_footer: bool,
    has_header: bool,
}

impl BlockComponentVerifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finish(&self) -> result::Result<(), BlockstoreProcessorError> {
        if !self.has_footer {
            return Err(BlockstoreProcessorError::MissingBlockFooter);
        }

        if !self.has_header {
            return Err(BlockstoreProcessorError::MissingBlockHeader);
        }

        Ok(())
    }

    pub fn on_marker(
        &mut self,
        marker: &VersionedBlockMarker,
    ) -> result::Result<(), BlockstoreProcessorError> {
        let marker = match marker {
            VersionedBlockMarker::V1(marker) | VersionedBlockMarker::Current(marker) => marker,
        };

        match marker {
            BlockMarkerV1::BlockFooter(_) => self.on_footer(),
            BlockMarkerV1::BlockHeader(_) => self.on_header(),
            // We process UpdateParent messages on shred ingest
            BlockMarkerV1::UpdateParent(_) => Ok(()),
        }
    }

    fn on_footer(&mut self) -> result::Result<(), BlockstoreProcessorError> {
        if self.has_footer {
            return Err(BlockstoreProcessorError::MultipleBlockFooters);
        }

        self.has_footer = true;
        Ok(())
    }

    fn on_header(&mut self) -> result::Result<(), BlockstoreProcessorError> {
        if self.has_header {
            return Err(BlockstoreProcessorError::MultipleBlockHeaders);
        }

        self.has_header = true;
        Ok(())
    }
}
