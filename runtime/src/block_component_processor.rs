use {
    agave_votor_messages::migration::MigrationStatus,
    solana_entry::block_component::{
        BlockMarkerV1, VersionedBlockFooter, VersionedBlockHeader, VersionedBlockMarker,
    },
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
            BlockMarkerV1::BlockFooter(footer) => self.on_footer(footer.inner()),
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
        _footer: &VersionedBlockFooter,
    ) -> Result<(), BlockComponentProcessorError> {
        // The block header must be the first component of each block.
        if !self.has_header {
            return Err(BlockComponentProcessorError::MissingBlockHeader);
        }

        if self.has_footer {
            return Err(BlockComponentProcessorError::MultipleBlockFooters);
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

        self.has_header = true;
        Ok(())
    }
}
