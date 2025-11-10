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
    pub fn finish(
        &self,
        migration_status: &MigrationStatus,
    ) -> Result<(), BlockComponentProcessorError> {
        // Pre-migration: blocks with block components should be marked as dead
        if !migration_status.is_alpenglow_enabled() {
            match self.has_footer || self.has_header {
                false => return Ok(()),
                true => return Err(BlockComponentProcessorError::BlockComponentPreMigration),
            }
        }

        // Post-migration: both header and footer are required
        if !self.has_footer {
            return Err(BlockComponentProcessorError::MissingBlockFooter);
        }

        if !self.has_header {
            return Err(BlockComponentProcessorError::MissingBlockHeader);
        }

        Ok(())
    }

    pub fn on_marker(
        &mut self,
        marker: &VersionedBlockMarker,
    ) -> Result<(), BlockComponentProcessorError> {
        let VersionedBlockMarker::V1(marker) = marker;

        match marker {
            BlockMarkerV1::BlockFooter(footer) => self.on_footer(footer.inner()),
            BlockMarkerV1::BlockHeader(header) => self.on_header(header.inner()),
            // We process UpdateParent messages on shred ingest, so no callback needed here
            BlockMarkerV1::UpdateParent(_) => Ok(()),
            BlockMarkerV1::GenesisCertificate(_) => Ok(()),
        }
    }

    fn on_footer(
        &mut self,
        _footer: &VersionedBlockFooter,
    ) -> Result<(), BlockComponentProcessorError> {
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
