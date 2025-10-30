use {
    crate::bank::Bank,
    solana_entry::block_component::{
        BlockMarkerV1, VersionedBlockFooter, VersionedBlockHeader, VersionedBlockMarker,
    },
    solana_votor_messages::migration::MigrationStatus,
    std::sync::Arc,
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BlockComponentVerifierError {
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
pub struct BlockComponentVerifier {
    has_header: bool,
    has_footer: bool,
}

impl BlockComponentVerifier {
    pub fn finish(
        &self,
        migration_status: &MigrationStatus,
    ) -> Result<(), BlockComponentVerifierError> {
        // Pre-migration: blocks with block components should be marked as dead
        if !migration_status.is_alpenglow_enabled() {
            match self.has_footer || self.has_header {
                false => return Ok(()),
                true => return Err(BlockComponentVerifierError::BlockComponentPreMigration),
            }
        }

        // Post-migration: both header and footer are required
        if !self.has_footer {
            return Err(BlockComponentVerifierError::MissingBlockFooter);
        }

        if !self.has_header {
            return Err(BlockComponentVerifierError::MissingBlockHeader);
        }

        Ok(())
    }

    pub fn on_marker(
        &mut self,
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        marker: &VersionedBlockMarker,
    ) -> Result<(), BlockComponentVerifierError> {
        let marker = match marker {
            VersionedBlockMarker::V1(marker) | VersionedBlockMarker::Current(marker) => marker,
        };

        match marker {
            BlockMarkerV1::BlockFooter(footer) => self.on_footer(bank, parent_bank, footer),
            BlockMarkerV1::BlockHeader(header) => self.on_header(header),
            // We process UpdateParent messages on shred ingest, so no callback needed here
            BlockMarkerV1::UpdateParent(_) => Ok(()),
        }
    }

    fn on_footer(
        &mut self,
        _bank: Arc<Bank>,
        _parent_bank: Arc<Bank>,
        _footer: &VersionedBlockFooter,
    ) -> Result<(), BlockComponentVerifierError> {
        if self.has_footer {
            return Err(BlockComponentVerifierError::MultipleBlockFooters);
        }

        self.has_footer = true;
        Ok(())
    }

    fn on_header(
        &mut self,
        _header: &VersionedBlockHeader,
    ) -> Result<(), BlockComponentVerifierError> {
        if self.has_header {
            return Err(BlockComponentVerifierError::MultipleBlockHeaders);
        }

        self.has_header = true;
        Ok(())
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
    fn test_missing_header_error() {
        let migration_status = MigrationStatus::post_migration_status();
        let verifier = BlockComponentVerifier::default();

        // Set footer but not header
        let mut v = verifier;
        v.has_footer = true;

        let result = v.finish(&migration_status);
        assert_eq!(result, Err(BlockComponentVerifierError::MissingBlockHeader));
    }

    #[test]
    fn test_missing_footer_error() {
        let migration_status = MigrationStatus::post_migration_status();
        let verifier = BlockComponentVerifier {
            has_header: true,
            ..BlockComponentVerifier::default()
        };

        let result = verifier.finish(&migration_status);
        assert_eq!(result, Err(BlockComponentVerifierError::MissingBlockFooter));
    }

    #[test]
    fn test_multiple_headers_error() {
        let mut verifier = BlockComponentVerifier::default();
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        // First header should succeed
        assert!(verifier.on_header(&header).is_ok());

        // Second header should fail
        let result = verifier.on_header(&header);
        assert_eq!(
            result,
            Err(BlockComponentVerifierError::MultipleBlockHeaders)
        );
    }

    #[test]
    fn test_multiple_footers_error() {
        let mut verifier = BlockComponentVerifier::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1_000_000_000,
            block_user_agent: vec![],
        });

        // First footer should succeed
        assert!(verifier
            .on_footer(bank.clone(), parent.clone(), &footer)
            .is_ok());

        // Second footer should fail
        let result = verifier.on_footer(bank, parent, &footer);
        assert_eq!(
            result,
            Err(BlockComponentVerifierError::MultipleBlockFooters)
        );
    }

    #[test]
    fn test_on_footer_sets_timestamp() {
        let mut verifier = BlockComponentVerifier::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        let footer_time = 1_234_567_890_000_000_000; // nanos
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: footer_time,
            block_user_agent: vec![],
        });

        verifier.on_footer(bank.clone(), parent, &footer).unwrap();

        assert!(verifier.has_footer);
    }

    #[test]
    fn test_on_header_sets_flag() {
        let mut verifier = BlockComponentVerifier::default();
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        verifier.on_header(&header).unwrap();
        assert!(verifier.has_header);
    }

    #[test]
    fn test_on_marker_processes_header() {
        let mut verifier = BlockComponentVerifier::default();
        let marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockHeader(
            VersionedBlockHeader::V1(BlockHeaderV1 {
                parent_slot: 0,
                parent_block_id: Hash::default(),
            }),
        ));

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        verifier.on_marker(bank, parent, &marker).unwrap();
        assert!(verifier.has_header);
    }

    #[test]
    fn test_on_marker_processes_footer() {
        let mut verifier = BlockComponentVerifier::default();
        let footer_time = 1_234_567_890_000_000_000;
        let marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockFooter(
            VersionedBlockFooter::V1(BlockFooterV1 {
                bank_hash: Hash::new_unique(),
                block_producer_time_nanos: footer_time,
                block_user_agent: vec![],
            }),
        ));

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        verifier.on_marker(bank.clone(), parent, &marker).unwrap();
        assert!(verifier.has_footer);
    }

    #[test]
    fn test_complete_workflow_success() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut verifier = BlockComponentVerifier::default();
        let parent = create_test_bank();
        let parent_time = 1_000_000_000_000_000_000u64;
        let bank = create_child_bank(&parent, 1);

        // Process header
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });
        verifier.on_header(&header).unwrap();

        // Process footer with valid timestamp
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: parent_time + 100_000_000, // 100ms later
            block_user_agent: vec![],
        });
        verifier
            .on_footer(bank.clone(), parent.clone(), &footer)
            .unwrap();

        // Finish verification
        let result = verifier.finish(&migration_status);
        assert!(result.is_ok());
    }

    #[test]
    fn test_block_component_detected_pre_migration_with_header() {
        let migration_status = MigrationStatus::default();
        let mut verifier = BlockComponentVerifier::default();

        // Add a header pre-migration
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });
        verifier.on_header(&header).unwrap();

        // Should fail because we have a header pre-migration
        let result = verifier.finish(&migration_status);
        assert_eq!(
            result,
            Err(BlockComponentVerifierError::BlockComponentPreMigration)
        );
    }

    #[test]
    fn test_block_component_detected_pre_migration_with_footer() {
        let migration_status = MigrationStatus::default();
        let mut verifier = BlockComponentVerifier::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Add a footer pre-migration
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1_000_000_000,
            block_user_agent: vec![],
        });
        verifier.on_footer(bank, parent, &footer).unwrap();

        // Should fail because we have a footer pre-migration
        let result = verifier.finish(&migration_status);
        assert_eq!(
            result,
            Err(BlockComponentVerifierError::BlockComponentPreMigration)
        );
    }

    #[test]
    fn test_no_block_components_pre_migration() {
        let migration_status = MigrationStatus::default();
        let verifier = BlockComponentVerifier::default();

        // Should succeed because no block components were added
        let result = verifier.finish(&migration_status);
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_workflow_post_migration() {
        let migration_status = MigrationStatus::post_migration_status();
        let mut verifier = BlockComponentVerifier::default();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        // Process header
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });
        verifier.on_header(&header).unwrap();

        // Process footer
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1_234_567_890_000_000_000,
            block_user_agent: vec![],
        });
        verifier.on_footer(bank, parent, &footer).unwrap();

        // Should succeed post-migration with both header and footer
        let result = verifier.finish(&migration_status);
        assert!(result.is_ok());
    }
}
