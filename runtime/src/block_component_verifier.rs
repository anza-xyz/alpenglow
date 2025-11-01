use {
    crate::bank::Bank,
    solana_entry::block_component::{
        BlockMarkerV1, VersionedBlockFooter, VersionedBlockHeader, VersionedBlockMarker,
    },
    std::{result, sync::Arc},
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
}

#[derive(Default)]
pub struct BlockComponentVerifier {
    has_header: bool,
    has_footer: bool,
}

impl BlockComponentVerifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finish(&self) -> result::Result<(), BlockComponentVerifierError> {
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
    ) -> result::Result<(), BlockComponentVerifierError> {
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
        bank: Arc<Bank>,
        parent_bank: Arc<Bank>,
        footer: &VersionedBlockFooter,
    ) -> result::Result<(), BlockComponentVerifierError> {
        if self.has_footer {
            return Err(BlockComponentVerifierError::MultipleBlockFooters);
        }

        let footer = match footer {
            VersionedBlockFooter::V1(footer) | VersionedBlockFooter::Current(footer) => footer,
        };

        // Update the bank's clock timestamp with the value from the block footer
        let parent_epoch = Some(parent_bank.epoch());
        bank.set_clock(parent_epoch, footer.block_producer_time_nanos);

        self.has_footer = true;
        Ok(())
    }

    fn on_header(
        &mut self,
        _header: &VersionedBlockHeader,
    ) -> result::Result<(), BlockComponentVerifierError> {
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
        let verifier = BlockComponentVerifier::new();

        // Set footer but not header
        let mut v = verifier;
        v.has_footer = true;

        let result = v.finish();
        assert_eq!(result, Err(BlockComponentVerifierError::MissingBlockHeader));
    }

    #[test]
    fn test_missing_footer_error() {
        let mut verifier = BlockComponentVerifier::new();
        verifier.has_header = true;

        let result = verifier.finish();
        assert_eq!(result, Err(BlockComponentVerifierError::MissingBlockFooter));
    }

    #[test]
    fn test_multiple_headers_error() {
        let mut verifier = BlockComponentVerifier::new();
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
        let mut verifier = BlockComponentVerifier::new();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
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
        let mut verifier = BlockComponentVerifier::new();
        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        let footer_time = 1_234_567_890_000_000_000; // nanos
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            block_producer_time_nanos: footer_time,
            block_user_agent: vec![],
        });

        verifier.on_footer(bank.clone(), parent, &footer).unwrap();

        assert!(verifier.has_footer);

        // Verify clock sysvar is set correctly (should be in seconds, not nanoseconds)
        let clock = bank.clock();
        assert_eq!(clock.unix_timestamp, (footer_time / 1_000_000_000) as i64);
    }

    #[test]
    fn test_on_header_sets_flag() {
        let mut verifier = BlockComponentVerifier::new();
        let header = VersionedBlockHeader::V1(BlockHeaderV1 {
            parent_slot: 0,
            parent_block_id: Hash::default(),
        });

        verifier.on_header(&header).unwrap();
        assert!(verifier.has_header);
    }

    #[test]
    fn test_on_marker_processes_header() {
        let mut verifier = BlockComponentVerifier::new();
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
        let mut verifier = BlockComponentVerifier::new();
        let footer_time = 1_234_567_890_000_000_000;
        let marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockFooter(
            VersionedBlockFooter::V1(BlockFooterV1 {
                block_producer_time_nanos: footer_time,
                block_user_agent: vec![],
            }),
        ));

        let parent = create_test_bank();
        let bank = create_child_bank(&parent, 1);

        verifier.on_marker(bank.clone(), parent, &marker).unwrap();
        assert!(verifier.has_footer);

        // Verify clock sysvar is set correctly (should be in seconds, not nanoseconds)
        let clock = bank.clock();
        assert_eq!(clock.unix_timestamp, (footer_time / 1_000_000_000) as i64);
    }

    #[test]
    fn test_complete_workflow_success() {
        let mut verifier = BlockComponentVerifier::new();
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
            block_producer_time_nanos: parent_time + 100_000_000, // 100ms later
            block_user_agent: vec![],
        });
        verifier
            .on_footer(bank.clone(), parent.clone(), &footer)
            .unwrap();

        let expected_timestamp = parent_time + 100_000_000;

        // Verify clock sysvar is set correctly (should be in seconds, not nanoseconds)
        let clock = bank.clock();
        assert_eq!(
            clock.unix_timestamp,
            (expected_timestamp / 1_000_000_000) as i64
        );

        // Finish verification
        let result = verifier.finish();
        assert!(result.is_ok());
    }
}
