use {
    super::{
        malicious_repair_handler::{MaliciousRepairConfig, MaliciousRepairHandler},
        serve_repair::ServeRepair,
        standard_repair_handler::StandardRepairHandler,
    },
    crate::repair::{
        repair_response,
        serve_repair::{AncestorHashesResponse, BlockIdRepairResponse, MAX_ANCESTOR_RESPONSES},
    },
    bincode::serialize,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::{
        ancestor_iterator::{AncestorIterator, AncestorIteratorWithHash},
        blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
        shred::{ErasureSetId, Nonce, DATA_SHREDS_PER_FEC_BLOCK},
    },
    solana_perf::packet::{Packet, PacketBatch, PacketBatchRecycler, PinnedPacketBatch},
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::SharableBanks,
    solana_votor_messages::migration::MigrationStatus,
    std::{
        collections::HashSet,
        net::SocketAddr,
        sync::{Arc, RwLock},
    },
};

/// Helper function to create a PacketBatch from a serializable response
fn create_response_packet_batch<T: serde::Serialize>(
    recycler: &PacketBatchRecycler,
    response: &T,
    from_addr: &SocketAddr,
    nonce: Nonce,
    debug_label: &'static str,
) -> Option<PacketBatch> {
    let serialized_response = serialize(response).ok()?;
    let packet =
        repair_response::repair_response_packet_from_bytes(serialized_response, from_addr, nonce)?;
    Some(
        PinnedPacketBatch::new_unpinned_with_recycler_data(recycler, debug_label, vec![packet])
            .into(),
    )
}

pub trait RepairHandler {
    fn blockstore(&self) -> &Blockstore;

    fn repair_response_packet(
        &self,
        slot: Slot,
        shred_index: u64,
        block_id: Option<Hash>,
        dest: &SocketAddr,
        nonce: Nonce,
    ) -> Option<Packet>;

    fn run_window_request(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        shred_index: u64,
        block_id: Option<Hash>,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        let packet = self.repair_response_packet(slot, shred_index, block_id, from_addr, nonce)?;
        Some(
            PinnedPacketBatch::new_unpinned_with_recycler_data(
                recycler,
                "run_window_request",
                vec![packet],
            )
            .into(),
        )
    }

    fn run_window_request_for_block_id(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        shred_index: u64,
        block_id: Hash,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        self.run_window_request(
            recycler,
            from_addr,
            slot,
            shred_index,
            Some(block_id),
            nonce,
        )
    }

    fn run_highest_window_request(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        highest_index: u64,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        let meta = self
            .blockstore()
            .meta(slot)
            .expect("Unable to fetch slot meta from blockstore")?;
        if meta.received > highest_index {
            // meta.received must be at least 1 by this point
            let packet =
                self.repair_response_packet(slot, meta.received - 1, None, from_addr, nonce)?;
            return Some(
                PinnedPacketBatch::new_unpinned_with_recycler_data(
                    recycler,
                    "run_highest_window_request",
                    vec![packet],
                )
                .into(),
            );
        }
        None
    }

    fn run_orphan(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        max_responses: usize,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        let mut res =
            PinnedPacketBatch::new_unpinned_with_recycler(recycler, max_responses, "run_orphan");
        // Try to find the next "n" parent slots of the input slot
        let packets = std::iter::successors(self.blockstore().meta(slot).ok()?, |meta| {
            self.blockstore().meta(meta.parent_slot?).ok()?
        })
        .map_while(|meta| {
            repair_response::repair_response_packet(
                self.blockstore(),
                meta.slot,
                meta.received.checked_sub(1u64)?,
                from_addr,
                nonce,
            )
        });
        for packet in packets.take(max_responses) {
            res.push(packet);
        }
        (!res.is_empty()).then_some(res.into())
    }

    fn run_ancestor_hashes(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        let ancestor_slot_hashes = if self.blockstore().is_duplicate_confirmed(slot) {
            let ancestor_iterator = AncestorIteratorWithHash::from(
                AncestorIterator::new_inclusive(slot, self.blockstore()),
            );
            ancestor_iterator.take(MAX_ANCESTOR_RESPONSES).collect()
        } else {
            // If this slot is not duplicate confirmed, return nothing
            vec![]
        };
        let response = AncestorHashesResponse::Hashes(ancestor_slot_hashes);
        create_response_packet_batch(recycler, &response, from_addr, nonce, "run_ancestor_hashes")
    }

    fn run_parent_fec_set_count(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        block_id: Hash,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        let location = self.blockstore().get_block_location(slot, block_id)?;
        // `get_block_location()` only returns if `DoubleMerkleMeta` is populated.
        // `DoubleMerkleMeta` is only populated if the slot is full, thus all expects here as safe
        debug_assert!(self
            .blockstore()
            .meta_from_location(slot, location)
            .unwrap()
            .unwrap()
            .is_full());

        let double_merkle_meta = self
            .blockstore()
            .get_double_merkle_meta(slot, location)
            .expect("Unable to fetch double merkle meta")
            .expect("If location exists, double merkle meta must be populated");
        let fec_set_count = double_merkle_meta.fec_set_count;

        let parent_meta = self
            .blockstore()
            .get_parent_meta(slot, location)
            .expect("Unable to fetch ParentMeta")
            .expect("ParentMeta must exist if location exists");

        let response = BlockIdRepairResponse::ParentFecSetCount {
            fec_set_count,
            parent_info: (parent_meta.parent_slot, parent_meta.parent_block_id),
            parent_proof: double_merkle_meta
                .proofs
                .get(fec_set_count)
                .expect("Blockstore inconsistency in DoubleMerkleMeta")
                .clone(),
        };

        create_response_packet_batch(
            recycler,
            &response,
            from_addr,
            nonce,
            "run_parent_fec_set_count",
        )
    }

    fn run_fec_set_root(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        block_id: Hash,
        fec_set_index: u32,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        let location = self.blockstore().get_block_location(slot, block_id)?;
        // `get_block_location()` only returns if `DoubleMerkleMeta` is populated.
        // `DoubleMerkleMeta` is only populated if the slot is full, thus all expects here as safe
        debug_assert!(self
            .blockstore()
            .meta_from_location(slot, location)
            .unwrap()
            .unwrap()
            .is_full());

        let double_merkle_meta = self
            .blockstore()
            .get_double_merkle_meta(slot, location)
            .expect("Unable to fetch double merkle meta")
            .expect("If location exists, double merkle meta must be populated");

        let fec_set_root = self
            .blockstore()
            .merkle_root_meta_from_location(ErasureSetId::new(slot, fec_set_index), location)
            .expect("Unable to fetch merkle root meta")?
            .merkle_root()
            .expect("Legacy shreds are gone, merkle root must exist");
        let proof_index = fec_set_index.checked_div(DATA_SHREDS_PER_FEC_BLOCK as u32)?;
        let fec_set_proof = double_merkle_meta
            .proofs
            .get(usize::try_from(proof_index).ok()?)?
            .clone();

        let response = BlockIdRepairResponse::FecSetRoot {
            fec_set_root,
            fec_set_proof,
        };
        create_response_packet_batch(recycler, &response, from_addr, nonce, "run_fec_set_root")
    }
}

#[derive(Clone, Debug, Default)]
pub enum RepairHandlerType {
    #[default]
    Standard,
    Malicious(MaliciousRepairConfig),
}

impl RepairHandlerType {
    pub fn to_handler(
        &self,
        blockstore: Arc<Blockstore>,
        keypair: Arc<Keypair>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> Box<dyn RepairHandler + Send + Sync> {
        match self {
            RepairHandlerType::Standard => Box::new(StandardRepairHandler::new(blockstore)),
            RepairHandlerType::Malicious(config) => Box::new(MaliciousRepairHandler::new(
                blockstore,
                keypair,
                leader_schedule_cache,
                config.clone(),
            )),
        }
    }

    pub fn create_serve_repair(
        &self,
        blockstore: Arc<Blockstore>,
        cluster_info: Arc<ClusterInfo>,
        sharable_banks: SharableBanks,
        serve_repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
        migration_status: Arc<MigrationStatus>,
        keypair: Arc<Keypair>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> ServeRepair {
        ServeRepair::new(
            cluster_info,
            sharable_banks,
            serve_repair_whitelist,
            self.to_handler(blockstore, keypair, leader_schedule_cache),
            migration_status,
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::Rng,
        solana_entry::entry::create_ticks,
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore_meta::{BlockLocation, ParentMeta},
            get_tmp_ledger_path_auto_delete,
            shred::{ProcessShredsStats, ReedSolomonCache, Shred, Shredder},
        },
        solana_perf::packet::PacketBatchRecycler,
        std::net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    /// Creates shreds for a slot with both data and coding shreds
    fn setup_erasure_shreds(
        slot: Slot,
        parent_slot: Slot,
        num_entries: u64,
    ) -> (Vec<Shred>, Vec<Shred>) {
        let entries = create_ticks(num_entries, 0, Hash::default());
        let leader_keypair = Arc::new(Keypair::new());
        let shredder = Shredder::new(slot, parent_slot, 0, 0).unwrap();
        let chained_merkle_root = Some(Hash::new_from_array(rand::thread_rng().gen()));
        let (data_shreds, coding_shreds) = shredder.entries_to_merkle_shreds_for_tests(
            &leader_keypair,
            &entries,
            true, // is_last_in_slot
            chained_merkle_root,
            0, // next_shred_index
            0, // next_code_index
            &ReedSolomonCache::default(),
            &mut ProcessShredsStats::default(),
        );

        (data_shreds, coding_shreds)
    }

    /// Sets up a blockstore with a complete slot and all necessary metadata
    fn setup_blockstore_with_complete_slot(
        slot: Slot,
        parent_slot: Slot,
        num_entries: u64,
    ) -> (Arc<Blockstore>, Hash, Vec<Hash>) {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());

        // First, insert the parent slot so the child slot can be marked full
        let grandparent_slot = parent_slot.saturating_sub(1);
        let (parent_data_shreds, parent_coding_shreds) =
            setup_erasure_shreds(parent_slot, grandparent_slot, 10);
        blockstore
            .insert_shreds(
                parent_data_shreds
                    .into_iter()
                    .chain(parent_coding_shreds)
                    .collect::<Vec<_>>(),
                None,
                true,
            )
            .unwrap();

        // Now create the target slot
        let (data_shreds, coding_shreds) = setup_erasure_shreds(slot, parent_slot, num_entries);

        // Collect merkle roots for each FEC set
        let mut fec_set_roots = Vec::new();
        for shred in data_shreds.iter() {
            if shred.index() % (DATA_SHREDS_PER_FEC_BLOCK as u32) == 0 {
                fec_set_roots.push(shred.merkle_root().unwrap());
            }
        }

        // Create and insert ParentMeta BEFORE inserting shreds so that
        // DoubleMerkleMeta can be computed when the slot becomes full
        let parent_block_id = Hash::new_unique();
        let parent_meta = ParentMeta {
            parent_slot,
            parent_block_id,
            replay_fec_set_index: 0,
        };
        blockstore
            .put_parent_meta(slot, BlockLocation::Original, &parent_meta)
            .unwrap();

        // Verify ParentMeta was stored
        let stored_parent_meta = blockstore
            .get_parent_meta(slot, BlockLocation::Original)
            .expect("get_parent_meta should succeed")
            .expect("ParentMeta should exist after put");
        assert_eq!(
            stored_parent_meta.parent_slot, parent_slot,
            "Stored ParentMeta should match"
        );

        // Insert shreds - DoubleMerkleMeta will be computed atomically when slot becomes full
        blockstore
            .insert_shreds(
                data_shreds
                    .into_iter()
                    .chain(coding_shreds)
                    .collect::<Vec<_>>(),
                None,
                true, // is_trusted
            )
            .unwrap();

        // Verify the slot is full
        let slot_meta = blockstore.meta(slot).unwrap().unwrap();
        assert!(
            slot_meta.is_full(),
            "Slot should be full after inserting all shreds"
        );

        // Get the double merkle root (computed during shred insertion when slot became full)
        let block_id = blockstore
            .get_double_merkle_root(slot, BlockLocation::Original)
            .expect("DoubleMerkleMeta should exist for full slot with ParentMeta");

        (blockstore, block_id, fec_set_roots)
    }

    #[test]
    fn test_run_fec_set_root() {
        let slot = 1000;
        let parent_slot = 999;
        // Use many entries to ensure multiple FEC sets (each FEC set has 32 data shreds)
        let num_entries = 2000;

        let (blockstore, block_id, fec_set_roots) =
            setup_blockstore_with_complete_slot(slot, parent_slot, num_entries);

        let handler = StandardRepairHandler::new(blockstore.clone());
        let recycler = PacketBatchRecycler::default();
        let from_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let nonce = 12345;

        // Verify we have multiple FEC sets
        assert!(
            fec_set_roots.len() >= 2,
            "Should have at least 2 FEC sets for this test, got {}",
            fec_set_roots.len()
        );

        // Test each FEC set using the actual fec_set_index (0, 32, 64, ...)
        for (i, expected_root) in fec_set_roots.iter().enumerate() {
            let fec_set_index = (i * DATA_SHREDS_PER_FEC_BLOCK) as u32;

            let result = handler.run_fec_set_root(
                &recycler,
                &from_addr,
                slot,
                block_id,
                fec_set_index,
                nonce,
            );

            assert!(
                result.is_some(),
                "run_fec_set_root should succeed for fec_set_index {fec_set_index}"
            );

            // Deserialize the response and verify
            let packet_batch = result.unwrap();
            assert_eq!(packet_batch.len(), 1);

            let packet = packet_batch.iter().next().unwrap();
            let (response, response_nonce): (BlockIdRepairResponse, Nonce) =
                bincode::deserialize(packet.data(..packet.meta().size).unwrap()).unwrap();

            assert_eq!(response_nonce, nonce);
            match response {
                BlockIdRepairResponse::FecSetRoot {
                    fec_set_root,
                    fec_set_proof,
                } => {
                    assert_eq!(
                        fec_set_root, *expected_root,
                        "FEC set root should match for index {fec_set_index}"
                    );
                    assert!(
                        !fec_set_proof.is_empty(),
                        "FEC set proof should not be empty"
                    );
                }
                _ => panic!("Expected FecSetRoot response"),
            }
        }

        // Test with invalid block_id returns None
        let invalid_block_id = Hash::new_unique();
        let result =
            handler.run_fec_set_root(&recycler, &from_addr, slot, invalid_block_id, 0, nonce);
        assert!(result.is_none(), "Should return None for invalid block_id");

        // Test with out-of-bounds fec_set_index returns None
        let invalid_fec_set_index = (fec_set_roots.len() * DATA_SHREDS_PER_FEC_BLOCK) as u32;
        let result = handler.run_fec_set_root(
            &recycler,
            &from_addr,
            slot,
            block_id,
            invalid_fec_set_index,
            nonce,
        );
        assert!(
            result.is_none(),
            "Should return None for out-of-bounds fec_set_index"
        );
    }

    #[test]
    fn test_run_parent_fec_set_count() {
        let slot = 1000;
        let parent_slot = 999;
        let num_entries = 2000;

        let (blockstore, block_id, fec_set_roots) =
            setup_blockstore_with_complete_slot(slot, parent_slot, num_entries);

        let handler = StandardRepairHandler::new(blockstore.clone());
        let recycler = PacketBatchRecycler::default();
        let from_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let nonce = 12345;

        let result = handler.run_parent_fec_set_count(&recycler, &from_addr, slot, block_id, nonce);

        assert!(result.is_some(), "run_parent_fec_set_count should succeed");

        // Deserialize and verify the response
        let packet_batch = result.unwrap();
        assert_eq!(packet_batch.len(), 1);

        let packet = packet_batch.iter().next().unwrap();
        let (response, response_nonce): (BlockIdRepairResponse, Nonce) =
            bincode::deserialize(packet.data(..packet.meta().size).unwrap()).unwrap();

        assert_eq!(response_nonce, nonce);
        match response {
            BlockIdRepairResponse::ParentFecSetCount {
                fec_set_count,
                parent_info: (p_slot, p_block_id),
                parent_proof,
            } => {
                assert_eq!(
                    fec_set_count,
                    fec_set_roots.len(),
                    "FEC set count should match"
                );
                assert_eq!(p_slot, parent_slot, "Parent slot should match");

                // Verify parent_block_id matches what we set
                let parent_meta = blockstore
                    .get_parent_meta(slot, BlockLocation::Original)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    p_block_id, parent_meta.parent_block_id,
                    "Parent block ID should match"
                );

                assert!(!parent_proof.is_empty(), "Parent proof should not be empty");
            }
            _ => panic!("Expected ParentFecSetCount response"),
        }

        // Test with invalid block_id returns None
        let invalid_block_id = Hash::new_unique();
        let result =
            handler.run_parent_fec_set_count(&recycler, &from_addr, slot, invalid_block_id, nonce);
        assert!(result.is_none(), "Should return None for invalid block_id");

        // Test with non-existent slot returns None
        let result = handler.run_parent_fec_set_count(&recycler, &from_addr, 9999, block_id, nonce);
        assert!(result.is_none(), "Should return None for non-existent slot");
    }
}
