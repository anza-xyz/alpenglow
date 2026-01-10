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
    solana_ledger::{
        ancestor_iterator::{AncestorIterator, AncestorIteratorWithHash},
        blockstore::Blockstore,
        shred::{ErasureSetId, Nonce},
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
            .expect("Unable to fetch merkle root meta")
            .expect("Slot is full, MerkleRootMeta must exist")
            .merkle_root()
            .expect("Legacy shreds are gone, merkle root must exist");
        let fec_set_proof = double_merkle_meta
            .proofs
            .get(usize::try_from(fec_set_index).ok()?)?
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
    pub fn to_handler(&self, blockstore: Arc<Blockstore>) -> Box<dyn RepairHandler + Send + Sync> {
        match self {
            RepairHandlerType::Standard => Box::new(StandardRepairHandler::new(blockstore)),
            RepairHandlerType::Malicious(config) => {
                Box::new(MaliciousRepairHandler::new(blockstore, *config))
            }
        }
    }

    pub fn create_serve_repair(
        &self,
        blockstore: Arc<Blockstore>,
        cluster_info: Arc<ClusterInfo>,
        sharable_banks: SharableBanks,
        serve_repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
        migration_status: Arc<MigrationStatus>,
    ) -> ServeRepair {
        ServeRepair::new(
            cluster_info,
            sharable_banks,
            serve_repair_whitelist,
            self.to_handler(blockstore),
            migration_status,
        )
    }
}
