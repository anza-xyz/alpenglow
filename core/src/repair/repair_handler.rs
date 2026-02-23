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
    agave_votor_messages::migration::MigrationStatus,
    bincode::serialize,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::{
        ancestor_iterator::{AncestorIterator, AncestorIteratorWithHash},
        blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
        shred::{DATA_SHREDS_PER_FEC_BLOCK, ErasureSetId, Nonce},
    },
    solana_perf::packet::{Packet, PacketBatch, PacketBatchRecycler, RecycledPacketBatch},
    solana_poh::poh_recorder::SharedLeaderState,
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::SharableBanks,
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
    Some(RecycledPacketBatch::new_with_recycler_data(recycler, debug_label, vec![packet]).into())
}

pub trait RepairHandler {
    fn blockstore(&self) -> &Blockstore;

    fn repair_response_packet(
        &self,
        slot: Slot,
        shred_index: u64,
        dest: &SocketAddr,
        nonce: Nonce,
    ) -> Option<Packet>;

    fn run_window_request(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        shred_index: u64,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        // Try to find the requested index in one of the slots
        let packet = self.repair_response_packet(slot, shred_index, from_addr, nonce)?;
        Some(
            RecycledPacketBatch::new_with_recycler_data(
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
        let dmr = self.blockstore().get_double_merkle_root(slot)?;
        if dmr != block_id {
            return None;
        }
        self.run_window_request(recycler, from_addr, slot, shred_index, nonce)
    }

    fn run_highest_window_request(
        &self,
        recycler: &PacketBatchRecycler,
        from_addr: &SocketAddr,
        slot: Slot,
        highest_index: u64,
        nonce: Nonce,
    ) -> Option<PacketBatch> {
        // Try to find the requested index in one of the slots
        let meta = self.blockstore().meta(slot).ok()??;
        if meta.received > highest_index {
            // meta.received must be at least 1 by this point
            let packet = self.repair_response_packet(slot, meta.received - 1, from_addr, nonce)?;
            return Some(
                RecycledPacketBatch::new_with_recycler_data(
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
    ) -> Option<PacketBatch>;

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
        let dmr = self.blockstore().get_double_merkle_root(slot)?;
        if dmr != block_id {
            return None;
        }

        debug_assert!(self.blockstore().meta(slot).unwrap().unwrap().is_full());

        let double_merkle_meta = self
            .blockstore()
            .get_double_merkle_meta(slot)
            .expect("Unable to fetch double merkle meta")
            .expect("If location exists, double merkle meta must be populated");
        let fec_set_count = double_merkle_meta.fec_set_count;

        let parent_meta = self
            .blockstore()
            .get_parent_meta(slot)
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
        let dmr = self.blockstore().get_double_merkle_root(slot)?;
        if dmr != block_id {
            return None;
        }

        debug_assert!(self.blockstore().meta(slot).unwrap().unwrap().is_full());

        let double_merkle_meta = self
            .blockstore()
            .get_double_merkle_meta(slot)
            .expect("Unable to fetch double merkle meta")
            .expect("If location exists, double merkle meta must be populated");

        let fec_set_root = self
            .blockstore()
            .merkle_root_meta_for_fec_set(ErasureSetId::new(slot, fec_set_index))
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
        leader_state: SharedLeaderState,
        migration_status: Arc<MigrationStatus>,
        keypair: Arc<Keypair>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> ServeRepair {
        ServeRepair::new_with_leader_state(
            cluster_info,
            sharable_banks,
            serve_repair_whitelist,
            self.to_handler(blockstore, keypair, leader_schedule_cache),
            leader_state,
            migration_status,
        )
    }
}
