use {
    super::{repair_handler::RepairHandler, repair_response},
    solana_clock::Slot,
    solana_hash::Hash,
    solana_ledger::{blockstore::Blockstore, shred::Nonce},
    solana_perf::packet::Packet,
    std::{net::SocketAddr, sync::Arc},
};

pub(crate) struct StandardRepairHandler {
    blockstore: Arc<Blockstore>,
}

impl StandardRepairHandler {
    pub(crate) fn new(blockstore: Arc<Blockstore>) -> Self {
        Self { blockstore }
    }
}

impl RepairHandler for StandardRepairHandler {
    fn blockstore(&self) -> &Blockstore {
        &self.blockstore
    }

    fn repair_response_packet(
        &self,
        slot: Slot,
        shred_index: u64,
        block_id: Option<Hash>,
        dest: &SocketAddr,
        nonce: Nonce,
    ) -> Option<Packet> {
        match block_id {
            None => repair_response::repair_response_packet(
                self.blockstore.as_ref(),
                slot,
                shred_index,
                dest,
                nonce,
            ),
            Some(block_id) => {
                let location = self.blockstore().get_block_location(slot, block_id)?;
                let shred = self
                    .blockstore()
                    .get_data_shred_from_location(slot, shred_index, location)
                    .expect("Blockstore could not get data shred")?;
                repair_response::repair_response_packet_from_bytes(shred, dest, nonce)
            }
        }
    }
}
