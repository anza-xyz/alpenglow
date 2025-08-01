//! Events that can be sent from the blockstore ingest thread
//! to the shred resolver service, for informing logic that decides
//! to dump or repair shreds.

use {
    crate::{blockstore_meta::BlockLocation, shred::ShredType},
    crossbeam_channel::{Receiver, Sender},
    solana_clock::Slot,
    solana_hash::Hash,
};

pub type ShredEventSender = Sender<ShredEvent>;
pub type ShredEventReceiver = Receiver<ShredEvent>;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ShredEvent {
    CompletedFECSet {
        location: BlockLocation,
        slot: Slot,
        fec_set_index: u32,
        is_last_in_slot: bool,
    },

    NewFECSet {
        location: BlockLocation,
        slot: Slot,
        fec_set_index: u32,
        merkle_root: Hash,
        chained_merkle_root: Hash,
    },

    MerkleRootConflict {
        location: BlockLocation,
        slot: Slot,
        fec_set_index: u32,
        conflicting_shred_index: u32,
        conflicting_shred_type: ShredType,
    },

    ChainedMerkleRootConflict {
        location: BlockLocation,
        slot: Slot,
        lower_fec_set_index: u32,
        lower_merkle_root: Hash,
        higher_fec_set_index: u32,
        higher_chained_merkle_root: Hash,
    },
}
