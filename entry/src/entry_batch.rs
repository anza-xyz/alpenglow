use solana_clock::Slot;
use solana_hash::Hash;

use crate::entry::Entry;

pub struct EntryBatch {
    entries: Vec<Entry>,
    special: Option<VersionedSpecialEntry>,
}

pub struct VersionedSpecialEntry {
    version: u16,
    inner: SpecialEntry,
}

pub enum SpecialEntry {
    V0(SpecialEntryV0),
    Current(SpecialEntryV0),
}

pub enum SpecialEntryV0 {
    Empty,
    ParentReadyUpdate(VersionedParentReadyUpdate),
}

pub struct VersionedParentReadyUpdate {
    version: u8,
    inner: ParentReadyUpdate,
}

pub enum ParentReadyUpdate {
    V0(ParentReadyUpdateV0),
    Current(ParentReadyUpdateV0),
}

pub struct ParentReadyUpdateV0 {
    version: u8,
    new_parent_slot: Slot,
    new_parent_block_id: Hash,
}
