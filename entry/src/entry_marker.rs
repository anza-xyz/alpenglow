//! Entry marker types for the PoH recording pipeline.
//!
//! This module defines `EntryMarker`, a wrapper type that allows both regular entries and block
//! markers (headers, footers) to flow through the same PoH recording channel.
use crate::{
    block_component::{BlockComponent, VersionedBlockMarker},
    entry::Entry,
};

/// Errors that can occur when converting to EntryMarker
#[derive(Debug, thiserror::Error)]
pub enum EntryMarkerError {
    /// Attempted to convert an EntryBatch with multiple entries to EntryMarker
    #[error("BlockComponent::EntryBatch must contain exactly one entry, found {0}")]
    MultipleEntries(usize),
}

/// Wraps either a regular entry or a block metadata marker.
///
/// The PoH recorder uses this type to stream both transaction-containing entries and block markers
/// through a unified channel to downstream consumers, e.g., broadcast stage.
#[derive(Clone, Debug)]
pub enum EntryMarker {
    /// A regular entry containing transactions and/or ticks
    Entry(Entry),
    /// A block metadata marker (header or footer)
    Marker(VersionedBlockMarker),
}

impl EntryMarker {
    /// Creates an EntryMarker wrapping an entry.
    pub fn new_entry(entry: Entry) -> Self {
        EntryMarker::Entry(entry)
    }

    /// Creates an EntryMarker wrapping a block marker.
    pub fn new_marker(marker: VersionedBlockMarker) -> Self {
        EntryMarker::Marker(marker)
    }

    /// Consumes self and returns the inner Entry if this is an Entry variant.
    ///
    /// Returns `None` if this is a Marker variant.
    pub fn into_entry(self) -> Option<Entry> {
        match self {
            EntryMarker::Entry(entry) => Some(entry),
            _ => None,
        }
    }

    /// Returns a reference to the inner Entry if this is an Entry variant.
    ///
    /// Returns `None` if this is a Marker variant.
    pub fn as_entry(&self) -> Option<&Entry> {
        match self {
            EntryMarker::Entry(entry) => Some(entry),
            _ => None,
        }
    }

    /// Consumes self and returns the inner VersionedBlockMarker if this is a Marker variant.
    ///
    /// Returns `None` if this is an Entry variant.
    pub fn into_marker(self) -> Option<VersionedBlockMarker> {
        match self {
            EntryMarker::Marker(marker) => Some(marker),
            _ => None,
        }
    }

    /// Returns a reference to the inner VersionedBlockMarker if this is a Marker variant.
    ///
    /// Returns `None` if this is an Entry variant.
    pub fn as_marker(&self) -> Option<&VersionedBlockMarker> {
        match self {
            EntryMarker::Marker(marker) => Some(marker),
            _ => None,
        }
    }
}

/// Converts an Entry into an EntryMarker.
impl From<Entry> for EntryMarker {
    fn from(entry: Entry) -> Self {
        EntryMarker::Entry(entry)
    }
}

/// Converts a VersionedBlockMarker into an EntryMarker.
impl From<VersionedBlockMarker> for EntryMarker {
    fn from(marker: VersionedBlockMarker) -> Self {
        EntryMarker::Marker(marker)
    }
}

/// Converts a BlockComponent into an EntryMarker.
///
/// # Errors
///
/// Returns `EntryMarkerError::MultipleEntries` if the BlockComponent is an EntryBatch
/// containing more than one entry, as EntryMarker can only wrap a single entry.
impl TryFrom<BlockComponent> for EntryMarker {
    type Error = EntryMarkerError;

    fn try_from(component: BlockComponent) -> Result<Self, Self::Error> {
        match component {
            BlockComponent::EntryBatch(entries) => {
                if entries.len() != 1 {
                    return Err(EntryMarkerError::MultipleEntries(entries.len()));
                }
                Ok(EntryMarker::Entry(entries[0].clone()))
            }
            BlockComponent::BlockMarker(marker) => Ok(EntryMarker::Marker(marker)),
        }
    }
}

/// Converts an EntryMarker into a BlockComponent.
///
/// Entry variants become single-element EntryBatch components.
/// Marker variants become BlockMarker components.
impl From<EntryMarker> for BlockComponent {
    fn from(entry_marker: EntryMarker) -> Self {
        match entry_marker {
            EntryMarker::Entry(entry) => BlockComponent::EntryBatch(vec![entry]),
            EntryMarker::Marker(marker) => BlockComponent::BlockMarker(marker),
        }
    }
}
