use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

use solana_clock::Slot;
use solana_hash::Hash;

use crate::entry::Entry;

/// A batch of entries with optional special metadata.
///
/// EntryBatch enforces a strict serialization rule:
/// - Either `entries.len() > 0` and `special` is `None` (regular entry batch)
/// - Or `entries.len() == 0` and `special` is `Some` (special-only batch)
///
/// Invalid combinations will result in serialization errors.
///
/// # Serialization Format
/// The binary format is:
/// ```
/// [8 bytes: entries_len as u64 little-endian]
/// [variable: serialized entries using bincode]
/// [variable: optional VersionedSpecialEntry if present]
/// ```
pub struct EntryBatch {
    entries: Vec<Entry>,
    special: Option<VersionedSpecialEntry>,
}

/// A versioned wrapper around SpecialEntry for backward compatibility.
///
/// # Serialization Format
/// ```
/// [2 bytes: version as u16 little-endian]
/// [variable: serialized SpecialEntry]
/// ```
pub struct VersionedSpecialEntry {
    version: u16,
    inner: SpecialEntry,
}

/// An enum representing different versions of special entries.
/// Currently both V0 and Current map to the same underlying type for compatibility.
///
/// # Serialization Format
/// Delegates to the inner SpecialEntryV0's serialization format.
pub enum SpecialEntry {
    V0(SpecialEntryV0),
    Current(SpecialEntryV0),
}

/// Version 0 of special entry types.
///
/// # Serialization Format
/// - Empty: 0 bytes (empty buffer)
/// - ParentReadyUpdate: delegates to VersionedParentReadyUpdate format
pub enum SpecialEntryV0 {
    Empty,
    ParentReadyUpdate(VersionedParentReadyUpdate),
}

/// A versioned wrapper around ParentReadyUpdate for backward compatibility.
///
/// # Serialization Format
/// ```
/// [1 byte: version as u8]
/// [variable: serialized ParentReadyUpdate]
/// ```
pub struct VersionedParentReadyUpdate {
    version: u8,
    inner: ParentReadyUpdate,
}

/// An enum representing different versions of parent ready updates.
/// Currently both V0 and Current map to the same underlying type for compatibility.
///
/// # Serialization Format
/// Delegates to bincode serialization of the inner ParentReadyUpdateV0.
pub enum ParentReadyUpdate {
    V0(ParentReadyUpdateV0),
    Current(ParentReadyUpdateV0),
}

/// Version 0 of parent ready update data.
/// Contains information about a new parent slot and block ID.
///
/// # Serialization Format
/// Uses bincode serialization for all fields.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct ParentReadyUpdateV0 {
    version: u8,
    new_parent_slot: Slot,
    new_parent_block_id: Hash,
}

impl EntryBatch {
    /// Validates the EntryBatch invariants and converts to bytes.
    ///
    /// # Errors
    /// - If entries.len() > 0 and special is Some
    /// - If entries.len() == 0 and special is None
    /// - If bincode serialization fails
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        // Enforce serialization rules
        let entries_len = self.entries.len();
        match (entries_len > 0, self.special.is_some()) {
            (true, true) => {
                return Err(bincode::Error::new(bincode::ErrorKind::Custom(
                    "EntryBatch cannot have both entries and special data".to_string(),
                )));
            }
            (false, false) => {
                return Err(bincode::Error::new(bincode::ErrorKind::Custom(
                    "EntryBatch must have either entries or special data".to_string(),
                )));
            }
            _ => {} // Valid combinations: (true, false) or (false, true)
        }

        let mut buffer = Vec::new();

        // Write entries length (8 bytes, little-endian)
        let entries_len = self.entries.len() as u64;
        buffer.extend_from_slice(&entries_len.to_le_bytes());

        // Write each entry using bincode
        for entry in &self.entries {
            let entry_bytes = bincode::serialize(entry)?;
            buffer.extend_from_slice(&entry_bytes);
        }

        // Write special entry if present
        if let Some(special) = &self.special {
            let special_bytes = special.to_bytes()?;
            buffer.extend_from_slice(&special_bytes);
        }

        Ok(buffer)
    }

    /// Deserializes an EntryBatch from bytes and validates invariants.
    ///
    /// # Errors
    /// - If data is too short (< 8 bytes for length header)
    /// - If the deserialized data violates EntryBatch rules
    /// - If bincode deserialization fails
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.len() < 8 {
            return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
        }

        // Read entries length from first 8 bytes
        let entries_len = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        let mut cursor = 8;
        let mut entries = Vec::new();

        // Deserialize each entry
        for _ in 0..entries_len {
            let entry: Entry = bincode::deserialize(&data[cursor..])?;
            let entry_size = bincode::serialized_size(&entry)? as usize;
            entries.push(entry);
            cursor += entry_size;
        }

        // Check for remaining data (special entry)
        let special = if cursor < data.len() {
            Some(VersionedSpecialEntry::from_bytes(&data[cursor..])?)
        } else {
            None
        };

        // Validate invariants after deserialization
        match (entries.len() > 0, special.is_some()) {
            (true, true) => {
                return Err(bincode::Error::new(bincode::ErrorKind::Custom(
                    "Deserialized EntryBatch cannot have both entries and special data".to_string(),
                )));
            }
            (false, false) => {
                return Err(bincode::Error::new(bincode::ErrorKind::Custom(
                    "Deserialized EntryBatch must have either entries or special data".to_string(),
                )));
            }
            _ => {} // Valid combinations
        }

        Ok(EntryBatch { entries, special })
    }
}

impl Serialize for EntryBatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

struct EntryBatchVisitor;

impl<'de> Visitor<'de> for EntryBatchVisitor {
    type Value = EntryBatch;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a serialized EntryBatch byte stream")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<EntryBatch, E>
    where
        E: de::Error,
    {
        EntryBatch::from_bytes(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for EntryBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(EntryBatchVisitor)
    }
}

impl VersionedSpecialEntry {
    /// Serializes the versioned special entry to bytes.
    /// Format: [2 bytes version] + [inner SpecialEntry bytes]
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.version.to_le_bytes());

        let inner_bytes = self.inner.to_bytes()?;
        buffer.extend_from_slice(&inner_bytes);

        Ok(buffer)
    }

    /// Deserializes a versioned special entry from bytes.
    /// Expects at least 2 bytes for the version header.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.len() < 2 {
            return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
        }

        let version = u16::from_le_bytes([data[0], data[1]]);
        let inner = SpecialEntry::from_bytes(&data[2..])?;
        Ok(VersionedSpecialEntry { version, inner })
    }
}

impl Serialize for VersionedSpecialEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

struct VersionedSpecialEntryVisitor;

impl<'de> Visitor<'de> for VersionedSpecialEntryVisitor {
    type Value = VersionedSpecialEntry;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a serialized VersionedSpecialEntry byte stream")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<VersionedSpecialEntry, E>
    where
        E: de::Error,
    {
        VersionedSpecialEntry::from_bytes(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for VersionedSpecialEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(VersionedSpecialEntryVisitor)
    }
}

impl SpecialEntry {
    /// Serializes the special entry by delegating to the inner SpecialEntryV0.
    /// Both V0 and Current variants use the same serialization format.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        match self {
            SpecialEntry::V0(entry) | SpecialEntry::Current(entry) => entry.to_bytes(),
        }
    }

    /// Deserializes a special entry, always creating a Current variant for backward compatibility.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        let entry = SpecialEntryV0::from_bytes(data)?;
        Ok(SpecialEntry::Current(entry))
    }
}

impl Serialize for SpecialEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

struct SpecialEntryVisitor;

impl<'de> Visitor<'de> for SpecialEntryVisitor {
    type Value = SpecialEntry;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a serialized SpecialEntry byte stream")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<SpecialEntry, E>
    where
        E: de::Error,
    {
        SpecialEntry::from_bytes(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for SpecialEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SpecialEntryVisitor)
    }
}

impl SpecialEntryV0 {
    /// Serializes the special entry V0.
    /// Empty variant produces 0 bytes, ParentReadyUpdate delegates to its serialization.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        match self {
            SpecialEntryV0::Empty => Ok(Vec::new()),
            SpecialEntryV0::ParentReadyUpdate(update) => update.to_bytes(),
        }
    }

    /// Deserializes a special entry V0.
    /// Empty data creates Empty variant, non-empty data creates ParentReadyUpdate.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.is_empty() {
            Ok(SpecialEntryV0::Empty)
        } else {
            let update = VersionedParentReadyUpdate::from_bytes(data)?;
            Ok(SpecialEntryV0::ParentReadyUpdate(update))
        }
    }
}

impl Serialize for SpecialEntryV0 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

struct SpecialEntryV0Visitor;

impl<'de> Visitor<'de> for SpecialEntryV0Visitor {
    type Value = SpecialEntryV0;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a serialized SpecialEntryV0 byte stream")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<SpecialEntryV0, E>
    where
        E: de::Error,
    {
        SpecialEntryV0::from_bytes(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for SpecialEntryV0 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SpecialEntryV0Visitor)
    }
}

impl VersionedParentReadyUpdate {
    /// Serializes the versioned parent ready update.
    /// Format: [1 byte version] + [inner ParentReadyUpdate bytes]
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.version.to_le_bytes());

        let inner_bytes = self.inner.to_bytes()?;
        buffer.extend_from_slice(&inner_bytes);

        Ok(buffer)
    }

    /// Deserializes a versioned parent ready update.
    /// Expects at least 1 byte for the version.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.is_empty() {
            return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
        }
        let version = data[0];
        let inner = ParentReadyUpdate::from_bytes(&data[1..])?;
        Ok(VersionedParentReadyUpdate { version, inner })
    }
}

impl Serialize for VersionedParentReadyUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

struct VersionedParentReadyUpdateVisitor;

impl<'de> Visitor<'de> for VersionedParentReadyUpdateVisitor {
    type Value = VersionedParentReadyUpdate;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a serialized VersionedParentReadyUpdate byte stream")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<VersionedParentReadyUpdate, E>
    where
        E: de::Error,
    {
        VersionedParentReadyUpdate::from_bytes(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for VersionedParentReadyUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(VersionedParentReadyUpdateVisitor)
    }
}

impl ParentReadyUpdate {
    /// Serializes the parent ready update using bincode for the inner data.
    /// Both V0 and Current variants use the same serialization format.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        match self {
            ParentReadyUpdate::V0(update) | ParentReadyUpdate::Current(update) => {
                bincode::serialize(update)
            }
        }
    }

    /// Deserializes a parent ready update, always creating a Current variant.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        let update: ParentReadyUpdateV0 = bincode::deserialize(data)?;
        Ok(ParentReadyUpdate::Current(update))
    }
}

impl Serialize for ParentReadyUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

struct ParentReadyUpdateVisitor;

impl<'de> Visitor<'de> for ParentReadyUpdateVisitor {
    type Value = ParentReadyUpdate;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a serialized ParentReadyUpdate byte stream")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<ParentReadyUpdate, E>
    where
        E: de::Error,
    {
        ParentReadyUpdate::from_bytes(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for ParentReadyUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(ParentReadyUpdateVisitor)
    }
}
