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
    /// Creates a new EntryBatch with entries only.
    pub fn new(entries: Vec<Entry>) -> Result<Self, String> {
        if entries.is_empty() {
            return Err("EntryBatch with entries cannot be empty".to_string());
        }

        Ok(Self {
            entries,
            special: None,
        })
    }

    /// Creates a new EntryBatch with special data only.
    pub fn new_special(special: VersionedSpecialEntry) -> Self {
        Self {
            entries: Vec::new(),
            special: Some(special),
        }
    }

    /// Validates the EntryBatch invariants.
    fn validate(&self) -> Result<(), String> {
        match (self.entries.is_empty(), self.special.is_some()) {
            (true, false) => Err("EntryBatch must have either entries or special data".to_string()),
            (false, true) => {
                Err("EntryBatch cannot have both entries and special data".to_string())
            }
            _ => Ok(()),
        }
    }

    /// Converts to bytes with validation.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        self.validate()
            .map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e)))?;

        let mut buffer = Vec::new();

        // Write entries length (8 bytes, little-endian)
        buffer.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());

        // Write entries
        for entry in &self.entries {
            buffer.extend_from_slice(&bincode::serialize(entry)?);
        }

        // Write special entry if present
        if let Some(ref special) = self.special {
            buffer.extend_from_slice(&special.to_bytes()?);
        }

        Ok(buffer)
    }

    /// Deserializes from bytes with validation.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        const HEADER_SIZE: usize = 8;

        if data.len() < HEADER_SIZE {
            return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
        }

        // Read entries length
        let entries_len = u64::from_le_bytes(
            data[..HEADER_SIZE]
                .try_into()
                .map_err(|_| bincode::Error::new(bincode::ErrorKind::SizeLimit))?,
        );

        let mut cursor = HEADER_SIZE;
        let mut entries = Vec::with_capacity(entries_len as usize);

        // Deserialize entries
        for _ in 0..entries_len {
            if cursor >= data.len() {
                return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
            }

            let entry: Entry = bincode::deserialize(&data[cursor..])?;
            let entry_size = bincode::serialized_size(&entry)? as usize;
            entries.push(entry);
            cursor += entry_size;
        }

        // Check for special entry
        let special = if cursor < data.len() {
            Some(VersionedSpecialEntry::from_bytes(&data[cursor..])?)
        } else {
            None
        };

        let batch = Self { entries, special };
        batch
            .validate()
            .map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e)))?;

        Ok(batch)
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

impl<'de> Deserialize<'de> for EntryBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntryBatchVisitor;

        impl<'de> Visitor<'de> for EntryBatchVisitor {
            type Value = EntryBatch;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a serialized EntryBatch byte stream")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<EntryBatch, E>
            where
                E: de::Error,
            {
                EntryBatch::from_bytes(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(EntryBatchVisitor)
    }
}

impl VersionedSpecialEntry {
    /// Creates a new versioned special entry.
    pub fn new(version: u16, inner: SpecialEntry) -> Self {
        Self { version, inner }
    }

    /// Serializes to bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.version.to_le_bytes());
        buffer.extend_from_slice(&self.inner.to_bytes()?);
        Ok(buffer)
    }

    /// Deserializes from bytes.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        const VERSION_SIZE: usize = 2;

        if data.len() < VERSION_SIZE {
            return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
        }

        let version = u16::from_le_bytes(
            data[..VERSION_SIZE]
                .try_into()
                .map_err(|_| bincode::Error::new(bincode::ErrorKind::SizeLimit))?,
        );

        let inner = SpecialEntry::from_bytes(&data[VERSION_SIZE..])?;
        Ok(Self { version, inner })
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

impl<'de> Deserialize<'de> for VersionedSpecialEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionedSpecialEntryVisitor;

        impl<'de> Visitor<'de> for VersionedSpecialEntryVisitor {
            type Value = VersionedSpecialEntry;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a serialized VersionedSpecialEntry byte stream")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<VersionedSpecialEntry, E>
            where
                E: de::Error,
            {
                VersionedSpecialEntry::from_bytes(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(VersionedSpecialEntryVisitor)
    }
}

impl SpecialEntry {
    /// Serializes by delegating to inner type.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        match self {
            Self::V0(entry) | Self::Current(entry) => entry.to_bytes(),
        }
    }

    /// Deserializes, always creating Current variant for forward compatibility.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        let entry = SpecialEntryV0::from_bytes(data)?;
        Ok(Self::Current(entry))
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

impl<'de> Deserialize<'de> for SpecialEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SpecialEntryVisitor;

        impl<'de> Visitor<'de> for SpecialEntryVisitor {
            type Value = SpecialEntry;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a serialized SpecialEntry byte stream")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<SpecialEntry, E>
            where
                E: de::Error,
            {
                SpecialEntry::from_bytes(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(SpecialEntryVisitor)
    }
}

impl SpecialEntryV0 {
    /// Serializes the special entry.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        match self {
            Self::Empty => Ok(Vec::new()),
            Self::ParentReadyUpdate(update) => update.to_bytes(),
        }
    }

    /// Deserializes based on data length.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.is_empty() {
            Ok(Self::Empty)
        } else {
            let update = VersionedParentReadyUpdate::from_bytes(data)?;
            Ok(Self::ParentReadyUpdate(update))
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

impl<'de> Deserialize<'de> for SpecialEntryV0 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SpecialEntryV0Visitor;

        impl<'de> Visitor<'de> for SpecialEntryV0Visitor {
            type Value = SpecialEntryV0;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a serialized SpecialEntryV0 byte stream")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<SpecialEntryV0, E>
            where
                E: de::Error,
            {
                SpecialEntryV0::from_bytes(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(SpecialEntryV0Visitor)
    }
}

impl VersionedParentReadyUpdate {
    /// Creates a new versioned parent ready update.
    pub fn new(version: u8, inner: ParentReadyUpdate) -> Self {
        Self { version, inner }
    }

    /// Serializes to bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut buffer = Vec::new();
        buffer.push(self.version);
        buffer.extend_from_slice(&self.inner.to_bytes()?);
        Ok(buffer)
    }

    /// Deserializes from bytes.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.is_empty() {
            return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
        }

        let version = data[0];
        let inner = ParentReadyUpdate::from_bytes(&data[1..])?;
        Ok(Self { version, inner })
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

impl<'de> Deserialize<'de> for VersionedParentReadyUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionedParentReadyUpdateVisitor;

        impl<'de> Visitor<'de> for VersionedParentReadyUpdateVisitor {
            type Value = VersionedParentReadyUpdate;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a serialized VersionedParentReadyUpdate byte stream")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<VersionedParentReadyUpdate, E>
            where
                E: de::Error,
            {
                VersionedParentReadyUpdate::from_bytes(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(VersionedParentReadyUpdateVisitor)
    }
}

impl ParentReadyUpdate {
    /// Serializes using bincode for inner data.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        match self {
            Self::V0(update) | Self::Current(update) => bincode::serialize(update),
        }
    }

    /// Deserializes, always creating Current variant.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        let update: ParentReadyUpdateV0 = bincode::deserialize(data)?;
        Ok(Self::Current(update))
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

impl<'de> Deserialize<'de> for ParentReadyUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ParentReadyUpdateVisitor;

        impl<'de> Visitor<'de> for ParentReadyUpdateVisitor {
            type Value = ParentReadyUpdate;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a serialized ParentReadyUpdate byte stream")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<ParentReadyUpdate, E>
            where
                E: de::Error,
            {
                ParentReadyUpdate::from_bytes(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(ParentReadyUpdateVisitor)
    }
}
