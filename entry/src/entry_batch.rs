use {
    crate::entry::Entry,
    serde::{
        de::{self, Visitor},
        Deserialize, Deserializer, Serialize, Serializer,
    },
    solana_clock::Slot,
    solana_hash::Hash,
    std::fmt,
};

/// A batch of entries with optional special metadata.
///
/// EntryBatch enforces validation rules but allows flexible combinations:
/// - Entries can be present with or without special data
/// - Special data can be present with or without entries
/// - Only restriction: entries cannot be both present and special data present simultaneously
/// - Empty entries with no special data is allowed
///
/// # Serialization Format
/// The binary format is:
/// [8 bytes: entries_len as u64 little-endian]
/// [variable: serialized entries using bincode, concatenated]
/// [variable: optional VersionedSpecialEntry if present]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryBatch {
    EntryVec(Vec<Entry>),
    Special(VersionedSpecialEntry),
}

/// A versioned wrapper around SpecialEntry for backward compatibility.
///
/// # Serialization Format
/// [2 bytes: version as u16 little-endian]
/// [variable: serialized SpecialEntry]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedSpecialEntry {
    pub version: u16,
    pub inner: SpecialEntry,
}

/// An enum representing different versions of special entries.
/// Both V0 and Current variants contain the same underlying type for compatibility.
/// During deserialization, always creates Current variant for forward compatibility.
///
/// # Serialization Format
/// Delegates to the inner SpecialEntryV0's serialization format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecialEntry {
    V0(SpecialEntryV0),
    Current(SpecialEntryV0),
}

/// Version 0 of special entry types.
/// Currently only supports ParentReadyUpdate variant.
///
/// # Serialization Format
/// - ParentReadyUpdate: delegates to VersionedParentReadyUpdate format
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecialEntryV0 {
    ParentReadyUpdate(VersionedParentReadyUpdate),
}

/// A versioned wrapper around ParentReadyUpdate for backward compatibility.
///
/// # Serialization Format
/// [1 byte: version as u8]
/// [variable: serialized ParentReadyUpdate]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedParentReadyUpdate {
    pub version: u8,
    pub inner: ParentReadyUpdate,
}

/// An enum representing different versions of parent ready updates.
/// Both V0 and Current variants contain the same underlying type for compatibility.
/// During deserialization, always creates Current variant for forward compatibility.
///
/// # Serialization Format
/// Delegates to bincode serialization of the inner ParentReadyUpdateV0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentReadyUpdate {
    V0(ParentReadyUpdateV0),
    Current(ParentReadyUpdateV0),
}

/// Version 0 of parent ready update data.
/// Contains information about a new parent slot and block ID, plus a version field.
///
/// # Serialization Format
/// Uses bincode serialization for all fields (version, new_parent_slot, new_parent_block_id).
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct ParentReadyUpdateV0 {
    pub version: u8,
    pub new_parent_slot: Slot,
    pub new_parent_block_id: Hash,
}

impl Default for EntryBatch {
    fn default() -> Self {
        Self::EntryVec(Vec::new())
    }
}

impl EntryBatch {
    /// Maximum number of entries allowed in an EntryBatch.
    ///
    /// TODO(karthik): lower this to a reasonable value.
    const MAX_ENTRIES: usize = u32::MAX as usize;

    /// Creates a new EntryBatch with entries only.
    /// Requires at least one entry and validates entry count limits.
    pub fn new(entries: Vec<Entry>) -> Result<Self, String> {
        if entries.is_empty() {
            return Err("EntryBatch with entries cannot be empty".to_string());
        }

        Self::validate_entries_length(entries.len())?;
        Ok(Self::EntryVec(entries))
    }

    /// Creates a new EntryBatch with special data only.
    pub const fn new_special(special: VersionedSpecialEntry) -> Self {
        Self::Special(special)
    }

    /// Returns the entries from the EntryBatch.
    pub fn entries(&self) -> &[Entry] {
        match self {
            Self::EntryVec(entries) => entries,
            Self::Special(_) => &[],
        }
    }

    /// Returns the special data from the EntryBatch if present.
    pub const fn special(&self) -> Option<&VersionedSpecialEntry> {
        match self {
            Self::EntryVec(_) => None,
            Self::Special(special) => Some(special),
        }
    }

    /// Returns true if the EntryBatch contains entries.
    pub const fn is_entry_vec(&self) -> bool {
        matches!(self, Self::EntryVec(_))
    }

    /// Returns true if the EntryBatch contains special data.
    pub const fn is_special(&self) -> bool {
        matches!(self, Self::Special(_))
    }

    /// Validates that entries length doesn't exceed maximum allowed.
    fn validate_entries_length(len: usize) -> Result<(), String> {
        if len >= Self::MAX_ENTRIES {
            Err(format!(
                "EntryBatch entries length {len} exceeds maximum {}",
                Self::MAX_ENTRIES
            ))
        } else {
            Ok(())
        }
    }

    /// Converts to bytes with validation.
    /// Serializes entries length as u64, followed by bincode-serialized entries,
    /// followed by optional special entry data.
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut buffer = Vec::new();

        match self {
            Self::EntryVec(entries) => {
                Self::validate_entries_length(entries.len())
                    .map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e)))?;

                // Write entries length (8 bytes, little-endian)
                buffer.extend_from_slice(&(entries.len() as u64).to_le_bytes());

                // Write entries
                for entry in entries {
                    buffer.extend_from_slice(&bincode::serialize(entry)?);
                }
            }
            Self::Special(special) => {
                // Write entries length as 0 (8 bytes, little-endian)
                buffer.extend_from_slice(&0u64.to_le_bytes());
                // Write special entry
                buffer.extend_from_slice(&special.to_bytes()?);
            }
        }

        Ok(buffer)
    }

    /// Deserializes from bytes with validation.
    /// Reads entries length, deserializes each entry individually by calculating
    /// serialized size, then checks for remaining data to deserialize special entry.
    ///
    /// TODO(karthik): fuzz test this function.
    pub fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        const HEADER_SIZE: usize = 8;

        let entries_len_bytes = data
            .get(..HEADER_SIZE)
            .ok_or_else(|| bincode::Error::new(bincode::ErrorKind::SizeLimit))?;

        let entries_len = u64::from_le_bytes(
            entries_len_bytes
                .try_into()
                .map_err(|_| bincode::Error::new(bincode::ErrorKind::SizeLimit))?,
        );

        // Check if entries length exceeds maximum
        if entries_len as usize >= Self::MAX_ENTRIES {
            return Err(bincode::Error::new(bincode::ErrorKind::Custom(format!(
                "EntryBatch entries length {entries_len} exceeds maximum {}",
                Self::MAX_ENTRIES
            ))));
        }

        let mut cursor = HEADER_SIZE;
        let mut entries = Vec::with_capacity(entries_len as usize);

        // Deserialize entries
        for _ in 0..entries_len {
            let remaining_data = data
                .get(cursor..)
                .ok_or_else(|| bincode::Error::new(bincode::ErrorKind::SizeLimit))?;

            let entry: Entry = bincode::deserialize(remaining_data)?;
            let entry_size = bincode::serialized_size(&entry)? as usize;
            entries.push(entry);
            cursor += entry_size;
        }

        // Check for special entry
        let batch = if let Some(remaining_data) = data.get(cursor..) {
            if !remaining_data.is_empty() {
                // There's remaining data, so it should be special data
                let special = VersionedSpecialEntry::from_bytes(remaining_data)?;
                if entries.is_empty() {
                    Self::Special(special)
                } else {
                    // This would be an invalid state - both entries and special data
                    return Err(bincode::Error::new(bincode::ErrorKind::Custom(
                        "EntryBatch cannot have both entries and special data".to_string(),
                    )));
                }
            } else {
                Self::EntryVec(entries)
            }
        } else {
            Self::EntryVec(entries)
        };

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

        impl Visitor<'_> for EntryBatchVisitor {
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
    /// Creates a new versioned special entry with the provided version and inner data.
    pub const fn new(version: u16, inner: SpecialEntry) -> Self {
        Self { version, inner }
    }

    /// Serializes to bytes by writing version as u16 little-endian followed by inner data.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.version.to_le_bytes());
        buffer.extend_from_slice(&self.inner.to_bytes()?);
        Ok(buffer)
    }

    /// Deserializes from bytes by reading u16 version followed by inner SpecialEntry data.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        const VERSION_SIZE: usize = 2;

        let version_bytes = data
            .get(..VERSION_SIZE)
            .ok_or_else(|| bincode::Error::new(bincode::ErrorKind::SizeLimit))?;

        let version = u16::from_le_bytes(
            version_bytes
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

        impl Visitor<'_> for VersionedSpecialEntryVisitor {
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
    /// Serializes by delegating to the inner SpecialEntryV0 regardless of variant.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let entry = match self {
            Self::V0(entry) | Self::Current(entry) => entry,
        };
        entry.to_bytes()
    }

    /// Deserializes by parsing SpecialEntryV0 data and always creating Current variant for forward compatibility.
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

        impl Visitor<'_> for SpecialEntryVisitor {
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
    /// Serializes the special entry by delegating to the inner type's serialization.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let Self::ParentReadyUpdate(update) = self;
        update.to_bytes()
    }

    /// Deserializes by parsing VersionedParentReadyUpdate data and creating ParentReadyUpdate variant.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        let update = VersionedParentReadyUpdate::from_bytes(data)?;
        Ok(Self::ParentReadyUpdate(update))
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

        impl Visitor<'_> for SpecialEntryV0Visitor {
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
    /// Creates a new versioned parent ready update with the provided version and inner data.
    pub const fn new(version: u8, inner: ParentReadyUpdate) -> Self {
        Self { version, inner }
    }

    /// Serializes to bytes by writing version as single byte followed by inner data.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut buffer = Vec::new();
        buffer.push(self.version);
        buffer.extend_from_slice(&self.inner.to_bytes()?);
        Ok(buffer)
    }

    /// Deserializes from bytes by reading single byte version followed by inner ParentReadyUpdate data.
    fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        let (&version, remaining) = data
            .split_first()
            .ok_or_else(|| bincode::Error::new(bincode::ErrorKind::SizeLimit))?;

        let inner = ParentReadyUpdate::from_bytes(remaining)?;
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

        impl Visitor<'_> for VersionedParentReadyUpdateVisitor {
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
    /// Serializes using bincode for the inner ParentReadyUpdateV0 data, regardless of variant.
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let update = match self {
            Self::V0(update) | Self::Current(update) => update,
        };
        bincode::serialize(update)
    }

    /// Deserializes using bincode and always creates Current variant for forward compatibility.
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

        impl Visitor<'_> for ParentReadyUpdateVisitor {
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

#[cfg(test)]
mod tests {
    use {super::*, solana_hash::Hash, std::iter::repeat};

    // Helper function to create a mock Entry
    fn create_mock_entry() -> Entry {
        Entry::default()
    }

    // Helper function to create multiple mock entries
    fn create_mock_entries(count: usize) -> Vec<Entry> {
        repeat_n(create_mock_entry(), count).collect()
    }

    // Helper function to create a ParentReadyUpdateV0
    fn create_parent_ready_update() -> ParentReadyUpdateV0 {
        ParentReadyUpdateV0 {
            version: 1,
            new_parent_slot: 42,
            new_parent_block_id: Hash::default(),
        }
    }

    // Helper function to create different ParentReadyUpdateV0 instances
    fn create_parent_ready_update_with_data(
        version: u8,
        slot: u64,
        hash: Hash,
    ) -> ParentReadyUpdateV0 {
        ParentReadyUpdateV0 {
            version,
            new_parent_slot: slot,
            new_parent_block_id: hash,
        }
    }

    // EntryBatch constructor tests
    #[test]
    fn test_entry_batch_new_valid() {
        let entries = create_mock_entries(3);
        let batch = EntryBatch::new(entries).unwrap();
        assert_eq!(batch.entries().len(), 3);
        assert!(batch.special().is_none());
    }

    #[test]
    fn test_entry_batch_new_empty_entries() {
        let entries = Vec::new();
        let result = EntryBatch::new(entries);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn test_entry_batch_new_exceeds_max_entries() {
        // Test that creating EntryBatch with too many entries fails
        // We can't actually create u32::MAX entries in memory, so we test the validation directly
        // by creating a batch with entries and then manually testing the length validation

        // First test that MAX_ENTRIES itself fails
        let result = EntryBatch::validate_entries_length(EntryBatch::MAX_ENTRIES);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));

        // Test that MAX_ENTRIES + 1 also fails
        let result = EntryBatch::validate_entries_length(EntryBatch::MAX_ENTRIES + 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));

        // Test that MAX_ENTRIES - 1 succeeds
        let result = EntryBatch::validate_entries_length(EntryBatch::MAX_ENTRIES - 1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_entry_batch_deserialization_exceeds_max_entries() {
        // Test that deserializing EntryBatch with too many entries fails
        let mut data = Vec::new();

        // Write entries length as u32::MAX (which equals MAX_ENTRIES)
        data.extend_from_slice(&(EntryBatch::MAX_ENTRIES as u64).to_le_bytes());

        // Add some dummy data to prevent other errors
        data.extend_from_slice(&[1, 2, 3, 4]);

        let result = EntryBatch::from_bytes(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));

        // Test with even larger number
        let mut data = Vec::new();
        data.extend_from_slice(&((EntryBatch::MAX_ENTRIES + 1000) as u64).to_le_bytes());
        data.extend_from_slice(&[1, 2, 3, 4]);

        let result = EntryBatch::from_bytes(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));

        // Test with u64::MAX
        let mut data = Vec::new();
        data.extend_from_slice(&u64::MAX.to_le_bytes());
        data.extend_from_slice(&[1, 2, 3, 4]);

        let result = EntryBatch::from_bytes(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));

        // Test that MAX_ENTRIES - 1 would succeed (if we had valid entry data)
        let mut data = Vec::new();
        data.extend_from_slice(&((EntryBatch::MAX_ENTRIES - 1) as u64).to_le_bytes());
        // Note: This will still fail because we don't have valid entry data,
        // but it should fail for a different reason (not the length check)

        let result = EntryBatch::from_bytes(&data);
        assert!(result.is_err());
        // Should NOT contain "exceeds maximum" since the length is valid
        assert!(!result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_entry_batch_new_max_entries() {
        // Test near the boundary - creating u32::MAX entries would be impractical
        // So we'll test the validation logic directly
        let result = EntryBatch::validate_entries_length(EntryBatch::MAX_ENTRIES);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }

    #[test]
    fn test_entry_batch_new_special() {
        let versioned_update = VersionedParentReadyUpdate::new(
            1,
            ParentReadyUpdate::Current(create_parent_ready_update()),
        );
        let special = VersionedSpecialEntry::new(
            1,
            SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(versioned_update)),
        );
        let batch = EntryBatch::new_special(special);
        assert_eq!(batch.entries().len(), 0);
        assert!(batch.special().is_some());
    }

    // EntryBatch serialization tests
    #[test]
    fn test_entry_batch_valid_entries_only() {
        let entries = create_mock_entries(3);
        let batch = EntryBatch::new(entries).unwrap();

        // Test serialization
        let bytes = batch.to_bytes().unwrap();
        assert!(!bytes.is_empty());

        // First 8 bytes should be entries length (3 as u64)
        let entries_len = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        assert_eq!(entries_len, 3);

        // Test deserialization
        let deserialized = EntryBatch::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.entries().len(), 3);
        assert!(deserialized.special().is_none());

        // Test serde serialization
        let serialized = bincode::serialize(&batch).unwrap();
        let serde_deserialized: EntryBatch = bincode::deserialize(&serialized).unwrap();
        assert_eq!(serde_deserialized.entries().len(), 3);
        assert!(serde_deserialized.special().is_none());
    }

    #[test]
    fn test_entry_batch_valid_special_only() {
        let versioned_update = VersionedParentReadyUpdate::new(
            1,
            ParentReadyUpdate::Current(create_parent_ready_update()),
        );
        let special = VersionedSpecialEntry::new(
            1,
            SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(versioned_update)),
        );
        let batch = EntryBatch::new_special(special);

        // Test serialization
        let bytes = batch.to_bytes().unwrap();
        assert!(!bytes.is_empty());

        // First 8 bytes should be entries length (0 as u64)
        let entries_len = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        assert_eq!(entries_len, 0);

        // Test deserialization
        let deserialized = EntryBatch::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.entries().len(), 0);
        assert!(deserialized.special().is_some());

        // Test serde serialization
        let serialized = bincode::serialize(&batch).unwrap();
        let serde_deserialized: EntryBatch = bincode::deserialize(&serialized).unwrap();
        assert_eq!(serde_deserialized.entries().len(), 0);
        assert!(serde_deserialized.special().is_some());
    }

    #[test]
    fn test_entry_batch_from_bytes_insufficient_data() {
        let short_data = vec![1, 2, 3]; // Less than 8 bytes
        let result = EntryBatch::from_bytes(&short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_entry_batch_large_entries_count() {
        let entries = create_mock_entries(1000);
        let batch = EntryBatch::EntryVec(entries);

        let bytes = batch.to_bytes().unwrap();
        let deserialized = EntryBatch::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.entries().len(), 1000);
    }

    #[test]
    fn test_entry_batch_empty_entries_with_special() {
        let special = VersionedSpecialEntry {
            version: 255,
            inner: SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(
                VersionedParentReadyUpdate {
                    version: 10,
                    inner: ParentReadyUpdate::Current(create_parent_ready_update()),
                },
            )),
        };
        let batch = EntryBatch::Special(special);

        let bytes = batch.to_bytes().unwrap();
        let deserialized = EntryBatch::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.entries().len(), 0);
        assert!(deserialized.special().is_some());

        let special = deserialized.special().unwrap();
        assert_eq!(special.version, 255);
    }

    // ParentReadyUpdateV0 Tests
    #[test]
    fn test_parent_ready_update_v0_serialization() {
        let update = create_parent_ready_update();

        let serialized = bincode::serialize(&update).unwrap();
        let deserialized: ParentReadyUpdateV0 = bincode::deserialize(&serialized).unwrap();

        assert_eq!(update, deserialized);
    }

    #[test]
    fn test_parent_ready_update_v0_clone_and_debug() {
        let update = create_parent_ready_update();
        let cloned_update = update.clone();

        assert_eq!(update, cloned_update);

        let debug_str = format!("{update:?}");
        assert!(debug_str.contains("ParentReadyUpdateV0"));
    }

    #[test]
    fn test_parent_ready_update_v0_with_different_values() {
        let hash = Hash::new_unique();
        let update = create_parent_ready_update_with_data(255, u64::MAX, hash);

        assert_eq!(update.version, 255);
        assert_eq!(update.new_parent_slot, u64::MAX);
        assert_eq!(update.new_parent_block_id, hash);

        let serialized = bincode::serialize(&update).unwrap();
        let deserialized: ParentReadyUpdateV0 = bincode::deserialize(&serialized).unwrap();
        assert_eq!(update, deserialized);
    }

    #[test]
    fn test_parent_ready_update_v0_equality() {
        let update1 = create_parent_ready_update();
        let update2 = create_parent_ready_update();
        let update3 = create_parent_ready_update_with_data(2, 43, Hash::new_unique());

        assert_eq!(update1, update2);
        assert_ne!(update1, update3);
    }

    // ParentReadyUpdate Tests
    #[test]
    fn test_parent_ready_update_serialization() {
        let original_data = create_parent_ready_update();
        let update = ParentReadyUpdate::Current(original_data.clone());

        let bytes = update.to_bytes().unwrap();
        let deserialized = ParentReadyUpdate::from_bytes(&bytes).unwrap();

        let ParentReadyUpdate::Current(deser_data) = deserialized else {
            panic!("Expected Current variant");
        };
        assert_eq!(original_data, deser_data);

        let serialized = bincode::serialize(&update).unwrap();
        let serde_deserialized: ParentReadyUpdate = bincode::deserialize(&serialized).unwrap();

        let ParentReadyUpdate::Current(serde_deser_data) = serde_deserialized else {
            panic!("Expected Current variant from serde");
        };
        assert_eq!(original_data, serde_deser_data);
    }

    #[test]
    fn test_parent_ready_update_v0_variant() {
        let original_data = create_parent_ready_update();
        let update = ParentReadyUpdate::V0(original_data.clone());

        let bytes = update.to_bytes().unwrap();
        let deserialized = ParentReadyUpdate::from_bytes(&bytes).unwrap();

        let ParentReadyUpdate::Current(deser_data) = deserialized else {
            panic!("Expected V0 -> Current conversion");
        };
        assert_eq!(original_data, deser_data);
    }

    #[test]
    fn test_parent_ready_update_empty_data() {
        let result = ParentReadyUpdate::from_bytes(&[]);
        assert!(result.is_err());
    }

    // VersionedParentReadyUpdate Tests
    #[test]
    fn test_versioned_parent_ready_update_serialization() {
        let original_data = create_parent_ready_update();
        let versioned_update = VersionedParentReadyUpdate {
            version: 42,
            inner: ParentReadyUpdate::Current(original_data.clone()),
        };

        let bytes = versioned_update.to_bytes().unwrap();
        let deserialized = VersionedParentReadyUpdate::from_bytes(&bytes).unwrap();

        assert_eq!(versioned_update.version, deserialized.version);
        let ParentReadyUpdate::Current(deser_data) = deserialized.inner else {
            panic!("Expected Current variant");
        };
        assert_eq!(original_data, deser_data);

        let serialized = bincode::serialize(&versioned_update).unwrap();
        let serde_deserialized: VersionedParentReadyUpdate =
            bincode::deserialize(&serialized).unwrap();
        assert_eq!(versioned_update.version, serde_deserialized.version);
    }

    #[test]
    fn test_versioned_parent_ready_update_empty_data() {
        let result = VersionedParentReadyUpdate::from_bytes(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_versioned_parent_ready_update_max_version() {
        let versioned_update = VersionedParentReadyUpdate {
            version: u8::MAX,
            inner: ParentReadyUpdate::Current(create_parent_ready_update_with_data(
                255,
                u64::MAX,
                Hash::new_unique(),
            )),
        };

        let bytes = versioned_update.to_bytes().unwrap();
        let deserialized = VersionedParentReadyUpdate::from_bytes(&bytes).unwrap();
        assert_eq!(versioned_update.version, deserialized.version);
    }

    // SpecialEntryV0 Tests
    #[test]
    fn test_special_entry_v0_parent_ready_update_serialization() {
        let versioned_update = VersionedParentReadyUpdate {
            version: 1,
            inner: ParentReadyUpdate::Current(create_parent_ready_update()),
        };
        let entry = SpecialEntryV0::ParentReadyUpdate(versioned_update);

        let bytes = entry.to_bytes().unwrap();
        assert!(!bytes.is_empty());

        let SpecialEntryV0::ParentReadyUpdate(update) = SpecialEntryV0::from_bytes(&bytes).unwrap();
        assert_eq!(update.version, 1);

        let serialized = bincode::serialize(&entry).unwrap();
        let SpecialEntryV0::ParentReadyUpdate(_) = bincode::deserialize(&serialized).unwrap();
    }

    // SpecialEntry Tests
    #[test]
    fn test_special_entry_current_variant_serialization() {
        let versioned_update = VersionedParentReadyUpdate {
            version: 2,
            inner: ParentReadyUpdate::Current(create_parent_ready_update()),
        };
        let entry = SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(versioned_update));

        let bytes = entry.to_bytes().unwrap();
        let deserialized = SpecialEntry::from_bytes(&bytes).unwrap();

        let SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(update)) = deserialized else {
            panic!("Expected Current(ParentReadyUpdate) variant");
        };
        assert_eq!(update.version, 2);

        let serialized = bincode::serialize(&entry).unwrap();
        let SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(_)) =
            bincode::deserialize(&serialized).unwrap()
        else {
            panic!("Expected Current(ParentReadyUpdate) variant from serde");
        };
    }

    // VersionedSpecialEntry Tests
    #[test]
    fn test_versioned_special_entry_serialization() {
        let versioned_entry = VersionedSpecialEntry {
            version: 1000,
            inner: SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(
                VersionedParentReadyUpdate {
                    version: 1,
                    inner: ParentReadyUpdate::Current(create_parent_ready_update()),
                },
            )),
        };

        let bytes = versioned_entry.to_bytes().unwrap();
        assert!(bytes.len() >= 2);

        let deserialized = VersionedSpecialEntry::from_bytes(&bytes).unwrap();
        assert_eq!(versioned_entry.version, deserialized.version);

        let serialized = bincode::serialize(&versioned_entry).unwrap();
        let serde_deserialized: VersionedSpecialEntry = bincode::deserialize(&serialized).unwrap();
        assert_eq!(versioned_entry.version, serde_deserialized.version);
    }

    #[test]
    fn test_versioned_special_entry_with_parent_ready_update() {
        let versioned_update = VersionedParentReadyUpdate {
            version: 5,
            inner: ParentReadyUpdate::Current(create_parent_ready_update_with_data(
                100,
                12345,
                Hash::new_unique(),
            )),
        };
        let versioned_entry = VersionedSpecialEntry {
            version: u16::MAX,
            inner: SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(versioned_update)),
        };

        let bytes = versioned_entry.to_bytes().unwrap();
        let deserialized = VersionedSpecialEntry::from_bytes(&bytes).unwrap();

        assert_eq!(versioned_entry.version, deserialized.version);

        let SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(update)) = deserialized.inner
        else {
            panic!("Expected ParentReadyUpdate variant");
        };
        assert_eq!(update.version, 5);

        let ParentReadyUpdate::Current(data) = update.inner else {
            panic!("Expected Current variant");
        };
        assert_eq!(data.version, 100);
        assert_eq!(data.new_parent_slot, 12345);
    }

    #[test]
    fn test_versioned_special_entry_insufficient_data() {
        let short_data = vec![1]; // Less than 2 bytes
        let result = VersionedSpecialEntry::from_bytes(&short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_versioned_special_entry_zero_version() {
        let versioned_entry = VersionedSpecialEntry {
            version: 0,
            inner: SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(
                VersionedParentReadyUpdate {
                    version: 1,
                    inner: ParentReadyUpdate::Current(create_parent_ready_update()),
                },
            )),
        };

        let bytes = versioned_entry.to_bytes().unwrap();
        let deserialized = VersionedSpecialEntry::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.version, 0);
    }

    // End-to-end Tests
    #[test]
    fn test_full_entry_batch_with_complex_special_data() {
        let complex_hash = Hash::new_unique();
        let parent_update = create_parent_ready_update_with_data(255, u64::MAX, complex_hash);
        let versioned_parent_update = VersionedParentReadyUpdate {
            version: u8::MAX,
            inner: ParentReadyUpdate::Current(parent_update),
        };
        let special_entry =
            SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(versioned_parent_update));
        let versioned_special = VersionedSpecialEntry {
            version: u16::MAX,
            inner: special_entry,
        };

        let batch = EntryBatch::Special(versioned_special);

        let bytes = batch.to_bytes().unwrap();
        let deserialized = EntryBatch::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.entries().len(), 0);
        assert!(deserialized.special().is_some());

        let special = deserialized.special().unwrap();
        assert_eq!(special.version, u16::MAX);

        let SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(update)) = &special.inner
        else {
            panic!("Expected ParentReadyUpdate variant");
        };
        assert_eq!(update.version, u8::MAX);

        let ParentReadyUpdate::Current(data) = &update.inner else {
            panic!("Expected Current variant");
        };
        assert_eq!(data.version, 255);
        assert_eq!(data.new_parent_slot, u64::MAX);
        assert_eq!(data.new_parent_block_id, complex_hash);

        let serde_bytes = bincode::serialize(&batch).unwrap();
        let serde_deserialized: EntryBatch = bincode::deserialize(&serde_bytes).unwrap();
        assert_eq!(serde_deserialized.entries().len(), 0);
        assert!(serde_deserialized.special().is_some());
    }

    #[test]
    fn test_entry_batch_with_mixed_entry_sizes() {
        let entries = create_mock_entries(10);
        let batch = EntryBatch::EntryVec(entries);

        let bytes = batch.to_bytes().unwrap();
        let deserialized = EntryBatch::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.entries().len(), 10);
        assert!(deserialized.special().is_none());
    }

    #[test]
    fn test_all_variant_combinations() {
        let v0_parent = ParentReadyUpdate::V0(create_parent_ready_update());
        let v0_versioned = VersionedParentReadyUpdate {
            version: 0,
            inner: v0_parent,
        };
        let v0_special = SpecialEntry::V0(SpecialEntryV0::ParentReadyUpdate(v0_versioned));
        let v0_versioned_special = VersionedSpecialEntry {
            version: 0,
            inner: v0_special,
        };

        let bytes = v0_versioned_special.to_bytes().unwrap();
        let deserialized = VersionedSpecialEntry::from_bytes(&bytes).unwrap();

        let SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(update)) = deserialized.inner
        else {
            panic!("Expected Current SpecialEntry");
        };

        let ParentReadyUpdate::Current(_) = update.inner else {
            panic!("Expected inner ParentReadyUpdate to be Current");
        };
    }

    #[test]
    fn test_boundary_values() {
        let boundary_update = create_parent_ready_update_with_data(0, 0, Hash::default());
        let boundary_versioned = VersionedParentReadyUpdate {
            version: 0,
            inner: ParentReadyUpdate::Current(boundary_update.clone()),
        };

        let bytes = boundary_versioned.to_bytes().unwrap();
        let deserialized = VersionedParentReadyUpdate::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.version, 0);
        let ParentReadyUpdate::Current(data) = deserialized.inner else {
            panic!("Expected Current variant");
        };
        assert_eq!(data, boundary_update);
    }

    #[test]
    fn test_serialization_deterministic() {
        let update = create_parent_ready_update();
        let batch = EntryBatch::Special(VersionedSpecialEntry {
            version: 1,
            inner: SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(
                VersionedParentReadyUpdate {
                    version: 1,
                    inner: ParentReadyUpdate::Current(update),
                },
            )),
        });

        let bytes1 = batch.to_bytes().unwrap();
        let bytes2 = batch.to_bytes().unwrap();
        let bytes3 = batch.to_bytes().unwrap();

        assert_eq!(bytes1, bytes2);
        assert_eq!(bytes2, bytes3);
    }

    #[test]
    fn test_large_version_numbers() {
        let large_versioned = VersionedSpecialEntry {
            version: u16::MAX,
            inner: SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(
                VersionedParentReadyUpdate {
                    version: u8::MAX,
                    inner: ParentReadyUpdate::Current(create_parent_ready_update_with_data(
                        u8::MAX,
                        u64::MAX,
                        Hash::new_unique(),
                    )),
                },
            )),
        };

        let bytes = large_versioned.to_bytes().unwrap();
        let deserialized = VersionedSpecialEntry::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.version, u16::MAX);

        let SpecialEntry::Current(SpecialEntryV0::ParentReadyUpdate(update)) = deserialized.inner
        else {
            panic!("Expected ParentReadyUpdate variant");
        };
        assert_eq!(update.version, u8::MAX);

        let ParentReadyUpdate::Current(data) = update.inner else {
            panic!("Expected Current variant");
        };
        assert_eq!(data.version, u8::MAX);
        assert_eq!(data.new_parent_slot, u64::MAX);
    }

    #[test]
    fn test_error_conditions_comprehensive() {
        assert!(ParentReadyUpdate::from_bytes(&[]).is_err());
        assert!(VersionedParentReadyUpdate::from_bytes(&[]).is_err());
        assert!(VersionedSpecialEntry::from_bytes(&[1]).is_err());
        assert!(EntryBatch::from_bytes(&[1, 2, 3]).is_err());
    }

    #[test]
    fn test_round_trip_consistency() {
        let original_update = create_parent_ready_update_with_data(42, 12345, Hash::new_unique());

        let bytes1 = bincode::serialize(&original_update).unwrap();
        let deser1: ParentReadyUpdateV0 = bincode::deserialize(&bytes1).unwrap();

        let bytes2 = bincode::serialize(&deser1).unwrap();
        let deser2: ParentReadyUpdateV0 = bincode::deserialize(&bytes2).unwrap();

        let bytes3 = bincode::serialize(&deser2).unwrap();
        let deser3: ParentReadyUpdateV0 = bincode::deserialize(&bytes3).unwrap();

        assert_eq!(original_update, deser1);
        assert_eq!(deser1, deser2);
        assert_eq!(deser2, deser3);
        assert_eq!(bytes1, bytes2);
        assert_eq!(bytes2, bytes3);
    }
}
