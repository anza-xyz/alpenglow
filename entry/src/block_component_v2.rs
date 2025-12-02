/// Block components using wincode serialization.
///
/// A `BlockComponent` represents either an entry batch or a special block marker.
/// Most of the time, a block component contains a vector of entries. However, periodically,
/// there are special messages that a block needs to contain. To accommodate these special
/// messages, `BlockComponent` allows for the inclusion of special data via `VersionedBlockMarker`.
///
/// ## Serialization Layouts
///
/// All numeric fields use little-endian encoding.
///
/// ### BlockComponent with EntryBatch
/// ```text
/// ┌─────────────────────────────────────────┐
/// │ Entry Count                  (8 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ bincode Entry 0           (variable)    │
/// ├─────────────────────────────────────────┤
/// │ bincode Entry 1           (variable)    │
/// ├─────────────────────────────────────────┤
/// │ ...                                     │
/// ├─────────────────────────────────────────┤
/// │ bincode Entry N-1         (variable)    │
/// └─────────────────────────────────────────┘
/// ```
///
/// ### BlockComponent with BlockMarker
/// ```text
/// ┌─────────────────────────────────────────┐
/// │ Entry Count = 0              (8 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ Marker Version               (2 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ Marker Data               (variable)    │
/// └─────────────────────────────────────────┘
/// ```
///
/// ### BlockMarkerV1 Layout
/// ```text
/// ┌─────────────────────────────────────────┐
/// │ Variant ID                   (1 byte)   │
/// ├─────────────────────────────────────────┤
/// │ Byte Length                  (2 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ Variant Data              (variable)    │
/// └─────────────────────────────────────────┘
/// ```
///
/// ### BlockHeaderV1 Layout
/// ```text
/// ┌─────────────────────────────────────────┐
/// │ Parent Slot                  (8 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ Parent Block ID             (32 bytes)  │
/// └─────────────────────────────────────────┘
/// ```
///
/// ### UpdateParentV1 Layout
/// ```text
/// ┌─────────────────────────────────────────┐
/// │ Parent Slot                  (8 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ Parent Block ID             (32 bytes)  │
/// └─────────────────────────────────────────┘
/// ```
///
/// ### BlockFooterV1 Layout
/// ```text
/// ┌─────────────────────────────────────────┐
/// │ Bank Hash                   (32 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ Producer Time Nanos          (8 bytes)  │
/// ├─────────────────────────────────────────┤
/// │ User Agent Length            (1 byte)   │
/// ├─────────────────────────────────────────┤
/// │ User Agent Bytes          (0-255 bytes) │
/// └─────────────────────────────────────────┘
/// ```
///
/// ### GenesisCertificate Layout
/// ```text
/// ┌─────────────────────────────────────────┐
/// │ Genesis Slot                  (8 bytes) │
/// ├─────────────────────────────────────────┤
/// │ Genesis Block ID             (32 bytes) │
/// ├─────────────────────────────────────────┤
/// │ BLS Signature               (192 bytes) │
/// ├─────────────────────────────────────────┤
/// │ Bitmap length (max 512)       (8 bytes) │
/// ├─────────────────────────────────────────┤
/// │ Bitmap                (up to 512 bytes) │
/// └─────────────────────────────────────────┘
/// ```
use {
    crate::entry::Entry,
    solana_bls_signatures::Signature as BLSSignature,
    solana_clock::Slot,
    solana_hash::Hash,
    solana_votor_messages::consensus_message::{Certificate, CertificateType},
    std::{marker::PhantomData, mem::MaybeUninit},
    wincode::{
        containers::{Pod, Vec as WincodeVec},
        error::{preallocation_size_limit, write_length_encoding_overflow},
        io::{Reader, Writer},
        len::{BincodeLen, SeqLen},
        ReadResult, SchemaRead, SchemaWrite, WriteResult,
    },
};

/// 1-byte length prefix (max 255 elements).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct U8Len;

impl SeqLen for U8Len {
    fn read<'de, T>(reader: &mut impl Reader<'de>) -> ReadResult<usize> {
        u8::get(reader).map(|len| len as usize)
    }

    fn write(writer: &mut impl Writer, len: usize) -> WriteResult<()> {
        let Ok(len) = len.try_into() else {
            return Err(write_length_encoding_overflow("u8::MAX"));
        };
        Ok(writer.write(&[len])?)
    }

    fn write_bytes_needed(_len: usize) -> WriteResult<usize> {
        Ok(1)
    }
}

/// Default maximum preallocation size for U16Len (4 MiB).
/// This is a safety precaution against malicious input causing OOM.
const DEFAULT_U16_LEN_MAX_SIZE: usize = 4 << 20;

/// 2-byte length prefix (max 65535 elements).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct U16Len<const MAX_SIZE: usize = DEFAULT_U16_LEN_MAX_SIZE>;

impl<const MAX_SIZE: usize> SeqLen for U16Len<MAX_SIZE> {
    fn read<'de, T>(reader: &mut impl Reader<'de>) -> ReadResult<usize> {
        let len = u16::get(reader)? as usize;
        let needed = len
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| preallocation_size_limit(usize::MAX, MAX_SIZE))?;
        if needed > MAX_SIZE {
            return Err(preallocation_size_limit(needed, MAX_SIZE));
        }
        Ok(len)
    }

    fn write(writer: &mut impl Writer, len: usize) -> WriteResult<()> {
        let Ok(len): Result<u16, _> = len.try_into() else {
            return Err(write_length_encoding_overflow("u16::MAX"));
        };
        Ok(writer.write(&len.to_le_bytes())?)
    }

    fn write_bytes_needed(_len: usize) -> WriteResult<usize> {
        Ok(2)
    }
}

/// Wraps a value with a length prefix for TLV-style serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LengthPrefixed<T, L: SeqLen = U16Len> {
    inner: T,
    _marker: PhantomData<L>,
}

impl<T, L: SeqLen> LengthPrefixed<T, L> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: SchemaWrite<Src = T>, L: SeqLen> SchemaWrite for LengthPrefixed<T, L> {
    type Src = Self;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        let inner_size = T::size_of(&src.inner)?;
        let len_size = L::write_bytes_needed(inner_size)?;
        Ok(len_size + inner_size)
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        let inner_size = T::size_of(&src.inner)?;
        L::write(writer, inner_size)?;
        T::write(writer, &src.inner)
    }
}

impl<'de, T: SchemaRead<'de, Dst = T>, L: SeqLen> SchemaRead<'de> for LengthPrefixed<T, L> {
    type Dst = Self;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let _len = L::read::<Self>(reader)?;
        let inner_dst = unsafe { &mut *(&raw mut (*dst.as_mut_ptr()).inner).cast() };
        T::read(reader, inner_dst)?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlockComponentError {
    #[error("Insufficient data")]
    InsufficientData,
    #[error("Entry count {count} exceeds max {max}")]
    TooManyEntries { count: usize, max: usize },
    #[error("Entry batch cannot be empty")]
    EmptyEntryBatch,
    #[error("Unknown {variant_type} variant: {id}")]
    UnknownVariant { variant_type: String, id: u8 },
    #[error("Unsupported version: {version}")]
    UnsupportedVersion { version: u16 },
    #[error("Cursor out of bounds")]
    CursorOutOfBounds,
    #[error(transparent)]
    SerializationFailed(#[from] wincode::WriteError),
    #[error(transparent)]
    DeserializationFailed(#[from] wincode::ReadError),
    #[error("Bincode error: {0}")]
    BincodeError(#[from] bincode::Error),
}

#[derive(Clone, PartialEq, Eq, Debug, SchemaWrite, SchemaRead)]
pub struct BlockHeaderV1 {
    pub parent_slot: Slot,
    #[wincode(with = "Pod<Hash>")]
    pub parent_block_id: Hash,
}

#[derive(Clone, PartialEq, Eq, Debug, SchemaWrite, SchemaRead)]
pub struct UpdateParentV1 {
    pub new_parent_slot: Slot,
    #[wincode(with = "Pod<Hash>")]
    pub new_parent_block_id: Hash,
}

/// Block production metadata. User agent is capped at 255 bytes.
#[derive(Clone, PartialEq, Eq, Debug, SchemaWrite, SchemaRead)]
pub struct BlockFooterV1 {
    #[wincode(with = "Pod<Hash>")]
    pub bank_hash: Hash,
    pub block_producer_time_nanos: u64,
    #[wincode(with = "WincodeVec<u8, U8Len>")]
    pub block_user_agent: Vec<u8>,
}

/// Attests to genesis block finalization with a BLS aggregate signature.
#[derive(Clone, PartialEq, Eq, Debug, SchemaWrite, SchemaRead)]
pub struct GenesisCertificate {
    pub slot: Slot,
    #[wincode(with = "Pod<Hash>")]
    pub block_id: Hash,
    #[wincode(with = "Pod<BLSSignature>")]
    pub bls_signature: BLSSignature,
    #[wincode(with = "WincodeVec<u8, BincodeLen>")]
    pub bitmap: Vec<u8>,
}

impl GenesisCertificate {
    /// Max bitmap size in bytes (supports up to 4096 validators).
    pub const MAX_BITMAP_SIZE: usize = 512;
}

impl TryFrom<Certificate> for GenesisCertificate {
    type Error = String;

    fn try_from(cert: Certificate) -> Result<Self, Self::Error> {
        let CertificateType::Genesis(slot, block_id) = cert.cert_type else {
            return Err("expected genesis certificate".into());
        };
        if cert.bitmap.len() > Self::MAX_BITMAP_SIZE {
            return Err(format!(
                "bitmap size {} exceeds max {}",
                cert.bitmap.len(),
                Self::MAX_BITMAP_SIZE
            ));
        }
        Ok(Self {
            slot,
            block_id,
            bls_signature: cert.signature,
            bitmap: cert.bitmap,
        })
    }
}

impl From<GenesisCertificate> for Certificate {
    fn from(cert: GenesisCertificate) -> Self {
        Self {
            cert_type: CertificateType::Genesis(cert.slot, cert.block_id),
            signature: cert.bls_signature,
            bitmap: cert.bitmap,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaWrite, SchemaRead)]
#[wincode(tag_encoding = "u8")]
pub enum VersionedBlockFooter {
    #[wincode(tag = 1)]
    V1(BlockFooterV1),
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaWrite, SchemaRead)]
#[wincode(tag_encoding = "u8")]
pub enum VersionedBlockHeader {
    #[wincode(tag = 1)]
    V1(BlockHeaderV1),
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaWrite, SchemaRead)]
#[wincode(tag_encoding = "u8")]
pub enum VersionedUpdateParent {
    #[wincode(tag = 1)]
    V1(UpdateParentV1),
}

/// TLV-encoded marker variants.
#[derive(Debug, Clone, PartialEq, Eq, SchemaWrite, SchemaRead)]
#[wincode(tag_encoding = "u8")]
pub enum BlockMarkerV1 {
    BlockFooter(LengthPrefixed<VersionedBlockFooter, U16Len>),
    BlockHeader(LengthPrefixed<VersionedBlockHeader, U16Len>),
    UpdateParent(LengthPrefixed<VersionedUpdateParent, U16Len>),
    GenesisCertificate(LengthPrefixed<GenesisCertificate, U16Len>),
}

impl BlockMarkerV1 {
    pub fn new_block_footer(f: VersionedBlockFooter) -> Self {
        Self::BlockFooter(LengthPrefixed::new(f))
    }

    pub fn new_block_header(h: VersionedBlockHeader) -> Self {
        Self::BlockHeader(LengthPrefixed::new(h))
    }

    pub fn new_update_parent(u: VersionedUpdateParent) -> Self {
        Self::UpdateParent(LengthPrefixed::new(u))
    }

    pub fn new_genesis_certificate(c: GenesisCertificate) -> Self {
        Self::GenesisCertificate(LengthPrefixed::new(c))
    }

    pub fn as_block_footer(&self) -> Option<&VersionedBlockFooter> {
        match self {
            Self::BlockFooter(lp) => Some(lp.inner()),
            _ => None,
        }
    }

    pub fn as_block_header(&self) -> Option<&VersionedBlockHeader> {
        match self {
            Self::BlockHeader(lp) => Some(lp.inner()),
            _ => None,
        }
    }

    pub fn as_update_parent(&self) -> Option<&VersionedUpdateParent> {
        match self {
            Self::UpdateParent(lp) => Some(lp.inner()),
            _ => None,
        }
    }

    pub fn as_genesis_certificate(&self) -> Option<&GenesisCertificate> {
        match self {
            Self::GenesisCertificate(lp) => Some(lp.inner()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaWrite, SchemaRead)]
#[wincode(tag_encoding = "u16")]
pub enum VersionedBlockMarker {
    #[wincode(tag = 1)]
    V1(BlockMarkerV1),
}

impl VersionedBlockMarker {
    pub const fn new(marker: BlockMarkerV1) -> Self {
        Self::V1(marker)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum BlockComponent {
    EntryBatch(Vec<Entry>),
    BlockMarker(VersionedBlockMarker),
}

impl BlockComponent {
    const MAX_ENTRIES: usize = u32::MAX as usize;
    const ENTRY_COUNT_SIZE: usize = 8;

    pub fn new_entry_batch(entries: Vec<Entry>) -> Result<Self, BlockComponentError> {
        if entries.is_empty() {
            return Err(BlockComponentError::EmptyEntryBatch);
        }

        if entries.len() >= Self::MAX_ENTRIES {
            return Err(BlockComponentError::TooManyEntries {
                count: entries.len(),
                max: Self::MAX_ENTRIES,
            });
        }

        Ok(Self::EntryBatch(entries))
    }

    pub const fn new_block_marker(marker: VersionedBlockMarker) -> Self {
        Self::BlockMarker(marker)
    }

    pub const fn as_marker(&self) -> Option<&VersionedBlockMarker> {
        match self {
            Self::BlockMarker(m) => Some(m),
            _ => None,
        }
    }

    pub fn infer_is_entry_batch(data: &[u8]) -> Option<bool> {
        data.get(..8)?
            .try_into()
            .ok()
            .map(|b| u64::from_le_bytes(b) != 0)
    }
}

impl SchemaWrite for BlockComponent {
    type Src = Self;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        match src {
            Self::EntryBatch(entries) => {
                // TODO(ksn): replace with wincode:: upon upstreaming to Agave. This also removes
                // the map_err.
                let size = bincode::serialized_size(entries).map_err(|_| {
                    wincode::WriteError::Custom("Couldn't invoke bincode::serialized_size")
                })?;
                Ok(size as usize)
            }
            Self::BlockMarker(marker) => {
                let marker_size = wincode::serialized_size(marker)? as usize;
                Ok(Self::ENTRY_COUNT_SIZE + marker_size)
            }
        }
    }

    fn write(writer: &mut impl Writer, src: &Self::Src) -> WriteResult<()> {
        match src {
            Self::EntryBatch(entries) => {
                // TODO(ksn): replace with wincode:: upon upstreaming to Agave. This also removes
                // the map_err.
                let bytes = bincode::serialize(entries).map_err(|_| {
                    wincode::WriteError::Custom("Couldn't invoke bincode::serialize")
                })?;
                writer.write(&bytes)?;
                Ok(())
            }
            Self::BlockMarker(marker) => {
                writer.write(&0u64.to_le_bytes())?;
                <VersionedBlockMarker as SchemaWrite>::write(writer, marker)
            }
        }
    }
}

impl<'de> SchemaRead<'de> for BlockComponent {
    type Dst = Self;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        // Read the entry count (first 8 bytes) to determine variant
        let count_bytes = reader.fill_array::<8>()?;
        let entry_count = u64::from_le_bytes(*count_bytes);

        if entry_count == 0 {
            // This is a BlockMarker - consume the count bytes and read the marker
            reader.consume(8)?;
            dst.write(Self::BlockMarker(VersionedBlockMarker::get(reader)?));
        } else {
            // This is an EntryBatch - read in the rest of the data. We do not anticipate having
            // cases where we need to deserialize multiple BlockComponents from a single slice, and
            // do not know where the delimiters are ahead of time.
            // First, get all remaining bytes to deserialize
            let data = reader.fill_buf(usize::MAX)?;

            // TODO(ksn): replace with wincode:: upon upstreaming to Agave. This also removes the
            // map_err.
            let entries: Vec<Entry> = bincode::deserialize(data)
                .map_err(|_| wincode::ReadError::Custom("Couldn't deserialize entries."))?;

            if entries.len() >= Self::MAX_ENTRIES {
                return Err(wincode::ReadError::Custom("Too many entries"));
            }

            // TODO(ksn): replace with wincode:: upon upstreaming to Agave. This also removes the
            // map_err.
            let consumed = bincode::serialized_size(&entries)
                .map_err(|_| wincode::ReadError::Custom("Couldn't determine serialized size."))?
                as usize;
            reader.consume(consumed)?;

            dst.write(Self::EntryBatch(entries));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::block_component as v1, std::iter::repeat_n};

    fn mock_entries(n: usize) -> Vec<Entry> {
        repeat_n(Entry::default(), n).collect()
    }

    fn sample_footer() -> (v1::BlockFooterV1, BlockFooterV1) {
        let v1 = v1::BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1234567890,
            block_user_agent: b"test-agent".to_vec(),
        };
        let v2 = BlockFooterV1 {
            bank_hash: v1.bank_hash,
            block_producer_time_nanos: v1.block_producer_time_nanos,
            block_user_agent: v1.block_user_agent.clone(),
        };
        (v1, v2)
    }

    #[test]
    fn leaf_type_compatibility() {
        // BlockHeaderV1
        let v1_header = v1::BlockHeaderV1 {
            parent_slot: 42,
            parent_block_id: Hash::new_unique(),
        };
        let v2_header = BlockHeaderV1 {
            parent_slot: v1_header.parent_slot,
            parent_block_id: v1_header.parent_block_id,
        };
        assert_eq!(
            bincode::serialize(&v1_header).unwrap(),
            wincode::serialize(&v2_header).unwrap()
        );

        // BlockFooterV1
        let (v1_footer, v2_footer) = sample_footer();
        assert_eq!(
            v1_footer.to_bytes().unwrap(),
            wincode::serialize(&v2_footer).unwrap()
        );

        // GenesisCertificate
        let v1_cert = v1::GenesisCertificate {
            slot: 999,
            block_id: Hash::new_unique(),
            bls_signature: BLSSignature::default(),
            bitmap: vec![1, 2, 3],
        };
        let v2_cert = GenesisCertificate {
            slot: v1_cert.slot,
            block_id: v1_cert.block_id,
            bls_signature: v1_cert.bls_signature,
            bitmap: v1_cert.bitmap.clone(),
        };
        assert_eq!(
            v1_cert.to_bytes().unwrap(),
            wincode::serialize(&v2_cert).unwrap()
        );
    }

    #[test]
    fn marker_compatibility() {
        let (v1_footer, v2_footer) = sample_footer();

        // BlockMarkerV1
        let v1_marker =
            v1::BlockMarkerV1::BlockFooter(v1::VersionedBlockFooter::new(v1_footer.clone()));
        let v2_marker =
            BlockMarkerV1::new_block_footer(VersionedBlockFooter::V1(v2_footer.clone()));
        assert_eq!(
            v1_marker.to_bytes().unwrap(),
            wincode::serialize(&v2_marker).unwrap()
        );

        // VersionedBlockMarker
        let v1_versioned = v1::VersionedBlockMarker::new(v1::BlockMarkerV1::BlockFooter(
            v1::VersionedBlockFooter::new(v1_footer),
        ));
        let v2_versioned = VersionedBlockMarker::new(BlockMarkerV1::new_block_footer(
            VersionedBlockFooter::V1(v2_footer),
        ));
        assert_eq!(
            v1_versioned.to_bytes().unwrap(),
            wincode::serialize(&v2_versioned).unwrap()
        );
    }

    #[test]
    fn block_component_compatibility() {
        // EntryBatch
        let entries = mock_entries(3);
        let v1_batch = v1::BlockComponent::new_entry_batch(entries.clone()).unwrap();
        let v2_batch = BlockComponent::new_entry_batch(entries).unwrap();
        assert_eq!(
            v1_batch.to_bytes().unwrap(),
            wincode::serialize(&v2_batch).unwrap()
        );

        // BlockMarker
        let (v1_footer, v2_footer) = sample_footer();
        let v1_comp = v1::BlockComponent::new_block_marker(v1::VersionedBlockMarker::new(
            v1::BlockMarkerV1::BlockFooter(v1::VersionedBlockFooter::new(v1_footer)),
        ));
        let v2_comp = BlockComponent::new_block_marker(VersionedBlockMarker::new(
            BlockMarkerV1::new_block_footer(VersionedBlockFooter::V1(v2_footer)),
        ));
        assert_eq!(
            v1_comp.to_bytes().unwrap(),
            wincode::serialize(&v2_comp).unwrap()
        );
    }

    #[test]
    fn round_trips() {
        let header = BlockHeaderV1 {
            parent_slot: 12345,
            parent_block_id: Hash::new_unique(),
        };
        let bytes = wincode::serialize(&header).unwrap();
        assert_eq!(
            header,
            wincode::deserialize::<BlockHeaderV1>(&bytes).unwrap()
        );

        let (_, footer) = sample_footer();
        let bytes = wincode::serialize(&footer).unwrap();
        assert_eq!(
            footer,
            wincode::deserialize::<BlockFooterV1>(&bytes).unwrap()
        );

        let cert = GenesisCertificate {
            slot: 999,
            block_id: Hash::new_unique(),
            bls_signature: BLSSignature::default(),
            bitmap: vec![1, 2, 3],
        };
        let bytes = wincode::serialize(&cert).unwrap();
        assert_eq!(
            cert,
            wincode::deserialize::<GenesisCertificate>(&bytes).unwrap()
        );

        let marker = VersionedBlockMarker::new(BlockMarkerV1::new_block_footer(
            VersionedBlockFooter::V1(footer.clone()),
        ));
        let bytes = wincode::serialize(&marker).unwrap();
        assert_eq!(
            marker,
            wincode::deserialize::<VersionedBlockMarker>(&bytes).unwrap()
        );

        let comp = BlockComponent::new_entry_batch(mock_entries(5)).unwrap();
        let bytes = wincode::serialize(&comp).unwrap();
        let deser: BlockComponent = wincode::deserialize(&bytes).unwrap();
        assert_eq!(comp, deser);

        let comp = BlockComponent::new_block_marker(marker);
        let bytes = wincode::serialize(&comp).unwrap();
        let deser: BlockComponent = wincode::deserialize(&bytes).unwrap();
        assert_eq!(comp, deser);
    }
}
