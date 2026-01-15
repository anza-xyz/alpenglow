//! Defines aggregates used for vote rewards.

use {
    solana_bls_signatures::SignatureCompressed as BLSSignatureCompressed,
    solana_clock::Slot,
    solana_hash::Hash,
    thiserror::Error,
    wincode::{
        containers::{Pod, Vec as WincodeVec},
        error::write_length_encoding_overflow,
        io::{Reader, Writer},
        len::SeqLen,
        ReadResult, SchemaRead, SchemaWrite, WriteResult,
    },
};

/// Number of slots in the past that the the current leader is responsible for producing the reward certificates.
pub const NUM_SLOTS_FOR_REWARD: u64 = 8;

/// Different types of errors that can be returned when constructing a new reward certificate.
#[derive(Debug, Error)]
pub enum RewardCertError {
    /// Invalid bitmap was supplied.
    #[error("invalid bitmap was supplied")]
    InvalidBitmap,
}

/// 2-byte length prefix (max 65535 elements).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct U16Len;

impl SeqLen for U16Len {
    fn read<'de, T>(reader: &mut impl Reader<'de>) -> ReadResult<usize> {
        u16::get(reader).map(|len| len as usize)
    }

    fn write(writer: &mut impl Writer, len: usize) -> WriteResult<()> {
        let Ok(len): Result<u16, _> = len.try_into() else {
            return Err(write_length_encoding_overflow("u16::MAX"));
        };
        writer.write(&len.to_le_bytes())?;
        Ok(())
    }

    fn write_bytes_needed(_len: usize) -> WriteResult<usize> {
        Ok(2)
    }
}

/// Reward certificate for the validators that voted skip.
///
/// Unlike the skip certificate which can be base-2 or base-3 encoded, this is guaranteed to be base-2 encoded.
#[derive(Clone, PartialEq, Eq, Debug, SchemaWrite, SchemaRead)]
pub struct SkipRewardCertificate {
    /// The slot the certificate is for.
    pub slot: Slot,
    /// The signature.
    #[wincode(with = "Pod<BLSSignatureCompressed>")]
    pub signature: BLSSignatureCompressed,
    /// The bitmap for validators, see solana-signer-store for encoding format.
    #[wincode(with = "WincodeVec<u8, U16Len>")]
    bitmap: Vec<u8>,
}

impl SkipRewardCertificate {
    /// Returns a new instance of [`SkipRewardCertificate`].
    pub fn try_new(
        slot: Slot,
        signature: BLSSignatureCompressed,
        bitmap: Vec<u8>,
    ) -> Result<Self, RewardCertError> {
        if bitmap.len() > u16::MAX as usize {
            return Err(RewardCertError::InvalidBitmap);
        }
        Ok(Self {
            slot,
            signature,
            bitmap,
        })
    }

    /// Returns a reference to the bitmap.
    pub fn bitmap(&self) -> &[u8] {
        &self.bitmap
    }
}

/// Reward certificate for the validators that voted notar.
#[derive(Clone, PartialEq, Eq, Debug, SchemaRead, SchemaWrite)]
pub struct NotarRewardCertificate {
    /// The slot the certificate is for.
    pub slot: Slot,
    /// The block id the certificate is for.
    #[wincode(with = "Pod<Hash>")]
    pub block_id: Hash,
    /// The signature.
    #[wincode(with = "Pod<BLSSignatureCompressed>")]
    pub signature: BLSSignatureCompressed,
    /// The bitmap for validators, see solana-signer-store for encoding format.
    #[wincode(with = "WincodeVec<u8, U16Len>")]
    bitmap: Vec<u8>,
}

impl NotarRewardCertificate {
    /// Returns a new instance of [`NotarRewardCertificate`].
    pub fn try_new(
        slot: Slot,
        block_id: Hash,
        signature: BLSSignatureCompressed,
        bitmap: Vec<u8>,
    ) -> Result<Self, RewardCertError> {
        if bitmap.len() > u16::MAX as usize {
            return Err(RewardCertError::InvalidBitmap);
        }
        Ok(Self {
            slot,
            block_id,
            signature,
            bitmap,
        })
    }

    /// Returns a reference to the bitmap.
    pub fn bitmap(&self) -> &[u8] {
        &self.bitmap
    }
}
