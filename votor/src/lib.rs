#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
use {
    solana_sdk::{clock::Slot, hash::Hash},
    solana_vote::alpenglow::{
        certificate::{Certificate, CertificateType},
        vote::Vote,
    },
    std::time::Duration,
};

pub mod certificate_pool;
mod certificate_pool_service;
pub mod commitment;
pub mod event;
mod event_handler;
pub mod root_utils;
mod skip_timer;
pub mod vote_history;
pub mod vote_history_storage;
pub mod voting_utils;
pub mod votor;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;

// Core consensus types and constants
pub type Stake = u64;
pub type Block = (Slot, Hash, Hash);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CertificateId {
    Finalize(Slot),
    FinalizeFast(Slot, Hash, Hash),
    Notarize(Slot, Hash, Hash),
    NotarizeFallback(Slot, Hash, Hash),
    Skip(Slot),
}

impl CertificateId {
    #[allow(dead_code)]
    pub fn slot(&self) -> Slot {
        match self {
            CertificateId::Finalize(slot)
            | CertificateId::FinalizeFast(slot, _, _)
            | CertificateId::Notarize(slot, _, _)
            | CertificateId::NotarizeFallback(slot, _, _)
            | CertificateId::Skip(slot) => *slot,
        }
    }

    pub fn is_fast_finalization(&self) -> bool {
        matches!(self, Self::FinalizeFast(_, _, _))
    }

    #[allow(dead_code)]
    pub fn is_finalization_variant(&self) -> bool {
        matches!(self, Self::Finalize(_) | Self::FinalizeFast(_, _, _))
    }

    #[allow(dead_code)]
    pub fn is_notarize_fallback(&self) -> bool {
        matches!(self, Self::NotarizeFallback(_, _, _))
    }

    #[allow(dead_code)]
    pub fn is_skip(&self) -> bool {
        matches!(self, Self::Skip(_))
    }

    pub fn to_block(self) -> Option<Block> {
        match self {
            CertificateId::Finalize(_) | CertificateId::Skip(_) => None,
            CertificateId::Notarize(slot, block_id, bank_hash)
            | CertificateId::NotarizeFallback(slot, block_id, bank_hash)
            | CertificateId::FinalizeFast(slot, block_id, bank_hash) => {
                Some((slot, block_id, bank_hash))
            }
        }
    }

    /// "Critical" certs are the certificates necessary to make progress
    /// We do not consider the next slot for voting until we've seen either
    /// a Skip certificate or a NotarizeFallback certificate for ParentReady
    ///
    /// Note: Notarization certificates necessarily generate a
    /// NotarizeFallback certificate as well
    pub fn is_critical(&self) -> bool {
        matches!(self, Self::NotarizeFallback(_, _, _) | Self::Skip(_))
    }
}

impl From<&Certificate> for CertificateId {
    fn from(certificate: &Certificate) -> Self {
        match certificate.certificate_type {
            CertificateType::Finalize => CertificateId::Finalize(certificate.slot),
            CertificateType::FinalizeFast => CertificateId::FinalizeFast(
                certificate.slot,
                certificate
                    .block_id
                    .expect("FinalizeFast must have block_id"),
                certificate
                    .replayed_bank_hash
                    .expect("FinalizeFast must have bank_hash"),
            ),
            CertificateType::Notarize => CertificateId::Notarize(
                certificate.slot,
                certificate.block_id.expect("Notarize must have block_id"),
                certificate
                    .replayed_bank_hash
                    .expect("Notarize must have bank_hash"),
            ),
            CertificateType::NotarizeFallback => CertificateId::NotarizeFallback(
                certificate.slot,
                certificate
                    .block_id
                    .expect("NotarizeFallback must have block_id"),
                certificate
                    .replayed_bank_hash
                    .expect("NotarizeFallback must have bank_hash"),
            ),
            CertificateType::Skip => CertificateId::Skip(certificate.slot),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VoteType {
    Finalize,
    Notarize,
    NotarizeFallback,
    Skip,
    SkipFallback,
}

impl VoteType {
    #[allow(dead_code)]
    pub fn is_notarize_type(&self) -> bool {
        matches!(self, Self::Notarize | Self::NotarizeFallback)
    }
}

pub const fn conflicting_types(vote_type: VoteType) -> &'static [VoteType] {
    match vote_type {
        VoteType::Finalize => &[VoteType::NotarizeFallback, VoteType::Skip],
        VoteType::Notarize => &[VoteType::Skip, VoteType::NotarizeFallback],
        VoteType::NotarizeFallback => &[VoteType::Finalize, VoteType::Notarize],
        VoteType::Skip => &[
            VoteType::Finalize,
            VoteType::Notarize,
            VoteType::SkipFallback,
        ],
        VoteType::SkipFallback => &[VoteType::Skip],
    }
}

/// Lookup from `CertificateId` to the `VoteType`s that contribute,
/// as well as the stake fraction required for certificate completion.
///
/// Must be in sync with `vote_to_certificate_ids`
pub const fn certificate_limits_and_vote_types(
    cert_type: CertificateId,
) -> (f64, &'static [VoteType]) {
    match cert_type {
        CertificateId::Notarize(_, _, _) => (0.6, &[VoteType::Notarize]),
        CertificateId::NotarizeFallback(_, _, _) => {
            (0.6, &[VoteType::Notarize, VoteType::NotarizeFallback])
        }
        CertificateId::FinalizeFast(_, _, _) => (0.8, &[VoteType::Notarize]),
        CertificateId::Finalize(_) => (0.6, &[VoteType::Finalize]),
        CertificateId::Skip(_) => (0.6, &[VoteType::Skip, VoteType::SkipFallback]),
    }
}

/// Lookup from `Vote` to the `CertificateId`s the vote accounts for
///
/// Must be in sync with `certificate_limits_and_vote_types` and `VoteType::get_type`
pub fn vote_to_certificate_ids(vote: &Vote) -> Vec<CertificateId> {
    match vote {
        Vote::Notarize(vote) => vec![
            CertificateId::Notarize(vote.slot(), *vote.block_id(), *vote.replayed_bank_hash()),
            CertificateId::NotarizeFallback(
                vote.slot(),
                *vote.block_id(),
                *vote.replayed_bank_hash(),
            ),
            CertificateId::FinalizeFast(vote.slot(), *vote.block_id(), *vote.replayed_bank_hash()),
        ],
        Vote::NotarizeFallback(vote) => vec![CertificateId::NotarizeFallback(
            vote.slot(),
            *vote.block_id(),
            *vote.replayed_bank_hash(),
        )],
        Vote::Finalize(vote) => vec![CertificateId::Finalize(vote.slot())],
        Vote::Skip(vote) => vec![CertificateId::Skip(vote.slot())],
        Vote::SkipFallback(vote) => vec![CertificateId::Skip(vote.slot())],
    }
}

pub const MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES: usize = 1;
pub const MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE: usize = 3;

// To avoid attacks, we only accept votes 512 slots newer than root.
pub const MAX_SLOT_AGE: u64 = 512;

pub const SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY: f64 = 0.4;
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP: f64 = 0.2;
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP: f64 = 0.6;

pub const SAFE_TO_SKIP_THRESHOLD: f64 = 0.4;

pub const STANDSTILL_TIMEOUT: Duration = Duration::from_secs(10);

/// Alpenglow block constants
/// The amount of time a leader has to build their block
pub const BLOCKTIME: Duration = Duration::from_millis(400);

/// The maximum message delay
pub const DELTA: Duration = Duration::from_millis(200);

/// The maximum delay a node can observe between entering the loop iteration
/// for a window and receiving any shred of the first block of the leader.
/// As a conservative global constant we set this to 3 * DELTA
pub const DELTA_TIMEOUT: Duration = DELTA.saturating_mul(3);

/// The timeout in ms for the leader block index within the leader window
#[inline]
pub fn skip_timeout(leader_block_index: usize) -> Duration {
    DELTA_TIMEOUT
        .saturating_add(
            BLOCKTIME
                .saturating_mul(leader_block_index as u32)
                .saturating_add(BLOCKTIME),
        )
        .saturating_add(DELTA)
}

/// Block timeout, when we should publish the final shred for the leader block index
/// within the leader window
#[inline]
pub fn block_timeout(leader_block_index: usize) -> Duration {
    // TODO: based on testing, perhaps adjust this
    BLOCKTIME.saturating_mul((leader_block_index as u32).saturating_add(1))
}
