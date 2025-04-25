pub mod bit_vector;
pub mod certificate_pool;
pub mod transaction;
pub mod utils;
pub mod vote_certificate;
pub mod vote_history;
pub mod vote_history_storage;
pub mod vote_pool;
pub mod voting_loop;

pub type Stake = u64;
pub const SUPERMAJORITY: f64 = 2f64 / 3f64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CertificateType {
    Finalize,
    FinalizeFast,
    Notarize,
    NotarizeFallback,
    Skip,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VoteType {
    Finalize,
    Notarize,
    NotarizeFallback,
    Skip,
    SkipFallback,
}

pub const CONFLICTING_VOTETYPES: [(VoteType, VoteType); 5] = [
    (VoteType::Finalize, VoteType::NotarizeFallback),
    (VoteType::Finalize, VoteType::Skip),
    (VoteType::Notarize, VoteType::Skip),
    (VoteType::Notarize, VoteType::NotarizeFallback),
    (VoteType::Skip, VoteType::SkipFallback),
];

pub const CERTIFICATE_LIMITS: [(CertificateType, (f64, &[VoteType])); 5] = [
    (CertificateType::FinalizeFast, (0.8, &[VoteType::Notarize])),
    (CertificateType::Finalize, (0.6, &[VoteType::Finalize])),
    (CertificateType::Notarize, (0.6, &[VoteType::Notarize])),
    (
        CertificateType::NotarizeFallback,
        (0.6, &[VoteType::Notarize, VoteType::NotarizeFallback]),
    ),
    (
        CertificateType::Skip,
        (0.6, &[VoteType::Skip, VoteType::SkipFallback]),
    ),
];

pub const MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES: usize = 1;

pub const MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE: usize = 3;

// To avoid attacks, we only accept votes 512 slots newer than root.
pub const MAX_SLOT_AGE: u64 = 512;

pub const SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY: f64 = 0.4;
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP: f64 = 0.2;
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP: f64 = 0.6;

pub const SAFE_TO_SKIP_THRESHOLD: f64 = 0.4;

/// The amount of time a leader has to build their block in ms
pub const BLOCKTIME: u128 = 400;

/// The maximum message delay in ms
pub const DELTA: u128 = 100;

/// The Maximum delay a node can observe between entering the loop iteration
/// for a window and receiving any shred of the first block of the leader.
/// As a conservative global constant we set this to 3 * DELTA
pub const DELTA_TIMEOUT: u128 = 300;
