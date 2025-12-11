use {
    solana_votor_messages::{
        consensus_message::CertificateType,
        migration::GENESIS_VOTE_THRESHOLD,
        vote::{Vote, VoteType},
    },
    std::time::Duration,
};

// Core consensus types and constants
pub type Stake = u64;

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
        VoteType::Genesis => &[
            VoteType::Finalize,
            VoteType::Notarize,
            VoteType::NotarizeFallback,
            VoteType::Skip,
            VoteType::SkipFallback,
        ],
    }
}

/// Lookup from `CertificateId` to the `VoteType`s that contribute,
/// as well as the stake fraction required for certificate completion.
///
/// Must be in sync with `vote_to_cert_types`
pub const fn certificate_limits_and_vote_types(
    cert_type: &CertificateType,
) -> (Fraction, &'static [VoteType]) {
    match cert_type {
        CertificateType::Notarize(_, _) => (Fraction::new(3, 5), &[VoteType::Notarize]),
        CertificateType::NotarizeFallback(_, _) => (
            Fraction::new(3, 5),
            &[VoteType::Notarize, VoteType::NotarizeFallback],
        ),
        CertificateType::FinalizeFast(_, _) => (Fraction::new(4, 5), &[VoteType::Notarize]),
        CertificateType::Finalize(_) => (Fraction::new(3, 5), &[VoteType::Finalize]),
        CertificateType::Skip(_) => (
            Fraction::new(3, 5),
            &[VoteType::Skip, VoteType::SkipFallback],
        ),
        CertificateType::Genesis(_, _) => (
            Fraction::from_tuple(GENESIS_VOTE_THRESHOLD),
            &[VoteType::Genesis],
        ),
    }
}

/// Numerator / denominator, with denominator != 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fraction {
    pub numerator: u64,
    pub denominator: u64,
}

impl Fraction {
    #[inline]
    pub const fn new(numerator: u64, denominator: u64) -> Self {
        debug_assert!(denominator != 0);
        Self {
            numerator,
            denominator,
        }
    }

    #[inline]
    pub const fn from_tuple((numerator, denominator): (u64, u64)) -> Self {
        Self::new(numerator, denominator)
    }
}

impl PartialOrd for Fraction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Fraction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Bowtie method to avoid division
        let lhs = (self.numerator as u128)
            .checked_mul(other.denominator as u128)
            .unwrap();
        let rhs = (other.numerator as u128)
            .checked_mul(self.denominator as u128)
            .unwrap();
        lhs.cmp(&rhs)
    }
}

/// Lookup from `Vote` to the `CertificateId`s the vote accounts for
///
/// Must be in sync with `certificate_limits_and_vote_types` and `VoteType::get_type`
pub fn vote_to_cert_types(vote: &Vote) -> Vec<CertificateType> {
    match vote {
        Vote::Notarize(vote) => vec![
            CertificateType::Notarize(vote.slot, vote.block_id),
            CertificateType::NotarizeFallback(vote.slot, vote.block_id),
            CertificateType::FinalizeFast(vote.slot, vote.block_id),
        ],
        Vote::NotarizeFallback(vote) => {
            vec![CertificateType::NotarizeFallback(vote.slot, vote.block_id)]
        }
        Vote::Finalize(vote) => vec![CertificateType::Finalize(vote.slot)],
        Vote::Skip(vote) => vec![CertificateType::Skip(vote.slot)],
        Vote::SkipFallback(vote) => vec![CertificateType::Skip(vote.slot)],
        Vote::Genesis(vote) => vec![CertificateType::Genesis(vote.slot, vote.block_id)],
    }
}

pub const MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES: usize = 1;
pub const MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE: usize = 3;

pub const SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY: Fraction = Fraction::new(2, 5);
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP: Fraction = Fraction::new(1, 5);
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP: Fraction = Fraction::new(3, 5);

pub const SAFE_TO_SKIP_THRESHOLD: Fraction = Fraction::new(2, 5);

/// Time bound assumed on network transmission delays during periods of synchrony.
pub(crate) const DELTA: Duration = Duration::from_millis(250);

/// Time the leader has for producing and sending the block.
pub(crate) const DELTA_BLOCK: Duration = Duration::from_millis(400);

/// Base timeout for when leader's first slice should arrive if they sent it immediately.
pub(crate) const DELTA_TIMEOUT: Duration = DELTA.checked_mul(3).unwrap();

/// Timeout for standstill detection mechanism.
pub(crate) const DELTA_STANDSTILL: Duration = Duration::from_millis(10_000);

/// Returns the Duration for when the `SkipTimer` should be set for for the given slot in the leader window.
#[inline]
pub fn skip_timeout(leader_block_index: usize) -> Duration {
    DELTA_TIMEOUT
        .saturating_add(
            DELTA_BLOCK
                .saturating_mul(leader_block_index as u32)
                .saturating_add(DELTA_TIMEOUT),
        )
        .saturating_add(DELTA)
}

/// Block timeout, when we should publish the final shred for the leader block index
/// within the leader window
#[inline]
pub fn block_timeout(leader_block_index: usize) -> Duration {
    // TODO: based on testing, perhaps adjust this
    DELTA_BLOCK.saturating_mul((leader_block_index as u32).saturating_add(1))
}
