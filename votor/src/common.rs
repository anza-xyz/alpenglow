use {
    solana_votor_messages::{fraction::Fraction, vote::VoteType},
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

pub const MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES: usize = 1;
pub const MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE: usize = 3;

pub const SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY: Fraction = Fraction::from_percentage(40);
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP: Fraction = Fraction::from_percentage(20);
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP: Fraction = Fraction::from_percentage(60);

pub const SAFE_TO_SKIP_THRESHOLD: Fraction = Fraction::from_percentage(40);

/// Time bound assumed on network transmission delays during periods of synchrony.
pub(crate) const DELTA: Duration = Duration::from_millis(250);

/// Time the leader has for producing and sending the block.
pub(crate) const DELTA_BLOCK: Duration = Duration::from_millis(200);

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
