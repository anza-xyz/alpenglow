//! Logic detailing the migration from TowerBFT to Alpenglow
//!
//! The migration process will begin after a certain slot offset in the first epoch
//! where the `alpenglow` feature flag is active.
//!
//! Once the migration starts:
//! - We enter vote only mode, no user txs will be present in blocks
//! - We stop rooting or reporting OC/Finalizations
//!
//! During the migration starting at slot `s`:
//! 1) We track blocks which have `GENESIS_VOTE_THRESHOLD`% of stake's vote txs for the parent block.
//!    The parent block is referred to as reaching super OC.
//! 2) Notice that all super OC blocks that must be a part of the same fork in presence of
//!    less than `MIGRATION_MALICIOUS_THRESHOLD` double voters
//! 3) We find the latest ancestor of the super OC block < `s`, `G` and cast a BLS vote (the genesis vote) via all to all
//! 4) If we observe `GENESIS_VOTE_THRESHOLD`% votes for the ancestor block `G`:
//!    5a) We clear any TowerBFT blocks past `G`.
//!    5b) We propagate the Genesis certificate for `G` via all to all
//! 5) We initialize Votor with `G` as genesis, and disable TowerBFT for any slots past `G`
//! 6) We exit vote only mode, and reenable rooting and commitment reporting
//!
//! If at any point during the migration we see:
//! - A genesis certificate
//! - A finalization certificate
//!
//! It means the cluster has already switched to Alpenglow and our node is behind. We perform any appropriate
//! repairs and immediately transition to Alpenglow at the certified block.
use solana_clock::Slot;

/// The slot offset post feature flag activation to begin the migration.
/// Epoch boundaries induce heavy computation often resulting in forks. It's best to decouple the migration period
/// from the boundary. We require that a root is made between the epoch boundary and this migration slot offset.
pub const MIGRATION_SLOT_OFFSET: Slot = 5000;

/// We match Alpenglow's 20 + 20 model, by allowing a maximum of 20% malicious stake during the migration.
pub const MIGRATION_MALICIOUS_THRESHOLD: f64 = 20.0 / 100.0;

/// In order to rollback a block eligble for genesis vote, we need:
/// `SWITCH_FORK_THRESHOLD` - (1 - `GENESIS_VOTE_THRESHOLD`) = `MIGRATION_MALICIOUS_THRESHOLD` malicious stake.
///
/// Using 38% as the `SWITCH_FORK_THRESHOLD` gives us 82% for `GENESIS_VOTE_THRESHOLD`.
pub const GENESIS_VOTE_THRESHOLD: f64 = 82.0 / 100.0;
