pub mod certificate_pool;
pub mod skip_pool;
pub mod utils;
pub mod vote_certificate;
pub mod vote_history;
pub mod vote_history_storage;
pub mod voting_loop;

pub type Stake = u64;
pub const SUPERMAJORITY: f64 = 2f64 / 3f64;

/// The amount of time a leader has to build their block in ms
pub const BLOCKTIME: u128 = 400;

/// The maximum message delay in ms
pub const DELTA: u128 = 100;

/// The Maximum delay a node can observe between entering the loop iteration
/// for a window and receiving any shred of the first block of the leader.
/// As a conservative global constant we set this to 3 * DELTA
pub const DELTA_TIMEOUT: u128 = 300;
