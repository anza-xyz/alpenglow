pub mod certificate_pool;
pub mod skip_pool;
pub mod vote_certificate;
pub mod vote_history;
pub mod vote_history_storage;

pub type Stake = u64;
pub const SUPERMAJORITY: f64 = 2f64 / 3f64;
