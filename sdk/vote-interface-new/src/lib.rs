#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
//! The [vote-new native program][np].
//!
//! [np]: https://docs.solanalabs.com/runtime/programs#vote-program-new

pub mod authorized_voters;
pub mod error;
pub mod instruction;
pub mod state;

pub mod program {
    pub use solana_sdk_ids::vote_new::{check_id, id, ID};
}
