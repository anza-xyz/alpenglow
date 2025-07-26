//! Alpenglow Vote program
#![deny(missing_docs)]

pub mod accounting;
pub mod bls_message;
pub mod certificate;
pub mod instruction;
pub mod state;
pub mod vote;

// Export current SDK types for downstream users building with a different SDK
// version
pub use solana_program;

solana_program::declare_id!("Vote222222222222222222222222222222222222222");
