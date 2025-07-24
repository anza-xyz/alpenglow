//! Alpenglow Vote program
#![deny(missing_docs)]
// Magic to enable frozen abi for on chain programs
#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]

pub mod accounting;
pub mod bls_message;
pub mod certificate;
mod entrypoint;
pub mod error;
pub mod instruction;
pub mod processor;
pub mod state;
pub mod vote;
mod vote_processor;

// Export current SDK types for downstream users building with a different SDK
// version
pub use solana_program;

solana_program::declare_id!("Vote222222222222222222222222222222222222222");

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;
