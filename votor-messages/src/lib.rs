//! Alpenglow vote message types
#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
#![deny(missing_docs)]

use {
    serde::{Deserialize, Serialize},
    solana_hash::Hash,
};

pub mod consensus_message;
pub mod vote;

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;

/// For every FEC set (AKA slice) of shreds, we have a Merkle tree over the shreds
/// signed by the leader. This is the root.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceRoot(pub Hash);

/// We locally build a second Merkle tree over the SliceRoots of a given block.
/// The root identifies the block and facilitates repair in Alpenglow.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpenglowBlockId(pub Hash);
