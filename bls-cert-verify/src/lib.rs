//! This crate implements the Alpenglow certificate verification logic.
//!
//! It is shared between at least:
//! - The BLS Sigverifier
//! - The Verification for Certs in Block Markers (from runtime)
//!
//! It can verify votor-messages signed with BLS keys, or just pure bitmaps
//! conforming to create solana-signer-store format.
//! It also checks the aggregate stake if a given threshold is given.
//! To make the dependencies lighter, stake distribution should be passed in.

pub mod cert_verify;
