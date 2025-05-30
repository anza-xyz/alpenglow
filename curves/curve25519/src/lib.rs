#![allow(clippy::arithmetic_side_effects, clippy::op_ref)]
//! Syscall operations for curve25519

pub mod edwards;
pub mod errors;
pub mod ristretto;
pub mod scalar;
