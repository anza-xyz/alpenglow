# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Alpenglow is a fork of Agave/Solana implementing a new consensus mechanism that replaces Tower-BFT with a certificate-based BFT system using BLS signatures. The codebase maintains the core Solana validator architecture while introducing significant consensus changes.

## Build Commands

```bash
./cargo build                    # Debug build (uses wrapper script with proper env)
./cargo build --release          # Release build (required for validators)
./cargo test                     # Run all tests
./cargo test -p <crate_name>     # Test a specific crate
./cargo test <test_name>         # Run a single test by name
```

## Code Quality

```bash
./cargo clippy --all             # Lint checks (required before PR)
./cargo fmt --all                # Format code
```

CI scripts for comprehensive checks:
```bash
ci/test-checks.sh                # Full lint, clippy, audit checks
ci/test-stable.sh                # Full stable test suite
```

## Architecture

### Alpenglow-Specific Components

**Votor** (`/votor`) - Core consensus engine implementing Alpenglow BFT:
- `consensus_pool.rs` - Main voting pool, certificate builder, vote aggregation
- `event_handler.rs` - Processes consensus events
- `voting_service.rs` - Manages voting lifecycle
- `consensus_rewards.rs` - Reward certificate aggregation

**Votor-Messages** (`/votor-messages`) - Protocol message types:
- Three vote types: `NotarizationVote`, `FinalizationVote`, `SkipVote`
- Reward certificates: `SkipRewardCertificate`, `NotarRewardCertificate`

**Vortexor** (`/vortexor`) - Transaction verification offloading:
- Receives transactions, performs signature verification/deduplication
- Forwards verified packets to validators via UDP/QUIC
- Can run on separate nodes for scalability

**BLS-Cert-Verify** (`/bls-cert-verify`) - Shared BLS certificate verification

### Core Validator Components

- `core/` - Main blockchain engine (banking, block creation, replay, consensus)
- `runtime/` - Transaction execution and state management
- `ledger/` - Blockstore, shred handling, persistence
- `gossip/` - P2P protocol for cluster communication
- `turbine/` - Block propagation protocol
- `validator/` - Validator entry point

## Key Conventions

### Rust Toolchain
- Stable: Rust 1.86.0 (defined in `rust-toolchain.toml`)
- Nightly required only for benchmarks: `cargo +nightly bench`

### Linting Rules (from `clippy.toml`)
- `arithmetic_side_effects = "deny"` - Must handle overflow explicitly
- `default_trait_access = "deny"` - Use `Type::default()` not `Default::default()`
- Use `solana_net_utils::bind_*` functions instead of direct `UdpSocket::bind`
- Use `std::sync::{LazyLock, OnceLock}` instead of `lazy_static!`

### Code Style
- Variable names: lowercase type name with underscores before capitals
- Function names: `<verb>_<subject>` pattern
- Test functions: `test_<subject>`
- Only use `unwrap()` when provably safe; prefer `.expect()` with message

### PRs and Consensus
- PRs should be under ~1000 lines of actual changes
- Consensus-breaking changes require feature gates and merged SIMD
- All changes need 90%+ test coverage with fast, non-flaky tests
- Include benchmark results for performance-impacting changes

### Feature Gates
Alpenglow-specific features are controlled via feature gates in `/feature-set`. Any consensus changes must be gated and activated through the standard feature gate process.
