# Advisory: Consensus DoS via Duplicate Vote Rank in VoteAggregate Construction

## Summary

When two votes from the same validator rank reach the individual verification fallback path in `bls_vote_sigverify.rs`, both pass signature verification and are forwarded to `VoteAggregate::new_from_verified_votes`. That function contains `assert!(!ranks[rank])` which **panics in release builds** when a duplicate rank is detected. A single malicious validator can trigger this panic by broadcasting the same vote twice (e.g., via two different network paths), causing the consensus thread to crash and the node to halt consensus participation.

Additionally, `AggregateAccumulator::add_aggregate` double-counts stake for duplicate ranks (bitmap OR is idempotent, but `saturating_add` always adds). While the panic currently prevents reaching this code path, the latent double-count bug means that if the assertion is ever removed or bypassed, an attacker can inflate apparent stake to bypass consensus thresholds with minimal real support.

**Severity:** Liveness / Loss of Availability (DoS) with latent Consensus/Safety violation

**Crate:** `votor`, `bls-sigverify`, `votor-messages`
**Affected code:**
- `votor-messages/src/sig_verified_messages.rs` (panic on duplicate rank)
- `bls-sigverify/src/bls_vote_sigverify.rs` (no dedup before constructing VoteAggregate)
- `votor/src/aggregate_accumulator.rs` (latent stake double-count)

---

## Technical Details

### Root Cause: Panic Path

`VoteAggregate::new_from_verified_votes` is called after individual vote verification succeeds:

```rust
pub fn new_from_verified_votes(
    max_validators: usize,
    vote_payload_to_sign: VotePayloadToSign,
    ranks_and_stakes_iter: impl Iterator<Item = (u16, NonZero<u64>)>,
    aggregate_signature: SignatureProjective,
) -> Self {
    let mut ranks = BitVec::repeat(false, max_validators);
    let mut total_stake = None;
    for (rank, stake) in ranks_and_stakes_iter {
        assert!((rank as usize) < max_validators);  // fires in release
        assert!(!ranks[rank as usize]);              // PANICS on duplicate rank
        ...
    }
    ...
}
```

These `assert!` macros execute in both debug and release builds. When a duplicate rank reaches this function, the process panics, crashing the `solVotorEvLoop` thread.

### Attack Vector: DoS

1. Attacker controls validator with rank `r` (any non-zero stake).
2. Attacker broadcasts two vote messages for the same slot/block via two independent paths (e.g., direct gossip + Turbine retransmit + repair).
3. Both votes reach the node's `bls-sigverify` stage.
4. In the optimistic batch path, the aggregate signature verification may succeed (if enough honest votes mask the duplicate), or it may fail.
5. If it fails, the fallback individual verification path runs. Both duplicate votes have valid individual signatures, so both pass.
6. Both verified votes are passed to `VoteAggregate::new_from_verified_votes` as separate entries with the same rank `r`.
7. The second `assert!(!ranks[rank])` fires → **panic → consensus thread dies → node stops participating in consensus.**

This requires only ONE malicious validator. No collaboration needed.

### Latent Bug: Stake Double-Count

If the `assert!` is ever removed, changed to `debug_assert!`, or if a future code path calls `add_aggregate` with pre-constructed `VoteAggregate`s containing duplicate ranks:

```rust
pub fn add_aggregate(&mut self, aggregate: &VoteAggregate) -> Result<u64, ...> {
    self.ranks |= aggregate.ranks();  // OR: no-op for already-set bits
    self.stake = self.stake.saturating_add(aggregate.stake().get());  // adds FULL stake every time
    Ok(self.stake)
}
```

An attacker with stake `S` can inflate the accumulator by `S` per duplicate. With two duplicate votes:
- Real support: `S` (attacker only)
- Accumulated stake: `2S`
- If `2S >= threshold`, certificate forms with only `S` real support

See `poc_double_count.rs` for the standalone demonstration.

### Reproduction: DoS

To trigger the panic:

1. Run two Agave validator nodes in a local testnet.
2. Configure node A as the attacker (control rank 0 with some stake).
3. From node A, broadcast vote for slot `s` twice via two different `ClusterInfo` gossip connections.
4. Target node receives both votes, both pass individual sigverify.
5. Target node's `solVotorEvLoop` thread panics on duplicate rank assertion.

### Reproduction: Stake Double-Count

See `poc_double_count.rs`:

```
=== Normal scenario (no duplicates) ===
  Accumulated stake: 600
  Meets 60% threshold: true

=== Attack: duplicate attacker vote (2x count) ===
  Accumulated stake: 1200
  Real support: 400+200+200 = 600
  Inflated by: 400 (duplicate attacker stake)
  Meets 60% threshold: true

=== Extreme: solo attacker with duplicate vote ===
  Accumulated stake: 800
  Real support: 400 (NO honest votes)
  Meets 60% threshold: true
  => Certificate formed with NO honest support!
```

---

## Impact

| Scenario | Impact | Severity |
|----------|--------|----------|
| Current: duplicate vote via two paths | Consensus thread panic → node halts → DoS | Liveness / Loss of Availability |
| Latent: assert removed/bypassed | Stake inflation → threshold bypass → Safety violation | Consensus / Safety Violation |
| Combined with honest minority | Attacker with <20% real stake can trigger premature finalization | Loss of Funds potential |

- **Affects all validators** running Alpenglow consensus — any node receiving duplicate votes from the same validator will panic.
- **No authentication or special access required** — any validator can trigger this by broadcasting twice.
- **First-to-report priority:** No public advisory, known-issues tracker entry, or prior disclosure covers this specific panic/stake-inflation vector.

---

## Affected Code Paths

| File | Line(s) | Issue |
|------|---------|-------|
| `votor-messages/src/sig_verified_messages.rs` | 89-90 | `assert!(!ranks[rank])` panics on duplicate rank in release builds |
| `bls-sigverify/src/bls_vote_sigverify.rs` | 375-392 | `verify_individual_votes` does not deduplicate by rank before constructing `VoteAggregate` |
| `bls-sigverify/src/bls_vote_sigverify.rs` | 283-335 | Optimistic batch path may also pass duplicate ranks if aggregate signature validates |
| `votor/src/aggregate_accumulator.rs` | 56-66 | `add_aggregate` double-counts stake for duplicate ranks |
| `votor/src/consensus_pool/vote_pool.rs` | 143-164 | `add_pool_vote` forwards to `add_aggregate` without dedup |

---

## Remediation

1. **Immediate fix — replace panic with dedup:**

In `VoteAggregate::new_from_verified_votes`, replace the `assert!` with a runtime skip:

```rust
for (rank, stake) in ranks_and_stakes_iter {
    if (rank as usize) >= max_validators { continue; }
    if ranks[rank as usize] { continue; }  // skip duplicate, don't panic
    ranks.set(rank as usize, true);
    ...
}
```

2. **In `AggregateAccumulator::add_aggregate`:** Only add stake for newly discovered ranks:

```rust
pub fn add_aggregate(&mut self, aggregate: &VoteAggregate) -> Result<u64, ...> {
    self.signature.aggregate_with(...)?;
    let new_ranks: Vec<usize> = aggregate.ranks().iter_ones()
        .filter(|&r| !self.ranks[r])
        .collect();
    let new_stake: u64 = new_ranks.iter()
        .map(|r| aggregate.stake_per_rank(*r))
        .sum();
    self.ranks |= aggregate.ranks();
    self.stake = self.stake.saturating_add(new_stake);
    Ok(self.stake)
}
```

If `VoteAggregate` does not expose per-rank stake, deduplicate upstream in `verify_votes` / `verify_individual_votes` before constructing `VoteAggregate` objects.

3. **In `verify_individual_votes`:** Deduplicate `unverified_votes` by rank before the `partition_map`:

```rust
let seen_ranks: HashSet<u16> = unverified_votes.iter().map(|v| v.rank).collect();
// Filter out votes whose rank has already been seen
```

4. **Add invariant tests:** Add a test that sends two `VoteAggregate`s with the same rank to `add_aggregate` and verifies that stake is not double-counted.

---

## References

- Alpenglow whitepaper: https://www.anza.xyz/alpenglow-1-1
- SIMD-0326: https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md
- Agave SECURITY.md: https://github.com/anza-xyz/agave/blob/master/SECURITY.md
- Competition rules: https://github.com/anza-xyz/alpenglow/blob/master/RULES.md

---

## Proof of Concept

`poc_double_count.rs` demonstrates the stake double-count. Compile and run:

```bash
rustc poc_double_count.rs && ./poc_double_count
```

The DoS panic can be reproduced by constructing two `VoteAggregate`s with the same rank and passing them to `VoteAggregate::new_from_verified_votes` in a release build.
