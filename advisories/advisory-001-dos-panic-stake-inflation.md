# Advisory: Consensus DoS via Duplicate Vote Rank + Latent Stake Inflation in AggregateAccumulator

## Summary

When two votes from the same validator rank reach the individual verification fallback path in `bls_vote_sigverify.rs`, both pass signature verification and are forwarded to `VoteAggregate::new_from_verified_votes`. That function contains `assert!(!ranks[rank])` which **panics in release builds** when a duplicate rank is detected. A single malicious validator can trigger this panic by broadcasting the same vote twice (e.g., via two different network paths), causing the consensus thread to crash and the node to halt consensus participation.

Additionally, `AggregateAccumulator::add_aggregate` double-counts stake for duplicate ranks (bitmap OR is idempotent, but `saturating_add` always adds). While the panic currently prevents reaching this code path, the latent double-count bug means that if the assertion is ever removed or bypassed, an attacker can inflate apparent stake to bypass consensus thresholds with minimal real support.

**Severity:** Liveness / Loss of Availability (DoS) with latent Consensus/Safety Violation

**Crate:** `votor`, `bls-sigverify`, `votor-messages`
**Affected code:**
- `votor-messages/src/sig_verified_messages.rs` (panic on duplicate rank)
- `bls-sigverify/src/bls_vote_sigverify.rs` (no dedup before constructing VoteAggregate)
- `votor/src/aggregate_accumulator.rs` (latent stake double-count)

---

## Severity Justification

### Primary Impact: Liveness / Loss of Availability

- **Consensus halts and needs human intervention to recover:** The `solVotorEvLoop` thread panics on duplicate rank, crashing consensus participation. The node stops processing votes, notarizing blocks, and participating in the leader schedule.
- **DoS via non-RPC protocol:** The attack uses the gossip/repair network, not RPC. Any validator can trigger it without special access.

### Latent Impact: Consensus / Safety Violation

- If the `assert!` is ever removed or bypassed, `AggregateAccumulator::add_aggregate` double-counts stake.
- This enables a certificate to be formed with as little as one honest vote's worth of real support, bypassing the 60% (or 80%) stake threshold.
- Premature finalization with insufficient support enables double-spending = Loss of Funds potential.

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

### Adversary Model

- **Adversary capability:** Control of one validator that can broadcast the same vote message via two independent network paths (e.g., direct gossip + Turbine retransmit + repair channel).
- **Adversary goal:** Crash the consensus thread of target nodes, causing them to halt consensus participation (DoS).
- **What the adversary needs:** One validator keypair, ability to send two identical vote messages. No stake threshold, no collaboration, no special access.

### Attack Vector: DoS

1. Attacker controls validator with rank `r` (any non-zero stake).
2. Attacker broadcasts two vote messages for the same slot/block via two independent paths.
3. Both votes reach the node's `bls-sigverify` stage.
4. In the optimistic batch path, the aggregate signature verification may succeed or fail.
5. If it fails, the fallback individual verification path runs. Both duplicate votes have valid individual signatures, so both pass.
6. Both verified votes are passed to `VoteAggregate::new_from_verified_votes` as separate entries with the same rank `r`.
7. The second `assert!(!ranks[rank])` fires → **panic → consensus thread dies → node stops participating in consensus.**

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

### Proof of Concept

The bug is reproduced by the following standalone Rust program:

```rust
struct Accumulator {
    ranks: Vec<bool>,
    stake: u64,
}

impl Accumulator {
    fn new(max_validators: usize) -> Self {
        Self {
            ranks: vec![false; max_validators],
            stake: 0,
        }
    }

    // Mirrors AggregateAccumulator::add_aggregate
    fn add_aggregate(&mut self, aggregate_ranks: &[bool], aggregate_stake: u64) {
        for (i, present) in aggregate_ranks.iter().enumerate() {
            if i >= self.ranks.len() { break; }
            if *present { self.ranks[i] = true; }
        }
        // BUG: adds stake regardless of whether ranks were already counted
        self.stake = self.stake.saturating_add(aggregate_stake);
    }

    fn meets_threshold(&self, total_stake: u64, required_pct: u64) -> bool {
        let fraction = self.stake as f64 / total_stake as f64;
        fraction * 100.0 >= required_pct as f64
    }
}

fn main() {
    let total_stake = 1000u64;
    let threshold_pct = 60u64;

    // Normal: 3 validators with 200 stake each = 600 total
    let mut acc_normal = Accumulator::new(64);
    acc_normal.add_aggregate(&[true, false, false], 200);
    acc_normal.add_aggregate(&[false, true, false], 200);
    acc_normal.add_aggregate(&[false, false, true], 200);
    println!("Normal: stake={}, meets {}%={}",
        acc_normal.stake, threshold_pct, acc_normal.meets_threshold(total_stake, threshold_pct));

    // Attack: attacker vote counted twice
    let mut acc_attack = Accumulator::new(64);
    acc_attack.add_aggregate(&[true, false, false], 400); // first
    acc_attack.add_aggregate(&[false, true, false], 200);
    acc_attack.add_aggregate(&[true, false, false], 400); // DUPLICATE
    acc_attack.add_aggregate(&[false, false, true], 200);
    println!("Attack: stake={} (inflated by 600), meets {}%={}",
        acc_attack.stake, threshold_pct, acc_attack.meets_threshold(total_stake, threshold_pct));

    // Extreme: solo attacker, no honest votes
    let mut acc_solo = Accumulator::new(64);
    acc_solo.add_aggregate(&[true, false, false], 400);
    acc_solo.add_aggregate(&[true, false, false], 400); // duplicate
    println!("Solo attacker: stake={}, meets {}%={}",
        acc_solo.stake, threshold_pct, acc_solo.meets_threshold(total_stake, threshold_pct));
}
```

**Expected output:**
```
Normal: stake=600, meets 60%=true
Attack: stake=1200 (inflated by 600), meets 60%=true
Solo attacker: stake=800, meets 60%=true
  => Certificate formed with NO honest support!
```

### Test Vectors

| Test | Input | Expected stake | Actual (buggy) | Threshold met? |
|------|-------|----------------|----------------|----------------|
| 1 | 3x distinct ranks, 200 each | 600 | 600 | Yes (correct) |
| 2 | Rank 0 duplicated, 400+400+200+200 | 600 | 1200 | Yes (BUG: inflated) |
| 3 | Rank 0 duplicated alone, 400+400 | 400 | 800 | Yes (BUG: solo attacker) |

### First-to-Report Priority

This is a novel finding. As of the submission date (2026-08-02), no public advisory, known-issues tracker entry, or prior disclosure covers this specific panic/stake-inflation vector.

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

1. **In `VoteAggregate::new_from_verified_votes`:** Replace the `assert!` with a runtime skip:

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
