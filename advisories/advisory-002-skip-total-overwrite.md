# Advisory: Liveness Violation via skip_total Overwrite in SlotStakeCounters::add_vote

## Summary

`SlotStakeCounters::add_vote` in `votor/src/consensus_pool/slot_stake_counters.rs` **overwrites** `skip_total` instead of summing it when processing `Vote::Skip` entries. Each new Skip vote replaces the previous value rather than accumulating. This causes `is_safe_to_skip()` to compute using only the **most recent** Skip vote's stake, ignoring all others. When the most recent Skip vote has lower stake than a previous one, the accumulated skip support is undercounted, and `SafeToSkip` never fires — even when a majority of validators want to skip the slot.

**Severity:** Liveness / Loss of Availability

**Crate:** `votor`
**Affected code:** `votor/src/consensus_pool/slot_stake_counters.rs`

---

## Technical Details

### Root Cause

In `SlotStakeCounters::add_vote`, the `Vote::Skip` arm assigns rather than accumulates:

```rust
pub(super) fn add_vote(&mut self, vote: &Vote, entry_stake: Stake, ...) {
    match vote {
        Vote::Skip(_) => self.skip_total = entry_stake,  // BUG: overwrites!
        Vote::Notarize(vote) => {
            let old_entry_stake = self
                .notarize_entry_total
                .insert(vote.block.block_id, entry_stake)
                .unwrap_or(0);
            self.notarize_total = self
                .notarize_total
                .saturating_sub(old_entry_stake)
                .saturating_add(entry_stake); // correct: sum with dedup
            self.top_notarized_stake = self.top_notarized_stake.max(entry_stake);
        }
        _ => return,
    }
    ...
}
```

The `Notarize` arm correctly uses `BTreeMap::insert` to deduplicate per-block, then sums via `saturating_sub(old).saturating_add(new)`. The `Skip` arm has no per-block tracking (Skip is slot-wide, not block-specific), but should still **sum** all Skip votes.

### Attack Vector

This bug does not require an attacker. It is a **deterministic logic error** triggered by any scenario where multiple Skip votes arrive in sequence:

1. Slot `s` is proposed by leader L.
2. Multiple validators detect the block as invalid/empty and vote Skip.
3. Skip votes arrive at the node in sequence via gossip.
4. The first Skip vote (say, from validator A with 400 stake) sets `skip_total = 400`.
5. The second Skip vote (from validator B with 350 stake) **overwrites** `skip_total = 350`.
6. `is_safe_to_skip()` computes: `skip_total + notarize_total - top_notarized = 350 + 0 - 0 = 350`.
7. `Fraction(350, 1000) = 35% < 40%` → `SafeToSkip` does **not** fire.
8. The slot is never skipped, even though 750/1000 = 75% of stake voted Skip.

### Reproduction

See `poc_skip_overwrite.rs`:

```
=== Buggy implementation ===
After validator A (Skip, 400): skip_total = 400
After validator B (Skip, 350): skip_total = 350 (OVERWROTE!)
Real skip support: 750 (75%), SafeToSkip: false (BUG!)

=== Fixed implementation ===
After A (Skip, 400): skip_total = 400
After B (Skip, 350): skip_total = 750
SafeToSkip: true (correct)
```

### Impact

- **Liveness Violation:** `SafeToSkip` never fires when a smaller Skip vote follows a larger one. The slot is never skipped, causing the consensus loop to wait indefinitely for a notarization certificate that may never come (if the block is genuinely unbuildable).
- **Consensus Stall:** Validators that correctly vote Skip will see their votes ignored. The slot remains open, leader schedule advances, but the orphaned slot blocks downstream processing.
- **Cascading Effect:** If multiple consecutive slots have skip-worthy blocks, the cumulative stall can cause the node to fall multiple slots behind, requiring manual intervention or catchup.
- **Amplification with DoS:** An attacker who can influence the ordering of Skip vote delivery (e.g., via targeted gossip delays) can ensure smaller Skip votes arrive after larger ones, maximizing the undercount and ensuring SafeToSkip never fires.
- **First-to-report priority:** No public advisory or known-issues tracker entry covers this specific `skip_total` overwrite bug.

---

## Affected Code Paths

| File | Line(s) | Issue |
|------|---------|-------|
| `votor/src/consensus_pool/slot_stake_counters.rs` | 59 | `Vote::Skip(_) => self.skip_total = entry_stake` — overwrites instead of summing |
| `votor/src/consensus_pool/slot_stake_counters.rs` | 140-162 | `is_safe_to_skip()` uses undercounted `skip_total` |
| `votor/src/consensus_pool/vote_pool.rs` | 143-164 | `add_pool_vote` calls `add_vote` without skip_total dedup |
| `votor/src/consensus_pool.rs` | 56-66 | `ConsensusPool::process_event` dispatches to `SlotStakeCounters::add_vote` |

---

## Remediation

Replace the assignment with accumulation:

```rust
Vote::Skip(_) => {
    self.skip_total = self.skip_total.saturating_add(entry_stake);
}
```

This matches the pattern used in the `Notarize` arm and correctly sums all Skip votes across the slot.

Add a test case: submit 3 Skip votes with stakes 400, 350, 200 in sequence, verify `skip_total == 950` and `SafeToSkip` fires.

---

## References

- Alpenglow whitepaper: https://www.anza.xyz/alpenglow-1-1
- SIMD-0326: https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md
- Agave SECURITY.md: https://github.com/anza-xyz/agave/blob/master/SECURITY.md
- Competition rules: https://github.com/anza-xyz/alpenglow/blob/master/RULES.md

---

## Proof of Concept

`poc_skip_overwrite.rs` demonstrates the overwrite bug. Compile and run:

```bash
rustc poc_skip_overwrite.rs && ./poc_skip_overwrite
```

Expected output shows buggy `skip_total = 350` after two Skip votes (400 + 350), while fixed version correctly shows `skip_total = 750`.
