# Alpenglow bug bounty: rejection criteria and known non-issues

These have been documented so submitters can avoid re-reporting work.

If your submission matches below, review the acceptance criteria and ensure you can prove an appropriate impact.

**These rules apply before anything below is considered. A finding that runs into one of them is closed on that basis alone:**

- Exceeding the security model is out of scope. The budget is strictly under 20% malicious stake and strictly under 20% offline stake; a finding that needs 20% or more of either, including a safety break that counts 20% or more of stake twice, is rejected.
- Leader equivocation (successfully delivering two different blocks via Rotor, not just sending them) does not break safety. Safety holds with under 20% byzantine stake, even under full asynchrony and any amount of offline stake. Liveness under an equivocating leader is only guaranteed with malicious plus offline stake under 20% combined, so a skipped window there is within the model. See Theorem 2 (Assumptions 1-3) and Corollary 44 of the [Alpenglow whitepaper](https://www.anza.xyz/alpenglow-1-1).
- Conditions that are not reachable in production are closed. Calling internal functions, hand-building state, or patching configuration shows only what the code does once that state exists. Show how an attacker reaches it.
- Running on a corrupted or stale ledger, snapshot, or vote_history.bin is treated as a malicious action by the validator operator. Maintaining that state through an untimely crash or corruption is the operator's responsibility: fetch a new snapshot or use --wait-to-vote-slot. Findings that depend on a validator restarting with stale or corrupted local state are closed.
- Losing one leader window is not a loss of availability. Skip voting is a normal outcome, the cluster certifies past the window, and the next one finalizes unaided.
- Migration runs under a stricter fault model than steady-state Alpenglow (SIMD-0384). It is not meant to complete with substantial stake offline or without an early-epoch root, so stalling there is intended.
- Reward attribution is deliberately permissive (SIMD-0326). Any valid vote earns rewards if it arrives before the deadline, whether or not the block existed or was finalized. Rewards missed through absent participation are forfeited by design, and gaming this path forfeits block rewards.
- Verify every citation against current master before filing. Cited code that does not exist, or that an upstream check already covers, is closed.

- [Intentional behavior](#intentional-behavior)
- [Bounded impact](#bounded-impact)
- [Already fixed or superseded](#already-fixed-or-superseded)

## Intentional behavior

| False positive summary | Component | Reason false positive |
|---|---|---|
| Leader-signed future shred makes migration purge scan linearly while holding the blockstore insertion mutex | Alpenglow migration purge in blockstore shred insertion | Clearing all future shreds is intended behavior, and the shred insertion filter's half-epoch window bounds the work. |
| Votes that are pre-signed, sent early, or name a block that never existed still earn vote credits and inflation rewards | Alpenglow reward certificates and vote ingress | Intended per SIMD-0326: any valid vote usable in a certificate earns rewards if it arrives before the reward deadline, and non-existence of a block is not provable. A validator gaming this forfeits block rewards, which are the bulk of validator income. |
| Destructive one-shot reward snapshot at R+8 lets a later valid vote recreate unreachable state, losing rewards | Block creation reward-certificate builder | Intentional cutoff: votes arriving after the snapshot simply miss out on credits. |
| A dropped or deferred FetchBlock event is never retried, stalling repair for that fork | Block-ID repair service event queue | Not a bug: fetch-block events follow from the cluster certifying a block, so a dropped event is requeued when a later block is certified, and Standstill refreshes old certificates. |
| Sparse future shreds make ReplayStage synchronously purge to the half-epoch horizon under the insert lock | Blockstore cleanup during Alpenglow migration | Intentional per SIMD-0384: all post-genesis shreds are deliberately cleared when Alpenglow is enabled. |
| Registering BLS keys that sum to the identity element lets non-signing accounts be counted for stake or credited with rewards | BLS certificate aggregation, stake accounting and reward attribution | Setting cancelling keys is a deliberate, self-inflicted malicious configuration and is immediately slashable. Proof-of-possession prevents cancelling an honest validator, the leader-side builder omits identity aggregates before a certificate is built, and forming a certificate this way needs far more stake than the model allows. |
| Footer certificate against a stale root bank lacking epoch stakes panics the consensus-pool thread | Consensus pool certificate ingestion | Intentional; a validator cannot be two epochs behind and catch up, so the precondition is not a supported state. |
| Rewards gated by next epoch's VAT-admitted set permanently deny rewards to tie-cutoff validators | Epoch stake reward distribution / VAT filtering | Intentional and mirrors TowerBFT inflation reward calculation; using the prior set is unsafe since restarting nodes lack that value during distribution. |
| One trailing byte makes genesis certificate deserialization return None, permanently wedging nodes through migration | Migration genesis certificate propagation through blockstore | The blockstore path is intentionally best-effort and is backstopped by A2A delivery of the certificate. |
| Reward certificate verification skips the 60% stake threshold, letting a leader take all rewards | Reward certificate verification | Intended per SIMD-0326: the leader's reward sharing with voters offsets this class of misbehavior. |
| Redemption reads only the latest epoch credits entry, stranding earlier rewards permanently unpayable | Runtime Alpenglow reward credit redemption | Deferred-reward forfeiture is intentional and documented in a merged PR: skipping validation duties for an epoch forfeits that epoch's rewards. See [anza-xyz/agave#13061](https://github.com/anza-xyz/agave/pull/13061) and its note on deferred rewards. |
| Alpenglow activation admits the migration epoch validator set without VAT filtering or the admission burn | Runtime epoch stakes and VAT burn at the migration epoch | Intentional per SIMD-0326 and the VAT SIMD: the first epoch is free because operators still pay 5k slots of TowerBFT vote costs. |
| Colluding validator and leader submit a notarization reward certificate for a block that never existed | Runtime reward certificates | By design: a non-notarized block is not guaranteed to be seen cluster-wide, so the requirement cannot be enforced. |
| A vote account closed before settlement is skipped at payout, dropping that signer's reward and the leader's matching share | Runtime reward settlement for vote accounts | Intended symmetry: the voter and the leader forfeit the same reward, and the party causing it is the voter whose own account was closed, not an unrelated actor. Nothing is taken from anyone. |
| A permissionless third-party transfer or delegation forces a victim vote account into the admitted set and burns its lamports | Runtime validator admission ticket (VAT) | Registering a vote account on chain is consent to be a validator and to pay the admission ticket. An operator who does not want to participate can close the vote account or keep it below the admission balance. |
| Per-epoch VAT burn drops offline validators from the rank map, shrinking the certificate stake denominator | Validator admission ticket (VAT) admission and certificate stake accounting | By design: the 20+20 model is defined only over the voting set that qualified for the VAT, so the shrunken denominator is intended. |
| Add_missing_parent_ready registers mid-window finalized slot, leaving a late-joining validator unable to resume voting | Votor event handler / ParentReady recovery | Intentional and does not impact recovery: ParentReady starts the timer and lets the rejoining validator vote skip or notar-fallback. |
| Mid-window finalization records an unusable parent-ready entry, suppressing window-start recovery and notarization | Votor event handler parent-ready recovery | The add_missing_parent_ready behavior is intended standstill recovery; notarizing at or below the finalized slot is pointless and no cluster liveness loss follows. |
| A slot is recorded as voted before the vote is actually generated or dispatched, so a skippable error leaves a ghost vote and silent abstention | Votor vote history and vote generation | Intended. Each of the cited failure conditions is precisely a case where the validator must not cast that vote, so recording the slot is the correct outcome rather than a lost vote. The ordering is also required for unstaked, non-voting and wait-to-vote nodes to operate normally. |
| The vote pool accepts a Notarize and a Finalize vote for the same slot from the same rank without treating it as equivocation | Votor vote pool admission | Permitted by the protocol: a node may vote Notarize and Finalize in the same slot, and the network can reorder those packets. Only duplicate Notarize votes are filtered, and conflicting certificates would still need 20% overlapping stake. |

## Bounded impact

The mechanism may be real, but the consequence is capped: the loss of a single leader window, a skip that the next window recovers from, memory bounded by the vote-window limit, or a slowdown that does not reproduce on real validator hardware. Quantify the impact on realistic hardware and say what the cluster-level consequence actually is.

| False positive summary | Component | Reason false positive |
|---|---|---|
| Unlimited sibling banks retain touched account copies in the write cache until rooting | Accounts write cache and replay bank forks | No OOM shown: Alpenglow roots roughly every 320ms and memory is reclaimed on purge, so banks cannot accumulate on a real cluster. |
| Mid-FEC-set UpdateParent marker ignored by ingest but honored by replay, splitting hard-dead decisions | Blockstore shred ingest versus replay (UpdateParent marker) | No divergence exists: ingest and replay apply the same rules, and such a block is marked hard dead rather than treated as an abandoned bank. |
| Votes naming many distinct future slots or block IDs retain per-slot state and exhaust validator memory or CPU | BLS sigverify vote pool / Votor consensus pool | The impact is bounded and this is not a denial of service, regardless of whether the future-slot window is 30,000 slots or 40. Admission accepts one vote per validator per slot and rejects conflicting values, so the retained state is shared across senders rather than multiplied by adding attackers, and it is reclaimed as the root advances. Load testing showed no memory or CPU degradation at the volumes these reports assume, and no OOM, crash or consensus effect has been demonstrated. The window has since been tightened to ParentReady + 40 by [anza-xyz/agave#14337](https://github.com/anza-xyz/agave/pull/14337), which lowers the ceiling further but is not what makes these reports invalid. Sustaining the attack also forfeits block rewards. |
| Ten-second BAN_TIMEOUT lets identities reconnect and repeatedly force costly invalid BLS verification work | BLS vote sigverify and transport ban handling | Not reproducible on modern validator hardware. Performance measured with all validators colocated on a single machine does not carry over to a real cluster. |

## Already fixed or superseded

Either a merged PR already closed it, an existing upstream check already covers it, or the cited code no longer exists on master. Verify every file, symbol, and line against current master immediately before filing.

| False positive summary | Component | Reason false positive |
|---|---|---|
| A vote-history restore failure during a runtime identity switch is swallowed and an empty history installed, letting the validator vote again on a slot it already voted | Admin RPC set-identity and vote history restore | Already covered upstream: the admin RPC set-identity path restores the target history first and rejects the request when the vote-history requirement applies, so the empty-history state is not reachable through ordinary administration. |
| Set_root erases parent notar-fallback status, blocking pending children from reaching SafeToNotar | Votor ParentReady tracker root pruning | The retain uses >= root so the root's own status is kept; chaining below root is correctly treated as unfinalizable and skipped. |

