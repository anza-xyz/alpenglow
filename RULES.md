# Alpenglow Bug Bounty Competition - Rules

Anza is running a time-boxed bug bounty competition on the Alpenglow consensus stack in Agave, with a prize pool of up to 50,000 SOL. For the competition window, the standing Agave bug bounty's exclusion of Alpenglow is lifted for the in-scope code. The standing program ([`agave/SECURITY.md`](https://github.com/anza-xyz/agave/blob/master/SECURITY.md)) is unchanged for everything else.

Reports are submitted through the submission portal at https://alpenglow.anza.xyz/, which files them as security advisories on the [`anza-xyz/alpenglow`](https://github.com/anza-xyz/alpenglow) repository; that repository hosts the competition rules and the advisory intake. The code under review is the Alpenglow consensus stack in the main [`anza-xyz/agave`](https://github.com/anza-xyz/agave) repository: `master` in these rules means `agave` `master`, and each report cites the `agave` commit it was found against (section 2). Known issues stay tracked in agave (section 8).

---

## 1. Overview

- **Prize pool:** Up to 50,000 SOL (section 5)  
- **Submission window:** 2026-08-05 16:00 UTC to 2026-08-19 16:00 UTC (2 weeks, Wednesday start)  
- **Scope basis:** continuous `master` HEAD, moving window (section 2)  
- **Adjudication close:** 2026-09-02, then payouts  
- **Submission channel:** the submission portal at https://alpenglow.anza.xyz/, which requires a non-refundable 0.5 SOL burn and files each finding as a GitHub Security Advisory on `anza-xyz/alpenglow`, one finding per advisory. Submissions received through any other channel are ineligible

---

## 2. Scope basis

Scope is defined by time, not by a frozen commit.

- The submission window (section 1) is the hard boundary; it closes the set of findings. Window times are UTC, and the advisory's GitHub timestamp is authoritative for both the window boundary and first-to-report priority (section 7).  
- In-scope code is the Alpenglow consensus surface on `agave` `master` as it evolves during the window, so fixes landed mid-window are themselves in scope.  
- Each report cites the `master` commit it was found against and is reproduced at that commit. The bug must still be live (unfixed on `master`) when the report is submitted; once an issue is fixed it is ineligible, even against an earlier in-window commit where it was present.  
- Findings are demonstrated on local forks, not against a live cluster.

---

## 3. Scope

In-scope is the Alpenglow consensus stack on `master` during the window, with all current mainnet features active and `alpenglow` (SIMD-0326), `alpenglow_fast_leader_handover` (SIMD-0337), and validator-admission (SIMD-0357) features active and the TowerBFT-to-Alpenglow migration path in scope. Anything reachable only with `alpenglow` inactive is TowerBFT-domain and out of scope. The test is Alpenglow-specificity: a fault is in scope if it occurs only because the Alpenglow feature is active, wherever in the tree it lives (for example, startup code that boot-loops only under Alpenglow). The areas listed out of scope below are excluded for their normal, non-Alpenglow behavior, not for Alpenglow-specific faults in them; this does not reach third-party dependencies, test code, or the standing out-of-scope set, which stay out regardless.

### Core crates

| Crate | What |
| :---- | :---- |
| `votor/` | Event loop; consensus pool (vote aggregation, certificate builder, parent-ready tracker, slot stake counters, vote pool); voting service and utils; vote history; timer manager; commitment; rooting |
| `votor-messages/` | Certificate, vote, consensus message, wire (de)serialization, reward certificate, migration handover, fraction |
| `bls-sigverify/` | BLS vote and certificate signature verification, reward sigverify |
| `bls-cert-verify/` | Certificate verification and stake-threshold checks |

The TowerBFT-to-Alpenglow migration logic (`votor-messages/src/migration.rs`) is in scope.

### Integration (in scope where it touches the Alpenglow path)

- `core/src/cluster_info_vote_listener.rs`: gossip vote ingestion  
- `core/src/block_creation_loop/rewards/**` (`certs_builder`, `reward_certs_service`, `certs_requestor`): aggregation of BLS votes into reward certificates  
- `runtime/src/validated_reward_certificate.rs`: reward-certificate validation  
- `runtime/src/validated_block_finalization.rs`: finalization-certificate validation  
- `runtime/src/epoch_stakes.rs`: BLS-pubkey rank map and voter set  
- `runtime/src/block_component_processor.rs` and `block_component_processor/vote_reward.rs`: block-component processing and vote-reward / credit accounting  
- `entry/src/block_component.rs`: block-component structure and parsing  
- `runtime/src/alpenglow_epoch_type.rs`: Alpenglow epoch-type semantics  
- `runtime/src/bank.rs`: validator-admission (VAT) enforcement  
- `runtime/src/bank_forks.rs`: Alpenglow rooting integration  
- `core/src/repair/{block_id_repair_service,repair_handler,serve_repair}.rs`: block-id (chained) repair  
- `core/src/replay_stage.rs` and `replay_stage/update_parent.rs`: Votor/replay integration, rooting, and the UpdateParent fast-handover markers  
- `ledger/src/blockstore_processor.rs`: UpdateParent replay offset and fast-leader-handover gating  
- `turbine/src/retransmit_stage.rs`: the `FirstShred` event into Votor  
- `core/src/window_service.rs`: duplicate-shred and chained-merkle-root handling

### Cryptographic primitives

BLS aggregation and verification logic, and its misuse, are in scope. Bugs in the underlying `blst` / BLS12-381 dependency are out of scope and should be reported upstream.

### Out of scope

- Startup and wiring, for their non-Alpenglow behavior: `core/src/{validator,tvu,tpu,admin_rpc_post_init}.rs` and the `block_creation_loop.rs` top-level orchestration (the certificate logic under `rewards/` remains in scope; an Alpenglow-only fault here is in scope per the test above)  
- TowerBFT-only or local-aggregation paths: `consensus.rs`, `commitment_service.rs`, `replay_stage/dead_slots.rs`  
- Leader-only block emission: `turbine/src/broadcast_stage.rs`  
- Any path reachable only when `alpenglow` is inactive (covered by the standing program)  
- Scaffolding behind features not active during the window  
- Test code, test harnesses, and `core/src/banking_simulation.rs`  
- The standing `agave/SECURITY.md` out-of-scope set (metrics, dependencies, snapshots, bootstrap-phase configuration, the in-built RPC behind a proxy, social engineering, Loader-V4, and geyser / scheduler-bindings external processes)  
- Anything previously disclosed in public; the known-issues tracker (section 8) is canonical  
- Anything fixed before the window or scoped out under the known-issues process (section 8)

---

## 4. Severity

The competition uses the standing Agave bounty categories, in order of severity. Validity and severity are assessed against Alpenglow's security model as specified in the [Alpenglow whitepaper](https://www.anza.xyz/alpenglow-1-1) and [SIMD-0326](https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md), including its byzantine and offline stake assumptions and its safety and liveness guarantees. A safety violation (two conflicting finalized blocks) is a Consensus / Safety Violation on its own, and counts as Loss of Funds only when the fund impact is demonstrated, such as a double-spend enabled by the conflicting finalizations.

- **Loss of Funds.** Theft or unauthorized movement of funds. In Alpenglow, most often a safety violation that enables a double-spend, or a reward/credit bug that misallocates stake or rewards.  
- **Consensus / Safety Violation.** A consensus safety break with no demonstrated fund impact: two conflicting blocks both notarized or finalized, a certificate accepted below the required stake threshold, contradictory finality, or acceptance of equivocating votes that should be rejected.  
- **Liveness / Loss of Availability.** Consensus halts and needs human intervention to recover: a certificate that can never form for an honest leader, a permanent skip-vote cascade or deadlock, a network partition, or an eclipse.  
- **DoS.** Remote resource exhaustion or degradation via non-RPC protocols that does not halt consensus, including recoverable stalls and a block delayed well beyond the target slot time.  
- **Other.** A finding with a reproduced, demonstrated impact (section 6) that falls outside the categories above but is genuinely worth fixing. It cannot be self-assigned; self-selecting Other will almost certainly disqualify a report.

---

## 5. Rewards

There is a single prize pool of up to 50,000 SOL. It unlocks with the most severe valid finding and caps the total paid out. Each finding's award goes in full to its earliest report that substantiates the issue at the severity Anza assesses it to be; duplicates are ineligible (section 7).

**The pool unlocks by severity.** The highest-severity valid finding in the competition sets the pool. Multiple findings at the same level do not raise it.

| Highest valid finding | Prize pool |
| :---- | :---- |
| DoS or Other only | 10,000 SOL |
| Liveness / Loss of Availability | 20,000 SOL |
| Consensus / Safety Violation | 30,000 SOL |
| Loss of Funds | 50,000 SOL |

**Awards, capped by the pool.** Anza assigns each valid finding an award from its category range, based on assessed impact and proof-of-concept quality:

| Category | Award (SOL) |
| :---- | :---- |
| Loss of Funds | 6,250 to 25,000 |
| Consensus / Safety Violation | 3,125 to 12,500 |
| Liveness / Loss of Availability | 1,250 to 5,000 |
| DoS | 315 to 1,250 |
| Other | discretionary |

Findings are paid those awards, up to the unlocked pool. If the awards together exceed the pool, they are all reduced pro-rata to fit. The pool is a ceiling, not a floor: a light field pays out less, and no award exceeds its category range.

Payouts are lump-sum after the adjudication close and after KYC, in 12-month locked SOL, following the standing bug bounty's payment terms.

---

## 6. Eligibility

- A finding must be present in the cited in-window `master` commit and still unfixed on `master` when submitted; it is reproduced at that commit. Once an issue is fixed it is ineligible.  
- It must not be public or in the known-issues tracker (section 8) as of submission.  
- One finding per advisory; advisories bundling multiple findings are closed as invalid and hold no duplicate-report standing.  
- Report through the submission portal at https://alpenglow.anza.xyz/. Submissions received through any other channel are ineligible, including advisories opened directly on `anza-xyz/alpenglow`; do not open a public GitHub issue to report a finding.  
- Any attempt to cheat the submission system or the competition process leads to disqualification.  
- Each advisory is self-contained: a clear title, a detailed description, reproduction steps, and the proof of concept, all inline. No attachments and no external file links; exploitation detail goes only in the advisory.  
- Enable two-factor authentication on your GitHub account.  
- Except where these rules override it, the standing `agave/SECURITY.md` and the participation agreement govern reporting, conduct, and eligibility, including the standard conflict-of-interest and privileged-access exclusions.  
- A proof of concept is required for all severities, demonstrated on a local fork, multi-node harness, or simulation, never on mainnet or a public testnet. The proof of concept must demonstrate the claimed impact at the cited commit, with the mechanism, adversary model, and steps. Submissions without a reproducing proof of concept are closed as speculative. Where a local reproduction of a safety finding is genuinely infeasible, Anza may accept a rigorous argument-only submission with Foundation sign-off.  
- No registration. Submitting a report constitutes acceptance of these rules. KYC and the participation agreement are completed before payout, not before submitting.  
- Code is in scope as soon as it lands on `master`; the standing one-week-on-master eligibility rule is waived for the competition, so fresh code and fixes are reviewable immediately.  
- Residents of OFAC-sanctioned jurisdictions are not eligible for payout.

---

## 7. Duplicates

This section overrides the Duplicate Reports policy in the standing `agave/SECURITY.md`.

Duplicates are not valid: for a given issue the whole reward goes to one report, and later reports of the same issue are ineligible.

- **Assessed severity governs, not claimed severity.** The full reward goes to the earliest report that substantiates the issue at the severity Anza assesses it to be; a later report that substantiates a strictly higher assessed severity takes precedence. Settled over the valid reports received before the fix lands; once fixed, the issue is ineligible to new reports (section 6). All other reports of the same issue are unpaid.  
- **Report at the severity you can substantiate.** Severity is what Anza assesses, not what a report claims. An inflated claim is treated at its assessed level, so it earns no higher payout and no priority over other reports at that level.  
- A report must meet the proof-of-concept bar (section 6) at submission to hold priority. A placeholder does not reserve an issue.  
- **Duplicate:** the same root cause, where a single fix closes both reports. The later report is ineligible.  
- Anza may combine related findings of the same class or root cause into a single reward, as reserved in the standing program.

---

## 8. Known issues

Anything already public is out of scope: existing public `agave` GitHub issues, prior advisories, and anything previously disclosed in a public venue (a SIMD, a public GitHub issue, PR, or fork, a public Discord or forum thread, a prior advisory, or published research). In addition, a live, timestamped tracker in agave (issues labeled `blocking-ag` and `consensus-team`) lists known issues. A submission is out of scope if a matching public issue or tracker entry predates the submission; an entry that first appears after a report is submitted does not affect that report. The tracker includes existing public issues and advisories, the external audit findings, prior internal findings recorded as public issues, by-design and accepted-risk items, and any confirmed-but-unpatched issues that have been scoped out.

Known-but-unpatched issues are handled by fixing them before the window or by scoping the affected path out of section 3. There is no partial-payout tier for known issues.

---

## 9. Adjudication

- Anza determines validity, severity, and duplication, and is the researcher-facing contact.  
- The Solana Foundation makes the final decision, including on scope, policy, and budget. Where the Foundation departs from Anza's determination, that is documented.  
- Service levels: a first response within 72 hours, an initial severity determination within 7 days of submission, and final adjudication of every valid submission by the adjudication close (section 1).  
- Payouts are lump-sum after the adjudication close.

---

## 10. Disclosure

- Findings are confidential until a fix ships.  
- For code not yet activated on mainnet, the embargo holds until the fix is merged and, where applicable, the feature gate activates.  
- The known-issues baseline (section 8) is already public and is not under embargo.  
- A researcher may publish their own write-up only after the fix ships and the embargo lifts.

---

## 11. Legal and safe harbor

Participation is governed by the competition participation agreement, accepted by submitting a report. In summary:

- **Safe harbor.** Anza and the Solana Foundation will not pursue civil claims against good-faith researchers acting within scope and these rules. No safe harbor can waive criminal prosecution by a government authority.  
- **Scope of activity.** The competition is find-and-report, demonstrated on local forks. Attacking mainnet or other live infrastructure is not authorized.  
- **Confidentiality and IP.** The disclosure embargo (section 10) is binding; findings and fixes are licensed or assigned to Anza and the Foundation.  
- **Representations.** Reporters represent that they are not sanctioned, are of age and legal capacity, submit original work, and have no motive beyond the reward.  
- **KYC and sanctions.** KYC, AML, and sanctions screening are completed at payout.  
- **Finality.** Program decisions are final at the Solana Foundation’s discretion.

