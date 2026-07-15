<!--
DRAFT teaser README for anza-xyz/alpenglow (PR #1, pre-launch).
Scope-expansion framing only: no pot, dates, or competition mechanics until the
announcement. Swap to the launch README (ALPENGLOW_PORTAL_README_LAUNCH.md) when
submissions open.
-->

# Alpenglow, coming into scope

Alpenglow is Solana's new consensus protocol. We're soon expanding the Agave bug
bounty to cover it, and this repository is where it will run. Break consensus,
get paid.

**Full scope, rules, and rewards are coming soon.** Submissions are not open yet.

## Start here

The Alpenglow consensus code under review lives in
[`anza-xyz/agave`](https://github.com/anza-xyz/agave). Begin with:

- [`votor`](https://github.com/anza-xyz/agave/tree/master/votor): the voting engine
- [`votor-messages`](https://github.com/anza-xyz/agave/tree/master/votor-messages): vote and certificate types
- [`bls-sigverify`](https://github.com/anza-xyz/agave/tree/master/bls-sigverify): BLS signature verification
- [`bls-cert-verify`](https://github.com/anza-xyz/agave/tree/master/bls-cert-verify): certificate verification and stake-threshold checks

Background: [SIMD-0326](https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md).

You will read and reproduce against agave, and submit reports here once the
program opens.

## What we've already found

Alpenglow has been under active review throughout its development. The consensus
issues we've found and fixed are public on agave, and they're the best sense of
the target: the kind of safety, liveness, and certificate-handling bugs that
matter here.

Browse them all:
[Alpenglow consensus issues on agave](https://github.com/anza-xyz/agave/issues?q=is%3Aissue+label%3Ablocking-ag+label%3Aconsensus-team)

A few examples:

- [Standstill recovery emits ParentReady for the wrong slot, so the missed window never notarizes](https://github.com/anza-xyz/agave/issues/13699)
- [Conflicting reward-certificate votes decided on vote count instead of stake](https://github.com/anza-xyz/agave/issues/13235)
- [Startup replay freezes Alpenglow blocks without checking the footer bank hash](https://github.com/anza-xyz/agave/issues/13058)
- [Panic in the block ID repair path](https://github.com/anza-xyz/agave/issues/12668)
- [Malleable proof for FEC set size in double-Merkle repair](https://github.com/anza-xyz/agave/issues/12496)

These are already fixed, so they won't be eligible once the program opens. Aim
at what's still live on agave.

## Get notified

Click **Watch → Custom → Releases** on this repository for the launch.
