# Alpenglow Bug Bounty Competition

Alpenglow is Solana's new consensus protocol. During development,
monorepo migration and internal audit phases, the Alpenglow logic
has been excluded from scope of the Agave bug bounty program. To
mark its introduction to eligibility, we're hosting a bug bounty
competition to raise awareness and catch standing issues that have
evaded prior review efforts

The competition will be hosted in this repository, through the
private vulnerability reporting feature

**Full scope, rules, dates, and rewards are coming soon**

Submissions are not open yet! Findings submitted before the competition
commences will be treated as out-of-scope/informational as per today's
security policy. You are of course free to start familiarizing yourself
with the code at your leisure

## Start here

The Alpenglow consensus code subject to the competition will be that hosted in the Anza's Agave Github repository
[`anza-xyz/agave`](https://github.com/anza-xyz/agave). Begin with:

- [`votor`](https://github.com/anza-xyz/agave/tree/master/votor): the voting engine
- [`votor-messages`](https://github.com/anza-xyz/agave/tree/master/votor-messages): vote and certificate types
- [`bls-sigverify`](https://github.com/anza-xyz/agave/tree/master/bls-sigverify): BLS signature verification
- [`bls-cert-verify`](https://github.com/anza-xyz/agave/tree/master/bls-cert-verify): certificate verification and stake-threshold checks

Background: the [Alpenglow whitepaper](https://www.anza.xyz/alpenglow-1-1) and [SIMD-0326](https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md).

To recap, the code subject to the competition resides in the _Agave
repository_, while competition submissions will be made to _this
repository_

## What we've already found

Alpenglow has been under active review throughout its development. The consensus issues we've found and fixed are public on Agave, and they're the best sense of the target: the kind of safety, liveness, and certificate-handling bugs that matter here.

Browse them all: [Alpenglow consensus issues on Agave](https://github.com/anza-xyz/agave/issues?q=is%3Aissue+label%3Ablocking-ag+label%3Aconsensus-team)

These are already fixed, so they won't be eligible once the program opens. Aim at what's still live on Agave.

## Get notified

Follow [@anza_xyz](https://x.com/anza_xyz) on X and **Watch** this repository.
Further competition details will be announced in both places.
