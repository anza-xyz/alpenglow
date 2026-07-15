# Alpenglow is coming into the bug bounty's scope

Alpenglow is Solana's new consensus protocol. We're soon expanding the Agave bug bounty to cover it, and this repository is where the program will run.

**Full scope, rules, dates, and rewards are coming soon.** Submissions are not open yet, but you can already start looking at the code.

## Start here

The Alpenglow consensus code under review lives in
[`anza-xyz/agave`](https://github.com/anza-xyz/agave). Begin with:

- [`votor`](https://github.com/anza-xyz/agave/tree/master/votor): the voting engine
- [`votor-messages`](https://github.com/anza-xyz/agave/tree/master/votor-messages): vote and certificate types
- [`bls-sigverify`](https://github.com/anza-xyz/agave/tree/master/bls-sigverify): BLS signature verification
- [`bls-cert-verify`](https://github.com/anza-xyz/agave/tree/master/bls-cert-verify): certificate verification and stake-threshold checks

Background: the [Alpenglow whitepaper](https://www.anza.xyz/alpenglow-1-1) and [SIMD-0326](https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md).

You will read and reproduce against Agave, and submit reports here once the program opens.

## What we've already found

Alpenglow has been under active review throughout its development. The consensus issues we've found and fixed are public on Agave, and they're the best sense of the target: the kind of safety, liveness, and certificate-handling bugs that matter here.

Browse them all: [Alpenglow consensus issues on Agave](https://github.com/anza-xyz/agave/issues?q=is%3Aissue+label%3Ablocking-ag+label%3Aconsensus-team)

These are already fixed, so they won't be eligible once the program opens. Aim at what's still live on Agave.

## Get notified

Follow [@anza_xyz](https://x.com/anza_xyz) on X and **Watch** this repository.
The launch will be announced in both places.
