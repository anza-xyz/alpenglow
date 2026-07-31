# Alpenglow Bug Bounty Competition

Alpenglow is Solana's new consensus protocol. During development,
monorepo migration and internal audit phases, the Alpenglow logic
has been excluded from scope of the Agave bug bounty program. To
mark its introduction to eligibility, we're hosting a bug bounty
competition to raise awareness and catch standing issues that have
evaded prior review efforts

- **Prize pool:** up to 50,000 SOL
- **Submission window:** 2026-08-05 16:00 UTC to 2026-08-19 16:00 UTC
- **How to submit:** open a [GitHub Security Advisory](https://github.com/anza-xyz/alpenglow/security/advisories/new)
  on this repository, one finding per advisory
- **Full rules:** [RULES.md](RULES.md) (scope, severity categories,
  rewards, eligibility, and duplicate policy)

Do not disclose a finding publicly (for example as a GitHub issue
here or on `agave`), as public findings are ineligible for a reward.
Findings submitted outside the window are handled under the standing
[Agave security policy](https://github.com/anza-xyz/agave/blob/master/SECURITY.md)

## Start here

The Alpenglow consensus code subject to the competition is hosted in Anza's Agave GitHub repository
[`anza-xyz/agave`](https://github.com/anza-xyz/agave). Begin with:

- [`votor`](https://github.com/anza-xyz/agave/tree/master/votor): the voting engine
- [`votor-messages`](https://github.com/anza-xyz/agave/tree/master/votor-messages): vote and certificate types
- [`bls-sigverify`](https://github.com/anza-xyz/agave/tree/master/bls-sigverify): BLS signature verification
- [`bls-cert-verify`](https://github.com/anza-xyz/agave/tree/master/bls-cert-verify): certificate verification and stake-threshold checks

These four crates are the core, but the scope extends to the
Alpenglow integration surface across the validator; see
[RULES.md section 3](RULES.md#3-scope) for the full list.

Background: the [Alpenglow whitepaper](https://www.anza.xyz/alpenglow-1-1) and [SIMD-0326](https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md).

To recap, the code subject to the competition resides in the _Agave
repository_, while competition submissions will be made to _this
repository_

## Known issues

The tracker below lists issues found during Alpenglow's development
and review. They can point you to areas worth investigating, but
they are also the known-issues baseline: anything already listed
there (or otherwise public) at the time you submit is out of scope
([RULES.md section 8](RULES.md#8-known-issues)):

[Alpenglow related issues on Agave](https://github.com/anza-xyz/agave/issues?q=is%3Aissue+label%3Aconsensus-team%2Cblocking-ag)

## Get notified

Follow [@anza_xyz](https://x.com/anza_xyz) on X and **Watch** this repository.
Further competition details will be announced in both places.
