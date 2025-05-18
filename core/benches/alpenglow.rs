#![allow(clippy::arithmetic_side_effects)]
#![feature(test)]
extern crate test;

use {
    alpenglow_vote::vote::Vote,
    solana_core::alpenglow_consensus::{
        certificate_pool::CertificatePool, vote_certificate::LegacyVoteCertificate,
    },
    solana_runtime::{
        bank::Bank,
        genesis_utils::{create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs},
    },
    solana_sdk::{hash::Hash, signer::Signer, transaction::VersionedTransaction},
    test::Bencher,
};

pub const NUM_VALIDATORS: usize = 2000;
pub const NUM_SLOTS: u64 = 96;

fn add_vote_bench(
    validator_keypairs: &[ValidatorVoteKeypairs],
    vote_fn: fn(u64) -> Vote,
    check_fn: fn(&CertificatePool<LegacyVoteCertificate>, u64) -> bool,
    pool: &mut CertificatePool<LegacyVoteCertificate>,
) {
    for slot in 0..NUM_SLOTS {
        let vote = vote_fn(slot);
        let mut has_error = false;
        for keypair in validator_keypairs {
            if pool
                .add_vote(
                    &vote,
                    VersionedTransaction::default(),
                    &keypair.vote_keypair.pubkey(),
                )
                .is_err()
            {
                has_error = true;
            }
        }
        if !check_fn(pool, slot) {
            panic!("Failed to verify cert for slot {} {}", slot, has_error,);
        }
    }
}

fn certificate_pool_add_vote_benchmark(
    b: &mut Bencher,
    vote_fn: fn(u64) -> Vote,
    check_fn: fn(&CertificatePool<LegacyVoteCertificate>, u64) -> bool,
) {
    let validator_keypairs = (0..NUM_VALIDATORS)
        .map(|_| ValidatorVoteKeypairs::new_rand())
        .collect::<Vec<_>>();
    let genesis = create_genesis_config_with_vote_accounts(
        1_000_000_000,
        &validator_keypairs,
        vec![100; NUM_VALIDATORS],
    );
    let bank = Bank::new_for_tests(&genesis.genesis_config);
    b.iter(|| {
        let mut pool = CertificatePool::<LegacyVoteCertificate>::new_from_root_bank(&bank, None);
        add_vote_bench(&validator_keypairs, vote_fn, check_fn, &mut pool);
    });
}

#[bench]
fn certificate_pool_add_vote_notarization_benchmark(b: &mut Bencher) {
    certificate_pool_add_vote_benchmark(
        b,
        |slot| Vote::new_notarization_vote(slot, Hash::default(), Hash::default()),
        |pool, slot| pool.is_notarized(slot, Hash::default(), Hash::default()),
    );
}

#[bench]
fn certificate_pool_add_vote_skip_benchmark(b: &mut Bencher) {
    certificate_pool_add_vote_benchmark(b, Vote::new_skip_vote, |pool, slot| {
        pool.skip_certified(slot)
    });
}

#[bench]
fn certificate_pool_add_vote_finalization_benchmark(b: &mut Bencher) {
    certificate_pool_add_vote_benchmark(b, Vote::new_finalization_vote, |pool, slot| {
        pool.is_finalized(slot)
    });
}
