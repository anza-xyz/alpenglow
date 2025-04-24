use {
    alpenglow_vote::vote::Vote,
    criterion::{black_box, criterion_group, criterion_main, Criterion},
    solana_core::alpenglow_consensus::certificate_pool::CertificatePool,
    solana_runtime::{
        bank::Bank,
        genesis_utils::{create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs},
    },
    solana_sdk::{hash::Hash, signer::Signer, transaction::VersionedTransaction},
};

pub const NUM_VALIDATORS: usize = 2000;
pub const NUM_SLOTS: u64 = 96;

fn add_vote_bench<F>(
    validator_keypairs: &[ValidatorVoteKeypairs],
    mut vote_fn: F,
    pool: &mut CertificatePool,
) where
    F: FnMut(u64) -> Vote,
{
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
        if pool.get_notarization_cert_size(slot).is_none() && !pool.skip_certified(slot) {
            panic!(
                "Failed to notarize or skip slot {} {} {:?} {}",
                slot,
                has_error,
                pool.get_notarization_cert_size(slot),
                pool.skip_certified(slot)
            );
        }
    }
}

pub fn certificate_pool_add_vote_benchmark(c: &mut Criterion) {
    let validator_keypairs = (0..NUM_VALIDATORS)
        .map(|_| ValidatorVoteKeypairs::new_rand())
        .collect::<Vec<_>>();
    let genesis = create_genesis_config_with_vote_accounts(
        1_000_000_000,
        &validator_keypairs,
        vec![100; NUM_VALIDATORS],
    );
    let bank = Bank::new_for_tests(&genesis.genesis_config);
    let mut pool = CertificatePool::new_from_root_bank(&bank);
    c.bench_function("add_vote_notarize", |b| {
        b.iter(|| {
            add_vote_bench(
                &validator_keypairs,
                |slot| Vote::new_notarization_vote(slot, Hash::new_unique(), Hash::new_unique()),
                black_box(&mut pool),
            );
        })
    });

    let mut pool = CertificatePool::new_from_root_bank(&bank);
    c.bench_function("add_vote_skip", |b| {
        b.iter(|| {
            add_vote_bench(
                &validator_keypairs,
                Vote::new_skip_vote,
                black_box(&mut pool),
            );
        })
    });
}

criterion_group!(benches, certificate_pool_add_vote_benchmark);
criterion_main!(benches);
