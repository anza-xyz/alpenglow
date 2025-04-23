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

fn add_vote_bench(validator_keypairs: &[ValidatorVoteKeypairs], vote: Vote, pool: &mut CertificatePool) {
    for keypair in validator_keypairs {
        let _ = pool.add_vote(
            &vote,
            VersionedTransaction::default(),
            &keypair.vote_keypair.pubkey(),
        );
    }
}

pub const NUM_VALIDATORS: usize = 2000;

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
            add_vote_bench(&validator_keypairs, Vote::new_notarization_vote(2, Hash::new_unique(), Hash::new_unique()), black_box(&mut pool));
        })
    });

    c.bench_function("add_vote_skip", |b| {
        b.iter(|| {
            add_vote_bench(&validator_keypairs, Vote::new_skip_vote(3), black_box(&mut pool));
        })
    });
}

criterion_group!(benches, certificate_pool_add_vote_benchmark);
criterion_main!(benches);
