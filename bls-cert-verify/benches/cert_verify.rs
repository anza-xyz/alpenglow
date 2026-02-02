use agave_bls_cert_verify::cert_verify::verify_cert_get_total_stake;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use solana_bls_signatures::{
    keypair::Keypair as BlsKeypair, pubkey::Pubkey as BlsPubkey,
    signature::Signature as BlsSignature,
};
use solana_hash::Hash;
use solana_votor::consensus_pool::certificate_builder::CertificateBuilder;
use solana_votor_messages::{
    consensus_message::{Certificate, CertificateType, VoteMessage},
    vote::Vote,
};

// Creates random BLS keypairs for bench tests
fn create_bls_keypairs(num_signers: usize) -> Vec<BlsKeypair> {
    (0..num_signers).map(|_| BlsKeypair::new()).collect()
}

// Creates vote messages for bench tests
fn create_signed_vote_message(bls_keypair: &BlsKeypair, vote: Vote, rank: usize) -> VoteMessage {
    let payload = bincode::serialize(&vote).expect("Failed to serialize vote");
    let signature: BlsSignature = bls_keypair.sign(&payload).into();
    VoteMessage {
        vote,
        signature,
        rank: rank as u16,
    }
}

// Creates a standard Base2 Certificate (All validators sign the same vote)
fn create_base2_cert(keypairs: &[BlsKeypair], num_signers: usize) -> Certificate {
    let slot = 100;
    let hash = Hash::new_unique();
    let cert_type = CertificateType::Notarize(slot, hash);
    let vote = cert_type.to_source_vote();

    let vote_messages: Vec<VoteMessage> = (0..num_signers)
        .map(|rank| create_signed_vote_message(&keypairs[rank], vote, rank))
        .collect();

    let mut builder = CertificateBuilder::new(cert_type);
    builder.aggregate(&vote_messages).unwrap();
    builder.build().unwrap()
}

// Creates a Split Vote Base3 Certificate (Validators split between Notarize and Fallback)
#[allow(clippy::arithmetic_side_effects)]
fn create_base3_cert(
    keypairs: &[BlsKeypair],
    num_notarize: usize,
    num_fallback: usize,
) -> Certificate {
    let slot = 100;
    let hash = Hash::new_unique();
    let cert_type = CertificateType::NotarizeFallback(slot, hash);

    let vote_notarize = Vote::new_notarization_vote(slot, hash);
    let vote_fallback = Vote::new_notarization_fallback_vote(slot, hash);

    let mut vote_messages = Vec::new();

    // Group 1: Signs Notarize
    for (i, keypair) in keypairs.iter().take(num_notarize).enumerate() {
        let rank = i;
        vote_messages.push(create_signed_vote_message(keypair, vote_notarize, rank));
    }

    // Group 2: Signs Fallback
    for (i, keypair) in keypairs
        .iter()
        .skip(num_notarize)
        .take(num_fallback)
        .enumerate()
    {
        let rank = num_notarize + i;
        vote_messages.push(create_signed_vote_message(keypair, vote_fallback, rank));
    }

    let mut builder = CertificateBuilder::new(cert_type);
    builder.aggregate(&vote_messages).unwrap();
    builder.build().unwrap()
}

#[allow(clippy::arithmetic_side_effects)]
fn bench_verify_cert(c: &mut Criterion) {
    let mut group = c.benchmark_group("BLS Cert Verify");

    let validator_sizes = [500, 1000, 1500, 2000];
    const TEST_STAKE: u64 = 30; // assume each validator has stake 30 (arbitrary number)

    for &size in &validator_sizes {
        let keypairs = create_bls_keypairs(size);

        // Pre-calculate public keys to simulate efficient Bank lookup
        let pubkeys: Vec<BlsPubkey> = keypairs.iter().map(|kp| kp.public.into()).collect();
        let pubkeys_ref = &pubkeys;

        // Base2 Setup
        // Assume 2/3rds of validataors sign
        let num_signers_base2 = (size * 2) / 3;
        let cert_base2 = create_base2_cert(&keypairs, num_signers_base2);

        group.bench_with_input(
            BenchmarkId::new("Base2_Notarize", size),
            &size,
            |b, &total_validators| {
                b.iter(|| {
                    // The rank_map closure simulates the Bank lookup.
                    // It adds stake (we use 1000 per validator) and returns the pubkey.
                    let _stake =
                        verify_cert_get_total_stake(&cert_base2, total_validators, |rank| {
                            pubkeys_ref
                                .get(rank)
                                .map(|bls_pubkey| (TEST_STAKE, *bls_pubkey))
                        })
                        .unwrap();
                })
            },
        );

        // Base3 Setup: Split vote
        // 40% sign Notarize, 30% sign Fallback (Total 70%)
        let num_notarize = (size * 40) / 100;
        let num_fallback = (size * 30) / 100;
        let cert_base3 = create_base3_cert(&keypairs, num_notarize, num_fallback);

        group.bench_with_input(
            BenchmarkId::new("Base3_NotarizeFallback", size),
            &size,
            |b, &total_validators| {
                b.iter(|| {
                    let _stake =
                        verify_cert_get_total_stake(&cert_base3, total_validators, |rank| {
                            pubkeys_ref
                                .get(rank)
                                .map(|bls_pubkey| (TEST_STAKE, *bls_pubkey))
                        })
                        .unwrap();
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_verify_cert);
criterion_main!(benches);
