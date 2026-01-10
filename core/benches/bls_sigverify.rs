#![allow(clippy::arithmetic_side_effects)]

use {
    criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput},
    crossbeam_channel::unbounded,
    solana_bls_signatures::signature::Signature as BlsSignature,
    solana_core::bls_sigverify::bls_sigverifier::BLSSigVerifier,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_perf::packet::{Packet, PacketBatch, PinnedPacketBatch},
    solana_pubkey::Pubkey,
    solana_rpc::alpenglow_last_voted::AlpenglowLastVoted,
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::{
            create_genesis_config_with_alpenglow_vote_accounts, ValidatorVoteKeypairs,
        },
    },
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_votor::consensus_pool::certificate_builder::CertificateBuilder,
    solana_votor_messages::{
        consensus_message::{CertificateType, ConsensusMessage, VoteMessage},
        vote::Vote,
    },
    std::{
        cell::RefCell,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
};

const BENCH_SLOT: u64 = 70;
// TODO(sam): use a small number for now to emulate the current test cluster
const NUM_VALIDATORS: usize = 50;

// We want enough unique inputs (due to Hash::new_unique() in generators) to
// saturate the internal caches (vote_payload_cache, verified_certs) during the
// warm-up phase, while keeping pre-generation time and memory usage reasonable.
const NUM_PREGENERATED_BATCHES: usize = 500;

struct BenchEnvironment {
    verifier: RefCell<BLSSigVerifier>,
    validator_keypairs: Arc<Vec<ValidatorVoteKeypairs>>,
}

struct PregeneratedBatches {
    batches: Vec<Vec<PacketBatch>>,
    counter: AtomicUsize,
}

impl PregeneratedBatches {
    fn new<F>(env: &BenchEnvironment, generator: F) -> Self
    where
        F: Fn(&BenchEnvironment) -> Vec<PacketBatch>,
    {
        // Generate the batches upfront, moving expensive crypto ops out of the benchmark loop
        //
        // NOTE: Because the root bank slot (which controls cache eviction) is static during the
        // benchmark, the caches do not get cleared. Therefore, this benchmark measures the *steady-state*
        // throughput with high cache hit rates. It intentionally excludes the overhead
        // of serialization (which occurs on cache misses).
        let batches = (0..NUM_PREGENERATED_BATCHES)
            .map(|_| generator(env))
            .collect();
        Self {
            batches,
            counter: AtomicUsize::new(0),
        }
    }

    // Get the next batch in a round-robin fashion.
    // We must clone because the verifier modifies the packets in place (e.g., setting discard flags).
    fn next(&self) -> Vec<PacketBatch> {
        let index = self.counter.fetch_add(1, Ordering::Relaxed) % self.batches.len();
        self.batches[index].clone()
    }
}

fn setup_environment() -> BenchEnvironment {
    let (verified_votes_s, _) = unbounded();
    let (consensus_msg_s, _) = unbounded();
    let (consensus_metrics_sender, _) = unbounded();
    let (reward_votes_sender, _) = unbounded();

    let validator_keypairs: Arc<Vec<_>> = Arc::new(
        (0..NUM_VALIDATORS)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect(),
    );

    let stakes_vec: Vec<_> = (0..NUM_VALIDATORS).map(|i| (10000 - i) as u64).collect();
    let genesis = create_genesis_config_with_alpenglow_vote_accounts(
        1_000_000_000,
        &validator_keypairs,
        stakes_vec,
    );

    let bank0 = Bank::new_for_tests(&genesis.genesis_config);
    // Ensure the bank slot is high enough so votes are not considered ancient.
    let root_bank = Bank::new_from_parent(Arc::new(bank0), &Pubkey::default(), BENCH_SLOT - 1);
    let bank_forks = BankForks::new_rw_arc(root_bank);
    let sharable_banks = bank_forks.read().unwrap().sharable_banks();
    let keypair = Keypair::new();
    let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
    let cluster_info = Arc::new(ClusterInfo::new(
        contact_info,
        Arc::new(keypair),
        SocketAddrSpace::Unspecified,
    ));
    let leader_schedule = Arc::new(LeaderScheduleCache::new_from_bank(&sharable_banks.root()));
    let verifier = BLSSigVerifier::new(
        sharable_banks,
        verified_votes_s,
        reward_votes_sender,
        consensus_msg_s,
        consensus_metrics_sender,
        Arc::new(AlpenglowLastVoted::default()),
        cluster_info,
        leader_schedule,
    );

    BenchEnvironment {
        verifier: RefCell::new(verifier),
        validator_keypairs,
    }
}

fn message_to_packet(msg: &ConsensusMessage) -> Packet {
    let mut packet = Packet::default();
    packet.populate_packet(None, msg).unwrap();
    packet
}

fn create_base2_cert_message(env: &BenchEnvironment, slot: u64, hash: Hash) -> ConsensusMessage {
    let num_signers = (NUM_VALIDATORS * 67) / 100; // 67% quorum
    let cert_type = CertificateType::Notarize(slot, hash);
    let original_vote = cert_type.to_source_vote();
    let payload = bincode::serialize(&original_vote).unwrap();

    let vote_messages: Vec<VoteMessage> = (0..num_signers)
        .map(|i| {
            let signature = env.validator_keypairs[i].bls_keypair.sign(&payload);
            VoteMessage {
                vote: original_vote,
                signature: signature.into(),
                rank: i as u16,
            }
        })
        .collect();

    let mut builder = CertificateBuilder::new(cert_type);
    builder.aggregate(&vote_messages).unwrap();
    let cert = builder.build().unwrap();
    ConsensusMessage::Certificate(cert)
}

fn create_base3_cert_message(env: &BenchEnvironment, slot: u64, hash: Hash) -> ConsensusMessage {
    let cert_type = CertificateType::NotarizeFallback(slot, hash);

    let vote1 = Vote::new_notarization_vote(slot, hash);
    let payload1 = bincode::serialize(&vote1).unwrap();
    let vote2 = Vote::new_notarization_fallback_vote(slot, hash);
    let payload2 = bincode::serialize(&vote2).unwrap();

    let mut all_vote_messages = Vec::new();

    // Define a split quorum: e.g., 40% sign Vote 1, 30% sign Vote 2 (Total 70%)
    let split1 = (NUM_VALIDATORS * 40) / 100;
    let split2 = (NUM_VALIDATORS * 70) / 100;

    // Signers for Vote 1
    for i in 0..split1 {
        let signature = env.validator_keypairs[i].bls_keypair.sign(&payload1);
        all_vote_messages.push(VoteMessage {
            vote: vote1,
            signature: signature.into(),
            rank: i as u16,
        });
    }
    // Signers for Vote 2
    for i in split1..split2 {
        let signature = env.validator_keypairs[i].bls_keypair.sign(&payload2);
        all_vote_messages.push(VoteMessage {
            vote: vote2,
            signature: signature.into(),
            rank: i as u16,
        });
    }

    let mut builder = CertificateBuilder::new(cert_type);
    builder.aggregate(&all_vote_messages).unwrap();
    let cert = builder.build().unwrap();
    ConsensusMessage::Certificate(cert)
}

// Scenario 1: One batch with two votes.
fn generate_two_votes_batch(env: &BenchEnvironment) -> Vec<PacketBatch> {
    // Use Notarization votes as in the original hardcoded data structure.
    let vote = Vote::new_notarization_vote(BENCH_SLOT, Hash::new_unique());
    let payload = bincode::serialize(&vote).unwrap();

    // Vote 1 (Signed by Rank 0)
    let kp1 = &env.validator_keypairs[0].bls_keypair;
    let sig1: BlsSignature = kp1.sign(&payload).into();
    let msg1 = ConsensusMessage::Vote(VoteMessage {
        vote,
        signature: sig1,
        rank: 0,
    });

    // Vote 2 (Signed by Rank 1)
    let kp2 = &env.validator_keypairs[1].bls_keypair;
    let sig2: BlsSignature = kp2.sign(&payload).into();
    let msg2 = ConsensusMessage::Vote(VoteMessage {
        vote,
        signature: sig2,
        rank: 1,
    });

    let packets = vec![message_to_packet(&msg1), message_to_packet(&msg2)];
    vec![PinnedPacketBatch::new(packets).into()]
}

// Scenario 2: One batch with a single vote.
fn generate_single_vote_batch(env: &BenchEnvironment) -> Vec<PacketBatch> {
    // Use a Finalization vote as in the original hardcoded data structure.
    let vote = Vote::new_finalization_vote(BENCH_SLOT);
    let payload = bincode::serialize(&vote).unwrap();

    // Vote 1 (Signed by Rank 0)
    let kp = &env.validator_keypairs[0].bls_keypair;
    let sig: BlsSignature = kp.sign(&payload).into();
    let msg = ConsensusMessage::Vote(VoteMessage {
        vote,
        signature: sig,
        rank: 0,
    });

    let packets = vec![message_to_packet(&msg)];
    vec![PinnedPacketBatch::new(packets).into()]
}

// Scenario 3: A batch with a single certificate.
fn generate_single_cert_batch(env: &BenchEnvironment) -> Vec<PacketBatch> {
    let hash = Hash::new_unique();
    // Generate a Base2 certificate
    let msg = create_base2_cert_message(env, BENCH_SLOT, hash);
    let packets = vec![message_to_packet(&msg)];
    vec![PinnedPacketBatch::new(packets).into()]
}

// Scenario 4: A batch with two certificates (one Base2, one Base3).
fn generate_two_certs_batch(env: &BenchEnvironment) -> Vec<PacketBatch> {
    let hash1 = Hash::new_unique();
    let hash2 = Hash::new_unique();

    // Cert 1 (Base2 - Notarize)
    let msg1 = create_base2_cert_message(env, BENCH_SLOT, hash1);
    // Cert 2 (Base3 - NotarizeFallback)
    let msg2 = create_base3_cert_message(env, BENCH_SLOT + 1, hash2);

    let packets = vec![message_to_packet(&msg1), message_to_packet(&msg2)];
    vec![PinnedPacketBatch::new(packets).into()]
}

fn bench_votes(c: &mut Criterion) {
    agave_logger::setup();
    let env = setup_environment();
    let mut group = c.benchmark_group("verify_votes");

    let two_votes_batches = Arc::new(PregeneratedBatches::new(&env, generate_two_votes_batch));
    let single_vote_batches = Arc::new(PregeneratedBatches::new(&env, generate_single_vote_batch));

    // Benchmark Scenario 1: Two votes in one batch
    // (about 20% of the non-zero votes in the test-cluster consist of a single vote)
    group.throughput(Throughput::Elements(2));
    let two_votes_batches_clone = two_votes_batches.clone();
    group.bench_function("dynamic/two_votes_batch", |b| {
        b.iter_batched(
            || two_votes_batches_clone.next(),
            |batches| env.verifier.borrow_mut().verify_and_send_batches(batches),
            BatchSize::SmallInput,
        );
    });

    // Benchmark Scenario 2: Single vote in one batch
    // (about 80% of the non-zero votes in the test-cluster consist of a single vote)
    group.throughput(Throughput::Elements(1));
    let single_vote_batches_clone = single_vote_batches.clone();
    group.bench_function("dynamic/single_vote_batch", |b| {
        b.iter_batched(
            || single_vote_batches_clone.next(),
            |batches| env.verifier.borrow_mut().verify_and_send_batches(batches),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_certificates(c: &mut Criterion) {
    agave_logger::setup();
    let env = setup_environment();
    let mut group = c.benchmark_group("verify_certificates");

    let single_cert_batches = Arc::new(PregeneratedBatches::new(&env, generate_single_cert_batch));
    let two_certs_batches = Arc::new(PregeneratedBatches::new(&env, generate_two_certs_batch));

    // Benchmark Scenario 3: Single certificate batch (Base2)
    group.throughput(Throughput::Elements(1));
    let single_cert_batches_clone = single_cert_batches.clone();
    group.bench_function("dynamic/single_cert_batch_base2", |b| {
        b.iter_batched(
            || single_cert_batches_clone.next(),
            |batches| env.verifier.borrow_mut().verify_and_send_batches(batches),
            BatchSize::SmallInput,
        );
    });

    // Benchmark Scenario 4: Two certificates batch (Base2 + Base3)
    group.throughput(Throughput::Elements(2));
    let two_certs_batches_clone = two_certs_batches.clone();
    group.bench_function("dynamic/two_certs_batch_base2_base3", |b| {
        b.iter_batched(
            || two_certs_batches_clone.next(),
            |batches| env.verifier.borrow_mut().verify_and_send_batches(batches),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_votes, bench_certificates);
criterion_main!(benches);
