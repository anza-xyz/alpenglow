#![allow(clippy::arithmetic_side_effects)]

use {
    criterion::{criterion_group, criterion_main, Bencher, Criterion},
    crossbeam_channel::{unbounded, Receiver, Sender},
    solana_bls_signatures::signature::Signature as BlsSignature,
    solana_core::{sigverifier::bls_sigverifier::BLSSigVerifier, sigverify_stage::SigVerifyStage},
    solana_perf::packet::{Packet, PacketBatch, PinnedPacketBatch},
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::{
            create_genesis_config_with_alpenglow_vote_accounts_no_program, ValidatorVoteKeypairs,
        },
    },
    solana_votor_messages::{
        consensus_message::{ConsensusMessage, VoteMessage},
        vote::Vote,
    },
    std::time::{Duration, Instant},
};

struct BenchSetup {
    stage: SigVerifyStage,
    packet_sender: Sender<PacketBatch>,
    consensus_message_receiver: Receiver<ConsensusMessage>,
    validator_keypairs: Vec<ValidatorVoteKeypairs>,
}

fn setup_bls_sigverify_stage() -> BenchSetup {
    const NUM_VALIDATORS: usize = 100;

    let (packet_s, packet_r) = unbounded();
    let (consensus_msg_s, consensus_msg_r) = unbounded();
    let (verified_votes_s, _verified_votes_r) = unbounded();

    let validator_keypairs: Vec<_> = (0..NUM_VALIDATORS)
        .map(|_| ValidatorVoteKeypairs::new_rand())
        .collect();
    let stakes_vec = vec![1_000; validator_keypairs.len()];
    let genesis = create_genesis_config_with_alpenglow_vote_accounts_no_program(
        1_000_000_000,
        &validator_keypairs,
        stakes_vec,
    );

    let bank0 = Bank::new_for_tests(&genesis.genesis_config);
    let bank_forks = BankForks::new_rw_arc(bank0);
    let root_bank = bank_forks.read().unwrap().sharable_root_bank();

    let verifier = BLSSigVerifier::new(root_bank, verified_votes_s, consensus_msg_s);
    let stage = SigVerifyStage::new(packet_r, verifier, "solBlsSigVerBench", "bench");

    BenchSetup {
        stage,
        packet_sender: packet_s,
        consensus_message_receiver: consensus_msg_r,
        validator_keypairs,
    }
}

fn gen_vote_batches(
    validator_keypairs: &[ValidatorVoteKeypairs],
    num_packets: usize,
    all_valid: bool,
) -> Vec<PacketBatch> {
    let vote = Vote::new_skip_vote(42);
    let valid_vote_payload = bincode::serialize(&vote).expect("Failed to serialize vote");
    let invalid_vote_payload =
        bincode::serialize(&Vote::new_skip_vote(99)).expect("Failed to serialize invalid vote");

    let packets: Vec<_> = (0..num_packets)
        .map(|i| {
            let rank = (i % validator_keypairs.len()) as u16;
            let validator_keypair = &validator_keypairs[rank as usize];
            let bls_keypair = &validator_keypair.bls_keypair;

            let is_invalid = !all_valid && i == 1;
            let signature: BlsSignature = if is_invalid {
                bls_keypair.sign(&invalid_vote_payload).into()
            } else {
                bls_keypair.sign(&valid_vote_payload).into()
            };

            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });

            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packet
        })
        .collect();

    packets
        .chunks(192)
        .map(|chunk| PinnedPacketBatch::new(chunk.to_vec()).into())
        .collect()
}

fn bench_bls_sigverify_stage(b: &mut Bencher, all_valid: bool) {
    let setup = setup_bls_sigverify_stage();
    let BenchSetup {
        stage,
        packet_sender,
        consensus_message_receiver,
        validator_keypairs,
    } = setup;

    const NUM_PACKETS: usize = 4096;

    b.iter(move || {
        let batches = gen_vote_batches(&validator_keypairs, NUM_PACKETS, all_valid);
        let mut sent_len = 0;
        for batch in batches {
            sent_len += batch.len();
            packet_sender.send(batch).unwrap();
        }

        let expected_len = if all_valid { sent_len } else { sent_len - 1 };
        let mut received_len = 0;
        let start = Instant::now();
        loop {
            if consensus_message_receiver.try_recv().is_ok() {
                received_len += 1;
                if received_len >= expected_len {
                    break;
                }
            }
            if start.elapsed() > Duration::from_secs(10) {
                panic!("benchmark timed out");
            }
        }
    });

    stage.join().unwrap();
}

fn bench_bls(c: &mut Criterion) {
    solana_logger::setup();
    c.bench_function("bls_sigverify_stage_all_valid", |b| {
        bench_bls_sigverify_stage(b, true);
    });
    c.bench_function("bls_sigverify_stage_one_invalid", |b| {
        bench_bls_sigverify_stage(b, false);
    });
}

criterion_group!(benches, bench_bls);
criterion_main!(benches);
