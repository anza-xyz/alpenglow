use {
    crate::{
        bls_sigverify::{
            bls_sigverifier::BLSSigVerifier, error::BLSSigVerifyError, stats::BLSPacketStats,
        },
        cluster_info_vote_listener::VerifiedVoteSender,
    },
    core::time::Duration,
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_measure::measure::Measure,
    solana_perf::{
        deduper::{self, Deduper},
        packet::PacketBatch,
    },
    solana_rpc::alpenglow_last_voted::AlpenglowLastVoted,
    solana_runtime::bank_forks::SharableBanks,
    solana_streamer::streamer::{self, StreamerError},
    solana_time_utils as timing,
    solana_votor::consensus_metrics::ConsensusMetricsEventSender,
    solana_votor_messages::{
        consensus_message::ConsensusMessage, reward_certificate::AddVoteMessage,
    },
    std::{
        sync::Arc,
        thread::{self, Builder, JoinHandle},
        time::Instant,
    },
};

pub struct BLSSigVerifyService {
    thread_hdl: JoinHandle<()>,
}

impl BLSSigVerifyService {
    pub fn new(
        packet_receiver: Receiver<PacketBatch>,
        sharable_banks: SharableBanks,
        verified_votes_sender: VerifiedVoteSender,
        reward_votes_sender: Sender<AddVoteMessage>,
        message_sender: Sender<ConsensusMessage>,
        consensus_metrics_sender: ConsensusMetricsEventSender,
        alpenglow_last_voted: Arc<AlpenglowLastVoted>,
        cluster_info: Arc<ClusterInfo>,
        leader_schedule: Arc<LeaderScheduleCache>,
    ) -> Self {
        let verifier = BLSSigVerifier::new(
            sharable_banks,
            verified_votes_sender,
            reward_votes_sender,
            message_sender,
            consensus_metrics_sender,
            alpenglow_last_voted,
            cluster_info,
            leader_schedule,
        );

        let thread_hdl = Builder::new()
            .name("solSigVerBLS".to_string())
            .spawn(move || Self::run(packet_receiver, verifier))
            .unwrap();

        Self { thread_hdl }
    }

    fn run(packet_receiver: Receiver<PacketBatch>, mut verifier: BLSSigVerifier) {
        let mut stats = BLSPacketStats::default();
        let mut last_print = Instant::now();
        const MAX_DEDUPER_AGE: Duration = Duration::from_secs(2);
        const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
        const DEDUPER_NUM_BITS: u64 = 63_999_979;

        let mut rng = rand::thread_rng();
        let mut deduper = Deduper::<2, [u8]>::new(&mut rng, DEDUPER_NUM_BITS);

        loop {
            if deduper.maybe_reset(&mut rng, DEDUPER_FALSE_POSITIVE_RATE, MAX_DEDUPER_AGE) {
                stats.num_deduper_saturations += 1;
            }
            if let Err(e) = Self::verifier(&deduper, &packet_receiver, &mut verifier, &mut stats) {
                match e {
                    BLSSigVerifyError::Streamer(StreamerError::RecvTimeout(
                        RecvTimeoutError::Disconnected,
                    )) => break,
                    BLSSigVerifyError::Streamer(StreamerError::RecvTimeout(
                        RecvTimeoutError::Timeout,
                    )) => (),
                    BLSSigVerifyError::Send(_) | BLSSigVerifyError::TrySend(_) => break,
                    _ => error!("{e:?}"),
                }
            }
            if last_print.elapsed().as_secs() > 2 {
                stats.maybe_report();
                stats = BLSPacketStats::default();
                last_print = Instant::now();
            }
        }
    }

    fn verifier<const K: usize>(
        deduper: &Deduper<K, [u8]>,
        recvr: &Receiver<PacketBatch>,
        verifier: &mut BLSSigVerifier,
        stats: &mut BLSPacketStats,
    ) -> Result<(), BLSSigVerifyError> {
        let (mut batches, num_packets, recv_duration) = streamer::recv_packet_batches(recvr)?;

        let batches_len = batches.len();
        debug!(
            "@{:?} bls_verifier: verifying: {}",
            timing::timestamp(),
            num_packets,
        );

        let mut dedup_time = Measure::start("bls_sigverify_dedup_time");
        let discard_or_dedup_fail =
            deduper::dedup_packets_and_count_discards(deduper, &mut batches) as usize;
        dedup_time.stop();

        let mut verify_time = Measure::start("sigverify_batch_time");
        verifier.verify_and_send_batches(batches)?;
        verify_time.stop();

        debug!(
            "@{:?} verifier: done. batches: {} total verify time: {:?} verified: {} v/s {}",
            timing::timestamp(),
            batches_len,
            verify_time.as_ms(),
            num_packets,
            (num_packets as f32 / verify_time.as_s())
        );

        stats
            .recv_batches_us_hist
            .increment(recv_duration.as_micros() as u64)
            .unwrap();
        stats
            .verify_batches_pp_us_hist
            .increment(verify_time.as_us() / (num_packets as u64))
            .unwrap();
        stats
            .dedup_packets_pp_us_hist
            .increment(dedup_time.as_us() / (num_packets as u64))
            .unwrap();
        stats.batches_hist.increment(batches_len as u64).unwrap();
        stats.packets_hist.increment(num_packets as u64).unwrap();
        stats.total_batches += batches_len;
        stats.total_packets += num_packets;
        stats.total_dedup += discard_or_dedup_fail;
        stats.total_dedup_time_us += dedup_time.as_us() as usize;
        stats.total_verify_time_us += verify_time.as_us() as usize;

        Ok(())
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
