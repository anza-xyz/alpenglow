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
    solana_perf::packet::PacketBatch,
    solana_rpc::alpenglow_last_voted::AlpenglowLastVoted,
    solana_runtime::bank_forks::SharableBanks,
    solana_streamer::streamer::{self, StreamerError},
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

        loop {
            // Receive packets
            let (batches, num_packets, recv_duration) =
                match Self::receive_packets(&packet_receiver) {
                    ReceiveAction::Process(b, n, d) => (b, n, d),
                    ReceiveAction::Continue => continue,
                    ReceiveAction::Break => break,
                };
            let batches_len = batches.len();

            // BLS Signature Verification (and Send)
            //
            // TODO(sam): Currently, verification result is sent inside the verification function.
            //            Consider refactoring out the send step out of the signature verification
            //            step.
            let verify_time_us = Self::verify_packets(&mut verifier, batches);

            match verify_time_us {
                Ok(verify_time_us) => {
                    Self::update_stats(
                        &mut stats,
                        num_packets,
                        batches_len,
                        recv_duration,
                        verify_time_us,
                    );
                }
                Err(e) => match e {
                    BLSSigVerifyError::Send(_) | BLSSigVerifyError::TrySend(_) => break,
                    _ => error!("{e:?}"),
                },
            }

            if last_print.elapsed().as_secs() > 2 {
                stats.maybe_report();
                stats = BLSPacketStats::default();
                last_print = Instant::now();
            }
        }
    }

    fn receive_packets(packet_receiver: &Receiver<PacketBatch>) -> ReceiveAction {
        match streamer::recv_packet_batches(packet_receiver) {
            Ok((batches, num_packets, recv_duration)) => {
                ReceiveAction::Process(batches, num_packets, recv_duration)
            }
            Err(e) => match e {
                StreamerError::RecvTimeout(RecvTimeoutError::Disconnected) => ReceiveAction::Break,
                StreamerError::RecvTimeout(RecvTimeoutError::Timeout) => ReceiveAction::Continue,
                _ => {
                    error!("{e:?}");
                    ReceiveAction::Continue
                }
            },
        }
    }

    fn verify_packets(
        verifier: &mut BLSSigVerifier,
        batches: Vec<PacketBatch>,
    ) -> Result<u64, BLSSigVerifyError> {
        let mut verify_time = Measure::start("sigverify_batch_time");
        verifier.verify_and_send_batches(batches)?;
        verify_time.stop();
        Ok(verify_time.as_us())
    }

    fn update_stats(
        stats: &mut BLSPacketStats,
        num_packets: usize,
        batches_len: usize,
        recv_duration: Duration,
        verify_time_us: u64,
    ) {
        stats
            .recv_batches_us_hist
            .increment(recv_duration.as_micros() as u64)
            .unwrap();
        stats
            .verify_batches_pp_us_hist
            .increment(verify_time_us / (num_packets as u64))
            .unwrap();
        stats.batches_hist.increment(batches_len as u64).unwrap();
        stats.packets_hist.increment(num_packets as u64).unwrap();
        stats.total_batches += batches_len;
        stats.total_packets += num_packets;
        stats.total_verify_time_us += verify_time_us as usize;
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

enum ReceiveAction {
    Process(Vec<PacketBatch>, usize, Duration),
    Continue,
    Break,
}
