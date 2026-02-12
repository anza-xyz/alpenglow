use {
    crate::{
        bls_sigverify::{
            bls_sigverifier::BLSSigVerifier, error::BLSSigVerifyError, stats::PacketStats,
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
    },
};

pub struct BLSSigVerifyService {
    thread_hdl: JoinHandle<()>,
}

impl BLSSigVerifyService {
    pub fn new(
        packet_receiver: Receiver<PacketBatch>,
        sharable_banks: SharableBanks,
        channel_to_repair: VerifiedVoteSender,
        channel_to_reward: Sender<AddVoteMessage>,
        channel_to_pool: Sender<Vec<ConsensusMessage>>,
        consensus_metrics_sender: ConsensusMetricsEventSender,
        alpenglow_last_voted: Arc<AlpenglowLastVoted>,
        cluster_info: Arc<ClusterInfo>,
        leader_schedule: Arc<LeaderScheduleCache>,
    ) -> Self {
        let verifier = BLSSigVerifier::new(
            sharable_banks,
            channel_to_repair,
            channel_to_reward,
            channel_to_pool,
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
        let mut stats = PacketStats::default();
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
                    stats.update(
                        num_packets as u64,
                        batches_len as u64,
                        recv_duration.as_micros() as u64,
                        verify_time_us,
                    );
                }
                Err(e) => match e {
                    BLSSigVerifyError::Send(_) | BLSSigVerifyError::TrySend(_) => break,
                    _ => error!("{e:?}"),
                },
            }
            stats.maybe_report();
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

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

enum ReceiveAction {
    Process(Vec<PacketBatch>, usize, Duration),
    Continue,
    Break,
}
