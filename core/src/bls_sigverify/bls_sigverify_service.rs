use {
    super::{bls_sigverifier::BLSSigVerifier, stats::PacketStats},
    crate::cluster_info_vote_listener::VerifiedVoteSender,
    core::time::Duration,
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_measure::measure_us,
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
            let (verify_res, verify_time_us) =
                measure_us!(verifier.verify_and_send_batches(batches));
            stats.update(
                num_packets as u64,
                batches_len as u64,
                recv_duration.as_micros() as u64,
                verify_time_us,
            );
            match verify_res {
                Ok(()) => (),
                Err(e) => {
                    error!("verify_and_send_batches() failed with {e}.  Exiting.");
                    break;
                }
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

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

enum ReceiveAction {
    Process(Vec<PacketBatch>, usize, Duration),
    Continue,
    Break,
}
