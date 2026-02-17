#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    histogram::Histogram,
    std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    },
};

pub(super) const STATS_INTERVAL_DURATION: Duration = Duration::from_secs(1);

pub(super) struct PacketStats {
    /// Measurements of [`BLSSigVerifyService::receive_packets`].
    recv_batches_hist: Histogram,
    /// Measurements of [`BLSSigVerifyService::verify_packets`].
    verify_batches_hist: Histogram,
    /// Measurements of batches sizes received from [`BLSSigVerifyService::receive_packets`].
    batches_hist: Histogram,
    /// Measurements of packets received from [`BLSSigVerifyService::receive_packets`].
    packets_hist: Histogram,
    /// Total amount of time spent calling [`BLSSigVerifyService::verify_packets`].
    total_verify_time_us: u64,
    /// Tracks when stats were last reported.
    last_report: Instant,
}

impl Default for PacketStats {
    fn default() -> Self {
        Self {
            recv_batches_hist: Histogram::default(),
            verify_batches_hist: Histogram::default(),
            batches_hist: Histogram::default(),
            packets_hist: Histogram::default(),
            total_verify_time_us: 0,
            last_report: Instant::now(),
        }
    }
}

impl PacketStats {
    pub(super) fn update(
        &mut self,
        num_packets_received: u64,
        num_batches_received: u64,
        receive_packets_us: u64,
        verify_packets_us: u64,
    ) {
        self.recv_batches_hist
            .increment(receive_packets_us)
            .unwrap();
        self.verify_batches_hist
            .increment(verify_packets_us / (num_packets_received))
            .unwrap();
        self.batches_hist.increment(num_batches_received).unwrap();
        self.packets_hist.increment(num_packets_received).unwrap();
        self.total_verify_time_us += verify_packets_us;
    }

    pub(super) fn maybe_report(&mut self) {
        let Self {
            recv_batches_hist,
            verify_batches_hist,
            batches_hist,
            packets_hist,
            total_verify_time_us,
            last_report,
        } = self;

        if last_report.elapsed() < STATS_INTERVAL_DURATION || batches_hist.entries() == 0 {
            return;
        }

        datapoint_info!(
            "bls-verifier-packet-stats",
            (
                "recv_batches_us_90pct",
                recv_batches_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_min",
                recv_batches_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_max",
                recv_batches_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_mean",
                recv_batches_hist.mean().unwrap_or(0),
                i64
            ),
            ("recv_batches_count", recv_batches_hist.entries(), i64),
            (
                "verify_batches_us_90pct",
                verify_batches_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "verify_batches_us_min",
                verify_batches_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_us_max",
                verify_batches_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_us_mean",
                verify_batches_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_us_count",
                verify_batches_hist.entries(),
                i64
            ),
            (
                "batches_90pct",
                batches_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            ("batches_min", batches_hist.minimum().unwrap_or(0), i64),
            ("batches_max", batches_hist.maximum().unwrap_or(0), i64),
            ("batches_mean", batches_hist.mean().unwrap_or(0), i64),
            ("batches_count", batches_hist.entries(), i64),
            (
                "packets_90pct",
                packets_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            ("packets_min", packets_hist.minimum().unwrap_or(0), i64),
            ("packets_max", packets_hist.maximum().unwrap_or(0), i64),
            ("packets_mean", packets_hist.mean().unwrap_or(0), i64),
            ("packets_count", packets_hist.entries(), i64),
            ("total_verify_time_us", *total_verify_time_us, i64),
        );

        *self = Self::default();
    }
}

// We are adding our own stats because we do BLS decoding in batch verification,
// and we send one BLS message at a time. So it makes sense to have finer-grained stats
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(super) struct BLSSigVerifierStats {
    pub(super) total_valid_packets: AtomicU64,
    pub(super) preprocess_count: AtomicU64,
    pub(super) preprocess_elapsed_us: AtomicU64,
    pub(super) votes_batch_distinct_messages_count: AtomicU64,
    pub(super) votes_batch_optimistic_elapsed_us: AtomicU64,
    pub(super) votes_batch_parallel_verify_count: AtomicU64,
    pub(super) votes_batch_parallel_verify_elapsed_us: AtomicU64,
    pub(super) certs_batch_count: AtomicU64,
    pub(super) certs_batch_elapsed_us: AtomicU64,

    /// Number of votes that we attempted to verify.
    pub(super) votes_to_verify: AtomicU64,
    /// Number of batches of votes that we attempted to verify.
    pub(super) votes_to_verify_batches: AtomicU64,
    /// Number of votes that were successfully verified.
    pub(super) verified_votes: AtomicU64,
    /// Number of msgs sent to the consensus pool after verifying votes.
    pub(super) verify_votes_consensus_sent: AtomicU64,
    /// Number of msgs sent to repair after verifying votes.
    pub(super) verify_votes_repair_sent: AtomicU64,
    /// Number of msgs sent to rewards after verifying votes.
    pub(super) verify_votes_rewards_sent: AtomicU64,
    /// Number of msgs sent to metrics after verifying votes.
    pub(super) verify_votes_metrics_sent: AtomicU64,
    /// Number of times the consensus channel was full while verifying votes.
    pub(super) verify_votes_consensus_channel_full: AtomicU64,
    /// Number of times the repair channel was full while verifying votes.
    pub(super) verify_votes_repair_channel_full: AtomicU64,
    /// Number of times the rewards channel was full while verifying votes.
    pub(super) verify_votes_rewards_channel_full: AtomicU64,
    /// Number of times the metrics channel was full while verifying votes.
    pub(super) verify_votes_metrics_channel_full: AtomicU64,

    /// Number of msgs sent to the consensus pool after verifying certs.
    pub(super) verify_certs_consensus_sent: AtomicU64,
    /// Number of times the consensus channel was full while verifying certs.
    pub(super) verify_certs_consensus_channel_full: AtomicU64,

    pub(super) received: AtomicU64,
    pub(super) received_bad_rank: AtomicU64,
    pub(super) received_bad_signature_certs: AtomicU64,
    pub(super) received_bad_signature_votes: AtomicU64,
    pub(super) received_not_enough_stake: AtomicU64,
    pub(super) received_discarded: AtomicU64,
    pub(super) received_malformed: AtomicU64,
    pub(super) received_no_epoch_stakes: AtomicU64,
    pub(super) received_old: AtomicU64,
    pub(super) received_verified: AtomicU64,
    pub(super) received_votes: AtomicU64,
    pub(super) last_stats_logged: Instant,
}

impl Default for BLSSigVerifierStats {
    fn default() -> Self {
        Self {
            total_valid_packets: AtomicU64::new(0),

            preprocess_count: AtomicU64::new(0),
            preprocess_elapsed_us: AtomicU64::new(0),
            votes_batch_distinct_messages_count: AtomicU64::new(0),
            votes_batch_optimistic_elapsed_us: AtomicU64::new(0),
            votes_batch_parallel_verify_count: AtomicU64::new(0),
            votes_batch_parallel_verify_elapsed_us: AtomicU64::new(0),
            certs_batch_count: AtomicU64::new(0),
            certs_batch_elapsed_us: AtomicU64::new(0),

            votes_to_verify: AtomicU64::new(0),
            votes_to_verify_batches: AtomicU64::new(0),
            verified_votes: AtomicU64::new(0),
            verify_votes_consensus_sent: AtomicU64::new(0),
            verify_votes_repair_sent: AtomicU64::new(0),
            verify_votes_rewards_sent: AtomicU64::new(0),
            verify_votes_metrics_sent: AtomicU64::new(0),
            verify_votes_consensus_channel_full: AtomicU64::new(0),
            verify_votes_repair_channel_full: AtomicU64::new(0),
            verify_votes_rewards_channel_full: AtomicU64::new(0),
            verify_votes_metrics_channel_full: AtomicU64::new(0),

            verify_certs_consensus_sent: AtomicU64::new(0),
            verify_certs_consensus_channel_full: AtomicU64::new(0),

            received: AtomicU64::new(0),
            received_bad_rank: AtomicU64::new(0),
            received_bad_signature_certs: AtomicU64::new(0),
            received_bad_signature_votes: AtomicU64::new(0),
            received_not_enough_stake: AtomicU64::new(0),
            received_discarded: AtomicU64::new(0),
            received_malformed: AtomicU64::new(0),
            received_no_epoch_stakes: AtomicU64::new(0),
            received_old: AtomicU64::new(0),
            received_verified: AtomicU64::new(0),
            received_votes: AtomicU64::new(0),
            last_stats_logged: Instant::now(),
        }
    }
}

impl BLSSigVerifierStats {
    /// If sufficient time has passed since last report, report stats.
    pub(super) fn maybe_report_stats(&mut self) {
        let now = Instant::now();
        let time_since_last_log = now.duration_since(self.last_stats_logged);
        if time_since_last_log < STATS_INTERVAL_DURATION {
            return;
        }
        datapoint_info!(
            "bls_sig_verifier_stats",
            (
                "preprocess_count",
                self.preprocess_count.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "preprocess_elapsed_us",
                self.preprocess_elapsed_us.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_distinct_messages_count",
                self.votes_batch_distinct_messages_count
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_optimistic_elapsed_us",
                self.votes_batch_optimistic_elapsed_us
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_parallel_verify_count",
                self.votes_batch_parallel_verify_count
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_parallel_verify_elapsed_us",
                self.votes_batch_parallel_verify_elapsed_us
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "certs_batch_count",
                self.certs_batch_count.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "certs_batch_elapsed_us",
                self.certs_batch_elapsed_us.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_to_verify_batches",
                self.votes_to_verify_batches.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_to_verify",
                self.votes_to_verify.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verified_votes",
                self.verified_votes.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_votes_consensus_sent",
                self.verify_votes_consensus_sent.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_votes_consensus_channel_full",
                self.verify_votes_consensus_channel_full
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_votes_repair_sent",
                self.verify_votes_repair_sent.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verified_votes_repair_channel_full",
                self.verify_votes_repair_channel_full
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_votes_rewards_sent",
                self.verify_votes_rewards_sent.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_votes_rewards_channel_full",
                self.verify_votes_rewards_channel_full
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_votes_metrics_sent",
                self.verify_votes_metrics_sent.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_votes_metrics_channel_full",
                self.verify_votes_metrics_channel_full
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_certs_consensus_sent",
                self.verify_certs_consensus_sent.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verity_certs_consensus_channel_full",
                self.verify_certs_consensus_channel_full
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received",
                self.received.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_bad_rank",
                self.received_bad_rank.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_bad_signature_certs",
                self.received_bad_signature_certs.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_bad_signature_votes",
                self.received_bad_signature_votes.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_not_enough_stake",
                self.received_not_enough_stake.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_discarded",
                self.received_discarded.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_old",
                self.received_old.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_verified",
                self.received_verified.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_votes",
                self.received_votes.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_no_epoch_stakes",
                self.received_no_epoch_stakes.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_malformed",
                self.received_malformed.load(Ordering::Relaxed) as i64,
                i64
            ),
        );
        *self = BLSSigVerifierStats::default();
    }
}
