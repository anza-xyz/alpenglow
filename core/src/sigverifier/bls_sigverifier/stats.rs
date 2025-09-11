use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

pub(super) const STATS_INTERVAL_DURATION: Duration = Duration::from_secs(1);

// We are adding our own stats because we do BLS decoding in batch verification,
// and we send one BLS message at a time. So it makes sense to have finer-grained stats
#[derive(Debug)]
pub(super) struct BLSSigVerifierStats {
    pub(super) verify_preprocess_count: AtomicU64,
    pub(super) verify_preprocess_elapsed_us: AtomicU64,
    pub(super) votes_batch_count: AtomicU64,
    pub(super) votes_batch_elapsed_us: AtomicU64,
    pub(super) certs_batch_count: AtomicU64,
    pub(super) certs_batch_elapsed_us: AtomicU64,

    pub(super) sent: AtomicU64,
    pub(super) sent_failed: AtomicU64,
    pub(super) verified_votes_sent: AtomicU64,
    pub(super) verified_votes_sent_failed: AtomicU64,
    pub(super) received: AtomicU64,
    pub(super) received_bad_rank: AtomicU64,
    pub(super) received_bad_signature_certs: AtomicU64,
    pub(super) received_bad_signature_votes: AtomicU64,
    pub(super) received_discarded: AtomicU64,
    pub(super) received_malformed: AtomicU64,
    pub(super) received_no_epoch_stakes: AtomicU64,
    pub(super) received_votes: AtomicU64,
    pub(super) last_stats_logged: Instant,
}

impl BLSSigVerifierStats {
    pub(super) fn new() -> Self {
        Self {
            verify_preprocess_count: AtomicU64::new(0),
            verify_preprocess_elapsed_us: AtomicU64::new(0),
            votes_batch_count: AtomicU64::new(0),
            votes_batch_elapsed_us: AtomicU64::new(0),
            certs_batch_count: AtomicU64::new(0),
            certs_batch_elapsed_us: AtomicU64::new(0),

            sent: AtomicU64::new(0),
            sent_failed: AtomicU64::new(0),
            verified_votes_sent: AtomicU64::new(0),
            verified_votes_sent_failed: AtomicU64::new(0),
            received: AtomicU64::new(0),
            received_bad_rank: AtomicU64::new(0),
            received_bad_signature_certs: AtomicU64::new(0),
            received_bad_signature_votes: AtomicU64::new(0),
            received_discarded: AtomicU64::new(0),
            received_malformed: AtomicU64::new(0),
            received_no_epoch_stakes: AtomicU64::new(0),
            received_votes: AtomicU64::new(0),
            last_stats_logged: Instant::now(),
        }
    }

    /// If sufficient time has passed since last report, report stats.
    pub(super) fn maybe_report_stats(&mut self) {
        let now = Instant::now();
        let time_since_last_log = now.duration_since(self.last_stats_logged);
        if time_since_last_log < STATS_INTERVAL_DURATION {
            return;
        }
        datapoint_info!(
            "bls_sig_verifier_timing",
            (
                "verify_preprocess_count",
                self.verify_preprocess_count.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verify_preprocess_elapsed_us",
                self.verify_preprocess_elapsed_us.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_count",
                self.votes_batch_count.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_elapsed_us",
                self.votes_batch_elapsed_us.load(Ordering::Relaxed) as i64,
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
        );
        datapoint_info!(
            "bls_sig_verifier_stats",
            ("sent", self.sent.load(Ordering::Relaxed) as i64, i64),
            (
                "sent_failed",
                self.sent_failed.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verified_votes_sent",
                self.verified_votes_sent.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verified_votes_sent_failed",
                self.verified_votes_sent_failed.load(Ordering::Relaxed) as i64,
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
                "received_discarded",
                self.received_discarded.load(Ordering::Relaxed) as i64,
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
        *self = BLSSigVerifierStats::new();
    }
}
