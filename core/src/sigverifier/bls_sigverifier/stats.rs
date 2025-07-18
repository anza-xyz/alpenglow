use std::time::{Duration, Instant};

pub(super) const STATS_INTERVAL_DURATION: Duration = Duration::from_secs(1);

// We are adding our own stats because we do BLS decoding in batch verification,
// and we send one BLS message at a time. So it makes sense to have finer-grained stats
#[derive(Debug)]
pub(super) struct BLSSigVerifierStats {
    pub(super) sent: u64,
    pub(super) sent_failed: u64,
    pub(super) verified_votes_sent: u64,
    pub(super) verified_votes_sent_failed: u64,
    pub(super) received: u64,
    pub(super) received_malformed: u64,
    pub(super) received_no_epoch_stakes: u64,
    pub(super) received_votes: u64,
    pub(super) last_stats_logged: Instant,
}

impl BLSSigVerifierStats {
    pub(super) fn new() -> Self {
        Self {
            sent: 0,
            sent_failed: 0,
            verified_votes_sent: 0,
            verified_votes_sent_failed: 0,
            received: 0,
            received_malformed: 0,
            received_no_epoch_stakes: 0,
            received_votes: 0,
            last_stats_logged: Instant::now(),
        }
    }

    pub(super) fn report_stats(&mut self) {
        let now = Instant::now();
        let time_since_last_log = now.duration_since(self.last_stats_logged);
        if time_since_last_log < STATS_INTERVAL_DURATION {
            return;
        }
        datapoint_info!(
            "bls_sig_verifier_stats",
            ("sent", self.sent as i64, i64),
            ("sent_failed", self.sent_failed as i64, i64),
            ("verified_votes_sent", self.verified_votes_sent as i64, i64),
            (
                "verified_votes_sent_failed",
                self.verified_votes_sent_failed as i64,
                i64
            ),
            ("received", self.received as i64, i64),
            ("received_votes", self.received_votes as i64, i64),
            (
                "received_no_epoch_stakes",
                self.received_no_epoch_stakes as i64,
                i64
            ),
            ("received_malformed", self.received_malformed as i64, i64),
        );
        *self = BLSSigVerifierStats::new();
    }
}
