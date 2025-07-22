use {
    solana_metrics::datapoint_info,
    std::time::{Duration, Instant},
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct CertificatePoolServiceStats {
    pub(crate) add_message_failed: u32,
    pub(crate) certificates_sent: u16,
    pub(crate) certificates_dropped: u16,
    pub(crate) new_finalized_slot: u16,
    pub(crate) new_parent_ready: u16,
    pub(crate) new_parent_ready_missed_window: u16,
    pub(crate) new_root: u16,
    pub(crate) received_votes: u32,
    pub(crate) received_certificates: u32,
    pub(crate) standstill_reset: u16,
    last_request_time: Instant,
}

impl CertificatePoolServiceStats {
    pub fn new() -> Self {
        Self {
            add_message_failed: 0,
            certificates_sent: 0,
            certificates_dropped: 0,
            new_finalized_slot: 0,
            new_parent_ready: 0,
            new_parent_ready_missed_window: 0,
            new_root: 0,
            received_votes: 0,
            received_certificates: 0,
            standstill_reset: 0,
            last_request_time: Instant::now(),
        }
    }

    pub fn incr_u16(value: &mut u16) {
        *value = value.saturating_add(1);
    }

    pub fn incr_u32(value: &mut u32) {
        *value = value.saturating_add(1);
    }

    fn reset(&mut self) {
        self.add_message_failed = 0;
        self.certificates_sent = 0;
        self.certificates_dropped = 0;
        self.new_finalized_slot = 0;
        self.new_parent_ready = 0;
        self.new_parent_ready_missed_window = 0;
        self.new_root = 0;
        self.received_votes = 0;
        self.received_certificates = 0;
        self.standstill_reset = 0;
        self.last_request_time = Instant::now();
    }

    fn report(&self) {
        datapoint_info!(
            "cert_pool_service",
            ("add_message_failed", self.add_message_failed, i64),
            ("certificates_sent", self.certificates_sent, i64),
            ("certificates_dropped", self.certificates_dropped, i64),
            ("new_finalized_slot", self.new_finalized_slot, i64),
            ("new_parent_ready", self.new_parent_ready, i64),
            (
                "new_parent_ready_missed_window",
                self.new_parent_ready_missed_window,
                i64
            ),
            ("new_root", self.new_root, i64),
            ("received_votes", self.received_votes, i64),
            ("received_certificates", self.received_certificates, i64),
            ("standstill_reset", self.standstill_reset, i64),
        );
    }

    pub fn maybe_report(&mut self) {
        if self.last_request_time.elapsed() >= STATS_REPORT_INTERVAL {
            self.report();
            self.reset();
        }
    }
}
