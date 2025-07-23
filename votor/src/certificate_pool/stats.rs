use {
    crate::VoteType,
    alpenglow_vote::certificate::CertificateType,
    solana_metrics::datapoint_info,
    std::time::{Duration, Instant},
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct CertificatePoolStats {
    pub(crate) conflicting_votes: u32,
    pub(crate) event_safe_to_skip: u32,
    pub(crate) event_safe_to_notarize: u32,

    pub(crate) exits: [u32; 2],
    pub(crate) incoming: [u32; 2],
    pub(crate) out_of_range: [u32; 2],

    pub(crate) new_certs_by_type: [Vec<u32>; 2], // 0 for ingested, 1 for generated
    pub(crate) ingested_votes_by_type: Vec<u32>,
    last_reported: Instant,
}

impl Default for CertificatePoolStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CertificatePoolStats {
    pub fn new() -> Self {
        Self {
            conflicting_votes: 0,
            event_safe_to_skip: 0,
            event_safe_to_notarize: 0,
            exits: [0, 0],
            incoming: [0, 0],
            out_of_range: [0, 0],
            new_certs_by_type: [Vec::new(), Vec::new()],
            ingested_votes_by_type: Vec::new(),
            last_reported: Instant::now(),
        }
    }

    pub fn incr_u32_array(value: &mut [u32; 2], is_vote: bool) {
        if is_vote {
            value[0] = value[0].saturating_add(1);
        } else {
            value[1] = value[1].saturating_add(1);
        }
    }

    pub fn incr_u32(value: &mut u32) {
        *value = value.saturating_add(1);
    }

    pub fn incr_ingested_vote_type(&mut self, vote_type: VoteType) {
        let index = vote_type as usize;

        if self.ingested_votes_by_type.len() <= index {
            self.ingested_votes_by_type
                .resize(index.saturating_add(1), 0);
        }

        self.ingested_votes_by_type[index] = self.ingested_votes_by_type[index].saturating_add(1);
    }

    pub fn incr_cert_type(&mut self, cert_type: CertificateType, is_generated: bool) {
        let index = cert_type as usize;
        let array = if is_generated {
            &mut self.new_certs_by_type[0]
        } else {
            &mut self.new_certs_by_type[1]
        };

        if array.len() <= index {
            array.resize(index.saturating_add(1), 0);
        }

        array[index] = array[index].saturating_add(1);
    }

    fn report(&self) {
        datapoint_info!(
            "certificate_pool_stats",
            ("conflicting_votes", self.conflicting_votes as i64, i64),
            ("event_safe_to_skip", self.event_safe_to_skip as i64, i64),
            (
                "event_safe_to_notarize",
                self.event_safe_to_notarize as i64,
                i64
            ),
            ("exits_votes", self.exits[0] as i64, i64),
            ("exits_certificates", self.exits[1] as i64, i64),
            ("incoming_votes", self.incoming[0] as i64, i64),
            ("incoming_certificates", self.incoming[1] as i64, i64),
            ("out_of_range_votes", self.out_of_range[0] as i64, i64),
            (
                "out_of_range_certificates",
                self.out_of_range[1] as i64,
                i64
            ),
        );

        datapoint_info!(
            "certificate_pool_ingested_votes_by_type",
            (
                "finalize",
                *self
                    .ingested_votes_by_type
                    .get(VoteType::Finalize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize",
                *self
                    .ingested_votes_by_type
                    .get(VoteType::Notarize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize_fallback",
                *self
                    .ingested_votes_by_type
                    .get(VoteType::NotarizeFallback as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "skip",
                *self
                    .ingested_votes_by_type
                    .get(VoteType::Skip as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "skip_fallback",
                *self
                    .ingested_votes_by_type
                    .get(VoteType::SkipFallback as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
        );

        datapoint_info!(
            "certfificate_pool_ingested_certs_by_type",
            (
                "finalize",
                *self.new_certs_by_type[0]
                    .get(CertificateType::Finalize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "finalize_fast",
                *self.new_certs_by_type[0]
                    .get(CertificateType::FinalizeFast as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize",
                *self.new_certs_by_type[0]
                    .get(CertificateType::Notarize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize_fallback",
                *self.new_certs_by_type[0]
                    .get(CertificateType::NotarizeFallback as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "skip",
                *self.new_certs_by_type[0]
                    .get(CertificateType::Skip as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
        );

        datapoint_info!(
            "certificate_pool_generated_certs_by_type",
            (
                "finalize",
                *self.new_certs_by_type[1]
                    .get(CertificateType::Finalize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "finalize_fast",
                *self.new_certs_by_type[1]
                    .get(CertificateType::FinalizeFast as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize",
                *self.new_certs_by_type[1]
                    .get(CertificateType::Notarize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize_fallback",
                *self.new_certs_by_type[1]
                    .get(CertificateType::NotarizeFallback as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "skip",
                *self.new_certs_by_type[1]
                    .get(CertificateType::Skip as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
        );
    }

    pub fn maybe_report(&mut self) {
        if self.last_reported.elapsed() >= STATS_REPORT_INTERVAL {
            self.report();
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.conflicting_votes = 0;
        self.event_safe_to_skip = 0;
        self.event_safe_to_notarize = 0;
        for i in 0..2 {
            self.incoming[i] = 0;
            self.out_of_range[i] = 0;
        }
        self.new_certs_by_type[0].clear();
        self.new_certs_by_type[1].clear();
        self.ingested_votes_by_type.clear();
        self.last_reported = Instant::now();
    }
}
