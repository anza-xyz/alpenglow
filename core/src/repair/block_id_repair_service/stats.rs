#[derive(Default)]
pub(super) struct BlockIdRepairResponsesStats {
    pub total_packets: usize,
    pub processed: usize,
    pub dropped_packets: usize,
    pub invalid_packets: usize,
    pub ping_responses: usize,
    pub parent_fec_set_count_responses: usize,
    pub fec_set_root_responses: usize,
}

impl BlockIdRepairResponsesStats {
    pub fn report(&mut self) {
        datapoint_info!(
            "block_id_repair_responses",
            ("total_packets", self.total_packets, i64),
            ("processed", self.processed, i64),
            ("dropped_packets", self.dropped_packets, i64),
            ("invalid_packets", self.invalid_packets, i64),
            ("ping_responses", self.ping_responses, i64),
            (
                "parent_fec_set_count_responses",
                self.parent_fec_set_count_responses,
                i64
            ),
            ("fec_set_root_responses", self.fec_set_root_responses, i64),
        );
        *self = Self::default();
    }
}

#[derive(Default)]
pub(super) struct BlockIdRepairRequestsStats {
    pub total_requests: usize,
    pub parent_fec_set_count_requests: usize,
    pub fec_set_root_requests: usize,
    pub shred_for_block_id_requests: usize,
}

impl BlockIdRepairRequestsStats {
    pub fn report(&mut self) {
        datapoint_info!(
            "block_id_repair_requests",
            ("total_requests", self.total_requests, i64),
            (
                "parent_fec_set_count_requests",
                self.parent_fec_set_count_requests,
                i64
            ),
            ("fec_set_root_requests", self.fec_set_root_requests, i64),
            (
                "shred_for_block_id_requests",
                self.shred_for_block_id_requests,
                i64
            ),
        );
        *self = Self::default();
    }
}
