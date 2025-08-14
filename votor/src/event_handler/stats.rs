use {
    crate::{voting_utils::BLSOp, VoteType},
    solana_metrics::datapoint_info,
    solana_votor_messages::bls_message::BLSMessage,
    std::time::{Duration, Instant},
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct EventHandlerStats {
    pub(crate) event_block: u32,
    pub(crate) event_block_notarized: u32,
    pub(crate) event_first_shred: u32,
    pub(crate) event_parent_ready: u32,
    pub(crate) event_timeout_crashed_leader: u32,
    pub(crate) event_timeout: u32,
    pub(crate) event_safe_to_notar: u32,
    pub(crate) event_safe_to_skip: u32,
    pub(crate) event_produce_window: u32,
    pub(crate) event_finalized: u32,
    pub(crate) event_standstill: u32,
    pub(crate) event_set_identity: bool,
    pub(crate) commitment_updates: u32,
    pub(crate) ignored: u32,
    pub(crate) leader_window_replaced: u32,
    pub(crate) set_root: u32,
    pub(crate) timeout_set: u32,
    pub(crate) sentout_votes: Vec<u32>,
    pub(crate) last_report_time: Instant,
}

impl Default for EventHandlerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHandlerStats {
    pub fn new() -> Self {
        let num_vote_types = (VoteType::SkipFallback as usize).saturating_add(1);
        Self {
            event_block: 0,
            event_block_notarized: 0,
            event_first_shred: 0,
            event_parent_ready: 0,
            event_timeout_crashed_leader: 0,
            event_timeout: 0,
            event_safe_to_notar: 0,
            event_safe_to_skip: 0,
            event_produce_window: 0,
            event_finalized: 0,
            event_standstill: 0,
            event_set_identity: false,
            commitment_updates: 0,
            ignored: 0,
            leader_window_replaced: 0,
            set_root: 0,
            timeout_set: 0,
            sentout_votes: vec![0; num_vote_types],
            last_report_time: Instant::now(),
        }
    }

    pub fn incr_vote(&mut self, bls_op: &BLSOp) {
        if let BLSOp::PushVote { bls_message, .. } = bls_op {
            let BLSMessage::Vote(vote) = **bls_message else {
                warn!("Unexpected BLS message type: {:?}", bls_message);
                return;
            };
            let vote_index = VoteType::get_type(&vote.vote) as usize;
            if vote_index < self.sentout_votes.len() {
                self.sentout_votes[vote_index] = self.sentout_votes[vote_index].saturating_add(1);
            } else {
                warn!(
                    "Vote type index {} out of bounds for sentout_votes",
                    vote_index
                );
            }
        } else {
            warn!("Unexpected BLS operation: {:?}", bls_op);
        }
    }

    pub fn maybe_report(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_report_time) < STATS_REPORT_INTERVAL {
            return;
        }
        datapoint_info!(
            "event_handler_stats",
            ("event_block", self.event_block as i64, i64),
            (
                "event_block_notarized",
                self.event_block_notarized as i64,
                i64
            ),
            ("event_first_shred", self.event_first_shred as i64, i64),
            ("event_parent_ready", self.event_parent_ready as i64, i64),
            (
                "event_timeout_crashed_leader",
                self.event_timeout_crashed_leader as i64,
                i64
            ),
            ("event_timeout", self.event_timeout as i64, i64),
            ("event_safe_to_notar", self.event_safe_to_notar as i64, i64),
            ("event_safe_to_skip", self.event_safe_to_skip as i64, i64),
            (
                "event_produce_window",
                self.event_produce_window as i64,
                i64
            ),
            ("event_finalized", self.event_finalized as i64, i64),
            ("event_standstill", self.event_standstill as i64, i64),
            ("event_set_identity", self.event_set_identity, i64),
            ("commitment_updates", self.commitment_updates as i64, i64),
            ("ignored", self.ignored as i64, i64),
            (
                "leader_window_replaced",
                self.leader_window_replaced as i64,
                i64
            ),
            ("set_root", self.set_root as i64, i64),
            ("timeout_set", self.timeout_set as i64, i64),
        );
        self.last_report_time = now;
        *self = EventHandlerStats::new();
    }
}
