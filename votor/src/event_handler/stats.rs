use {
    crate::{event::VotorEvent, voting_utils::BLSOp, VoteType},
    solana_metrics::datapoint_info,
    solana_votor_messages::bls_message::BLSMessage,
    std::time::{Duration, Instant},
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct EventHandlerStats {
    pub(crate) commitment_updates: u32,
    pub(crate) ignored: u32,
    pub(crate) leader_window_replaced: u32,
    pub(crate) set_root: u32,
    pub(crate) timeout_set: u32,

    /// All of the following fields are in microseconds
    pub(crate) receive_event_time: u64,
    pub(crate) send_vote_time: u64,

    pub(crate) received_events_count_and_timing: Vec<(u32, u64)>,
    pub(crate) sent_votes: Vec<u32>,

    pub(crate) last_report_time: Instant,
}

impl Default for EventHandlerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHandlerStats {
    fn event_to_index(event: &VotorEvent) -> usize {
        match event {
            VotorEvent::Block(_) => 0,
            VotorEvent::BlockNotarized(_) => 1,
            VotorEvent::FirstShred(_) => 2,
            VotorEvent::ParentReady { .. } => 3,
            VotorEvent::TimeoutCrashedLeader(_) => 4,
            VotorEvent::Timeout(_) => 5,
            VotorEvent::SafeToNotar(_) => 6,
            VotorEvent::SafeToSkip(_) => 7,
            VotorEvent::ProduceWindow(_) => 8,
            VotorEvent::Finalized(_) => 9,
            VotorEvent::Standstill(_) => 10,
            VotorEvent::SetIdentity => 11,
            // If you add an entry, change num_event_types below.
        }
    }

    pub fn new() -> Self {
        let num_vote_types = (VoteType::SkipFallback as usize).saturating_add(1);
        let num_event_types = 12; // Update this if you add more events.
        Self {
            commitment_updates: 0,
            ignored: 0,
            leader_window_replaced: 0,
            set_root: 0,
            timeout_set: 0,
            receive_event_time: 0,
            send_vote_time: 0,
            received_events_count_and_timing: vec![(0, 0); num_event_types],
            sent_votes: vec![0; num_vote_types],
            last_report_time: Instant::now(),
        }
    }

    pub fn incr_event_with_timing(&mut self, event: &VotorEvent, timing: u64) {
        let index = Self::event_to_index(event);
        if index < self.received_events_count_and_timing.len() {
            let entry = &mut self.received_events_count_and_timing[index];
            entry.0 = entry.0.saturating_add(1);
            entry.1 = entry.1.saturating_add(timing);
        } else {
            warn!(
                "Event index {} out of bounds for received_events_count_and_timing",
                index
            );
        }
    }

    pub fn incr_vote(&mut self, bls_op: &BLSOp) {
        if let BLSOp::PushVote { bls_message, .. } = bls_op {
            let BLSMessage::Vote(vote) = **bls_message else {
                warn!("Unexpected BLS message type: {:?}", bls_message);
                return;
            };
            let vote_index = VoteType::get_type(&vote.vote) as usize;
            if vote_index < self.sent_votes.len() {
                self.sent_votes[vote_index] = self.sent_votes[vote_index].saturating_add(1);
            } else {
                warn!(
                    "Vote type index {} out of bounds for sent_votes",
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
        datapoint_info!(
            "event_handler_received_events_count_and_timing",
            (
                "block",
                self.received_events_count_and_timing
                    .first()
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "block_notarized",
                self.received_events_count_and_timing
                    .get(1)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "first_shred",
                self.received_events_count_and_timing
                    .get(2)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "parent_ready",
                self.received_events_count_and_timing
                    .get(3)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "timeout_crashed_leader",
                self.received_events_count_and_timing
                    .get(4)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "timeout",
                self.received_events_count_and_timing
                    .get(5)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "safe_to_notar",
                self.received_events_count_and_timing
                    .get(6)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "safe_to_skip",
                self.received_events_count_and_timing
                    .get(7)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "produce_window",
                self.received_events_count_and_timing
                    .get(8)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "finalized",
                self.received_events_count_and_timing
                    .get(9)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "standstill",
                self.received_events_count_and_timing
                    .get(10)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "set_identity",
                self.received_events_count_and_timing
                    .get(11)
                    .map(|(count, _)| *count)
                    .unwrap_or(0) as i64,
                i64
            ),
        );
        datapoint_info!(
            "event_handler_handle_events_timing",
            (
                "block",
                self.received_events_count_and_timing
                    .first()
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "block_notarized",
                self.received_events_count_and_timing
                    .get(1)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "first_shred",
                self.received_events_count_and_timing
                    .get(2)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "parent_ready",
                self.received_events_count_and_timing
                    .get(3)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "timeout_crashed_leader",
                self.received_events_count_and_timing
                    .get(4)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "timeout",
                self.received_events_count_and_timing
                    .get(5)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "safe_to_notar",
                self.received_events_count_and_timing
                    .get(6)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "safe_to_skip",
                self.received_events_count_and_timing
                    .get(7)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "produce_window",
                self.received_events_count_and_timing
                    .get(8)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "finalized",
                self.received_events_count_and_timing
                    .get(9)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "standstill",
                self.received_events_count_and_timing
                    .get(10)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
            (
                "set_identity",
                self.received_events_count_and_timing
                    .get(11)
                    .map(|(_, ms)| *ms)
                    .unwrap_or(0) as i64,
                i64
            ),
        );
        datapoint_info!(
            "event_handler_timing",
            ("receive_event_time", self.receive_event_time as i64, i64),
            ("send_vote_time", self.send_vote_time as i64, i64),
        );
        datapoint_info!(
            "event_handler_sent_votes",
            (
                "finalize",
                *self
                    .sent_votes
                    .get(VoteType::Finalize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize",
                *self
                    .sent_votes
                    .get(VoteType::Notarize as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "notarize_fallback",
                *self
                    .sent_votes
                    .get(VoteType::NotarizeFallback as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
            (
                "skip",
                *self.sent_votes.get(VoteType::Skip as usize).unwrap_or(&0) as i64,
                i64
            ),
            (
                "skip_fallback",
                *self
                    .sent_votes
                    .get(VoteType::SkipFallback as usize)
                    .unwrap_or(&0) as i64,
                i64
            ),
        );
        self.last_report_time = now;
        *self = EventHandlerStats::new();
    }
}
