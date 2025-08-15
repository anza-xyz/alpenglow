use {
    crate::{event::VotorEvent, voting_utils::BLSOp, VoteType},
    solana_metrics::datapoint_info,
    solana_votor_messages::bls_message::BLSMessage,
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct EventHandlerStats {
    pub(crate) ignored: u16,
    pub(crate) leader_window_replaced: u16,
    pub(crate) set_root: u16,
    pub(crate) timeout_set: u16,

    /// All of the following fields are in microseconds
    pub(crate) receive_event_time: u32,
    pub(crate) send_vote_time: u32,

    pub(crate) received_events_count_and_timing: HashMap<StatsEvent, (u16, u32)>,
    pub(crate) sent_votes: HashMap<VoteType, u16>,

    pub(crate) last_report_time: Instant,
}

impl Default for EventHandlerStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatsEvent {
    Block,
    BlockNotarized,
    FirstShred,
    ParentReady,
    TimeoutCrashedLeader,
    Timeout,
    SafeToNotar,
    SafeToSkip,
    ProduceWindow,
    Finalized,
    Standstill,
    SetIdentity,
}

impl StatsEvent {
    pub fn new(event: &VotorEvent) -> Self {
        match event {
            VotorEvent::Block(_) => StatsEvent::Block,
            VotorEvent::BlockNotarized(_) => StatsEvent::BlockNotarized,
            VotorEvent::FirstShred(_) => StatsEvent::FirstShred,
            VotorEvent::ParentReady { .. } => StatsEvent::ParentReady,
            VotorEvent::TimeoutCrashedLeader(_) => StatsEvent::TimeoutCrashedLeader,
            VotorEvent::Timeout(_) => StatsEvent::Timeout,
            VotorEvent::SafeToNotar(_) => StatsEvent::SafeToNotar,
            VotorEvent::SafeToSkip(_) => StatsEvent::SafeToSkip,
            VotorEvent::ProduceWindow(_) => StatsEvent::ProduceWindow,
            VotorEvent::Finalized(_) => StatsEvent::Finalized,
            VotorEvent::Standstill(_) => StatsEvent::Standstill,
            VotorEvent::SetIdentity => StatsEvent::SetIdentity,
        }
    }
}

impl EventHandlerStats {
    pub fn new() -> Self {
        Self {
            ignored: 0,
            leader_window_replaced: 0,
            set_root: 0,
            timeout_set: 0,
            receive_event_time: 0,
            send_vote_time: 0,
            received_events_count_and_timing: HashMap::new(),
            sent_votes: HashMap::new(),
            last_report_time: Instant::now(),
        }
    }

    pub fn incr_event_with_timing(&mut self, stats_event: StatsEvent, timing: u64) {
        let entry = self
            .received_events_count_and_timing
            .entry(stats_event)
            .or_insert((0, 0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(timing as u32);
    }

    pub fn incr_vote(&mut self, bls_op: &BLSOp) {
        if let BLSOp::PushVote { bls_message, .. } = bls_op {
            let BLSMessage::Vote(vote) = **bls_message else {
                warn!("Unexpected BLS message type: {:?}", bls_message);
                return;
            };
            let vote_type = VoteType::get_type(&vote.vote);
            let entry = self.sent_votes.entry(vote_type).or_insert(0);
            *entry = entry.saturating_add(1);
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
            ("ignored", self.ignored as i64, i64),
            (
                "leader_window_replaced",
                self.leader_window_replaced as i64,
                i64
            ),
            ("set_root", self.set_root as i64, i64),
            ("timeout_set", self.timeout_set as i64, i64),
        );
        for (event, (count, ms)) in &self.received_events_count_and_timing {
            datapoint_info!(
                "event_handler_received_event_count_and_timing",
                ("event", format!("{:?}", event), String),
                ("count", *count as i64, i64),
                ("elapsed", *ms as i64, i64)
            );
        }
        datapoint_info!(
            "event_handler_timing",
            ("receive_event_time", self.receive_event_time as i64, i64),
            ("send_vote_time", self.send_vote_time as i64, i64),
        );
        for (vote_type, count) in &self.sent_votes {
            datapoint_info!(
                "event_handler_sent_vote_count",
                ("vote", format!("{:?}", vote_type), String),
                ("count", *count as i64, i64)
            );
        }
        self.last_report_time = now;
        *self = EventHandlerStats::new();
    }
}
