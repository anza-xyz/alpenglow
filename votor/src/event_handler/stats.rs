use {
    crate::{event::VotorEvent, voting_utils::BLSOp, VoteType},
    solana_clock::Slot,
    solana_metrics::datapoint_info,
    solana_votor_messages::bls_message::BLSMessage,
    std::{
        collections::{BTreeMap, HashMap},
        time::{Duration, Instant},
    },
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct SlotTracking {
    start: Instant,
    first_shred: Option<Instant>,
    parent_ready: Option<Instant>,
    vote_notarize: Option<Instant>,
    vote_skip: Option<Instant>,
    finalized: Option<Instant>,
    is_fast_finalization: bool,
}

impl Default for SlotTracking {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            first_shred: None,
            parent_ready: None,
            vote_notarize: None,
            vote_skip: None,
            finalized: None,
            is_fast_finalization: false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct EventHandlerStats {
    pub(crate) ignored: u16,
    pub(crate) leader_window_replaced: u16,
    pub(crate) set_root: u16,
    pub(crate) timeout_set: u16,

    /// All of the following fields are in microseconds
    pub(crate) receive_event_time: u32,
    pub(crate) send_vote_time: u32,

    received_events_count_and_timing: HashMap<StatsEvent, (u16, u32)>,
    sent_votes: HashMap<VoteType, u16>,

    slot_tracking_map: BTreeMap<Slot, SlotTracking>,

    highest_parent_ready: Slot,
    last_report_time: Instant,
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
            VotorEvent::Finalized(..) => StatsEvent::Finalized,
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
            slot_tracking_map: BTreeMap::new(),
            highest_parent_ready: 0,
            last_report_time: Instant::now(),
        }
    }

    pub fn handle_event_arrival(&mut self, event: &VotorEvent) -> StatsEvent {
        match event {
            VotorEvent::FirstShred(slot) => {
                let entry = self.slot_tracking_map.entry(*slot).or_default();
                entry.first_shred = Some(Instant::now());
            }
            VotorEvent::ParentReady { slot, .. } => {
                let entry = self.slot_tracking_map.entry(*slot).or_default();
                entry.parent_ready = Some(Instant::now());
                if *slot > self.highest_parent_ready {
                    self.highest_parent_ready = *slot;
                }
            }
            VotorEvent::Finalized((slot, _), is_fast_finalization) => {
                let entry = self.slot_tracking_map.entry(*slot).or_default();
                if entry.finalized.is_none() {
                    entry.finalized = Some(Instant::now());
                }
                // We can accept Notarize and FastFinalization, never set the flag from true to false
                if *is_fast_finalization {
                    entry.is_fast_finalization = true;
                }
            }
            _ => (),
        }
        StatsEvent::new(event)
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
            if vote_type == VoteType::Notarize {
                let entry = self.slot_tracking_map.entry(vote.vote.slot()).or_default();
                entry.vote_notarize = Some(Instant::now());
            } else if vote_type == VoteType::Skip {
                let entry = self.slot_tracking_map.entry(vote.vote.slot()).or_default();
                entry.vote_skip = Some(Instant::now());
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
        // Only report if the slot is lower than highest_parent_ready
        let split_off_map = self.slot_tracking_map.split_off(&self.highest_parent_ready);
        for (slot, tracking) in &self.slot_tracking_map {
            let start = tracking.start;
            datapoint_info!(
                "event_handler_slot_tracking",
                ("slot", *slot as i64, i64),
                (
                    "first_shred",
                    tracking.first_shred.map(|t| {
                        t.saturating_duration_since(start)
                            .as_micros()
                            .min(i64::MAX as u128) as i64
                    }),
                    Option<i64>
                ),
                (
                    "parent_ready",
                    tracking.parent_ready.map(|t| {
                        t.saturating_duration_since(start)
                            .as_micros()
                            .min(i64::MAX as u128) as i64
                    }),
                    Option<i64>
                ),
                (
                    "vote_notarize",
                    tracking.vote_notarize.map(|t| {
                        t.saturating_duration_since(start)
                            .as_micros()
                            .min(i64::MAX as u128) as i64
                    }),
                    Option<i64>
                ),
                (
                    "vote_skip",
                    tracking.vote_skip.map(|t| {
                        t.saturating_duration_since(start)
                            .as_micros()
                            .min(i64::MAX as u128) as i64
                    }),
                    Option<i64>
                ),
                (
                    "finalized",
                    tracking.finalized.map(|t| {
                        t.saturating_duration_since(start)
                            .as_micros()
                            .min(i64::MAX as u128) as i64
                    }),
                    Option<i64>
                ),
                ("is_fast_finalization", tracking.finalized.map(|_| tracking.is_fast_finalization), Option<bool>)
            );
        }
        self.last_report_time = now;
        *self = EventHandlerStats::new();
        self.slot_tracking_map = split_off_map;
    }
}
