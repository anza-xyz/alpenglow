use {
    crate::{event::VotorEvent, DELTA_BLOCK, DELTA_TIMEOUT},
    crossbeam_channel::Sender,
    solana_ledger::leader_schedule_utils::last_of_consecutive_leader_slots,
    solana_sdk::clock::Slot,
    std::{
        cmp::Reverse,
        collections::{BinaryHeap, HashMap},
        time::Instant,
    },
};

/// Encodes a basic state machine of the different stages involved in handling
/// timeouts for a window of slots.
enum TimerState {
    /// Waiting for the DELTA_TIMEOUT stage.
    WaitDeltaTimeout {
        /// The slots in the window.  Must not be empty.
        window: Vec<Slot>,
        /// Time when this stage will end.
        timeout: Instant,
    },
    /// Waiting for the DELTA_BLOCK stage.
    WaitDeltaBlock {
        /// The slots in the window.  Must not be empty.
        window: Vec<Slot>,
        /// Time when this stage will end.
        timeout: Instant,
    },
    /// The state machine is done.
    Done,
}

impl TimerState {
    /// Creates a new instance of the state machine.
    ///
    /// Also returns the next time the timer should fire.
    fn new(slot: Slot) -> (Self, Instant) {
        let mut window = (slot..last_of_consecutive_leader_slots(slot)).collect::<Vec<_>>();
        assert!(!window.is_empty());
        window.reverse();
        let timeout = Instant::now().checked_add(DELTA_TIMEOUT).unwrap();
        (Self::WaitDeltaTimeout { window, timeout }, timeout)
    }

    /// Call to make progress on the state machine.
    ///
    /// Returns a potentially empty list of events that should be sent.
    fn progress(&mut self) -> Vec<VotorEvent> {
        let now = Instant::now();
        match self {
            Self::WaitDeltaTimeout { window, timeout } => {
                if &now < timeout {
                    return vec![];
                }
                let slot = *window.last().unwrap();
                let timeout = now.checked_add(DELTA_BLOCK).unwrap();
                *self = Self::WaitDeltaBlock {
                    window: window.to_owned(),
                    timeout,
                };
                vec![VotorEvent::TimeoutCrashedLeader(slot)]
            }
            Self::WaitDeltaBlock { window, timeout } => {
                if &now < timeout {
                    return vec![];
                }

                let mut events = Vec::with_capacity(2);
                let slot = window.pop().unwrap();
                events.push(VotorEvent::Timeout(slot));
                if window.is_empty() {
                    *self = Self::Done;
                    return events;
                }
                events.push(VotorEvent::TimeoutCrashedLeader(slot));
                *timeout = now.checked_add(DELTA_BLOCK).unwrap();
                events
            }
            Self::Done => vec![],
        }
    }

    /// When would this state machine next be able to make progress.
    fn next_fire(&self) -> Option<Instant> {
        match self {
            Self::WaitDeltaTimeout { window: _, timeout } => Some(*timeout),
            Self::WaitDeltaBlock { window: _, timeout } => Some(*timeout),
            Self::Done => None,
        }
    }
}

/// Maintains all active timer states for windows of slots.
pub(super) struct Timers {
    /// Timers are indexed by slots.
    timers: HashMap<Slot, TimerState>,
    /// A min heap based on the time the next timer state might be ready.
    heap: BinaryHeap<Reverse<(Instant, Slot)>>,
    /// Channel to send events on.
    event_sender: Sender<VotorEvent>,
}

impl Timers {
    pub(super) fn new(event_sender: Sender<VotorEvent>) -> Self {
        Self {
            timers: HashMap::new(),
            heap: BinaryHeap::new(),
            event_sender,
        }
    }

    /// Call to set timeouts for a new window of slots.
    pub(super) fn set_timeouts(&mut self, slot: Slot) {
        assert_eq!(self.heap.len(), self.timers.len());
        let (timer, next_fire) = TimerState::new(slot);
        // It is possible that this slot already has a timer set e.g. if there
        // are multiple ParentReady for the same slot.  Do not insert new timer then.
        self.timers.entry(slot).or_insert_with(|| {
            self.heap.push(Reverse((next_fire, slot)));
            timer
        });
    }

    /// Call to make progress on the timer states.  If there are still active
    /// timer states, returns when the earliest one might become ready.
    pub(super) fn progress(&mut self) -> Option<Instant> {
        assert_eq!(self.heap.len(), self.timers.len());
        let mut ret_timeout = None;
        let now = Instant::now();
        loop {
            assert_eq!(self.heap.len(), self.timers.len());
            match self.heap.pop() {
                None => break,
                Some(Reverse((next_fire, slot))) => {
                    if next_fire < now {
                        self.heap.push(Reverse((next_fire, slot)));
                        break;
                    }

                    let mut timer = self.timers.remove(&slot).unwrap();
                    for event in timer.progress() {
                        self.event_sender.send(event).unwrap();
                    }
                    if let Some(next_fire) = timer.next_fire() {
                        self.heap.push(Reverse((next_fire, slot)));
                        assert!(self.timers.insert(slot, timer).is_none());
                        ret_timeout = Some(match ret_timeout {
                            None => next_fire,
                            Some(r) => std::cmp::min(r, next_fire),
                        });
                    }
                }
            }
        }
        ret_timeout
    }
}
