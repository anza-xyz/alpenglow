use {
    crate::event::VotorEvent,
    crossbeam_channel::Sender,
    solana_ledger::leader_schedule_utils::last_of_consecutive_leader_slots,
    solana_sdk::clock::Slot,
    std::{
        cmp::Reverse,
        collections::{BinaryHeap, HashMap},
        time::{Duration, Instant},
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
    fn new(slot: Slot, delta_timeout: Duration) -> (Self, Instant) {
        let mut window = (slot..=last_of_consecutive_leader_slots(slot)).collect::<Vec<_>>();
        assert!(!window.is_empty());
        window.reverse();
        let timeout = Instant::now().checked_add(delta_timeout).unwrap();
        (Self::WaitDeltaTimeout { window, timeout }, timeout)
    }

    /// Call to make progress on the state machine.
    ///
    /// Returns a potentially empty list of events that should be sent.
    fn progress(&mut self, delta_block: Duration) -> Option<VotorEvent> {
        let now = Instant::now();
        match self {
            Self::WaitDeltaTimeout { window, timeout } => {
                assert!(!window.is_empty());
                if &now < timeout {
                    return None;
                }
                let slot = *window.last().unwrap();
                let timeout = now.checked_add(delta_block).unwrap();
                *self = Self::WaitDeltaBlock {
                    window: window.to_owned(),
                    timeout,
                };
                Some(VotorEvent::TimeoutCrashedLeader(slot))
            }
            Self::WaitDeltaBlock { window, timeout } => {
                assert!(!window.is_empty());
                if &now < timeout {
                    return None;
                }

                let ret = Some(VotorEvent::Timeout(window.pop().unwrap()));
                if window.is_empty() {
                    *self = Self::Done;
                } else {
                    *timeout = now.checked_add(delta_block).unwrap();
                }
                ret
            }
            Self::Done => None,
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
    delta_timeout: Duration,
    delta_block: Duration,
    /// Timers are indexed by slots.
    timers: HashMap<Slot, TimerState>,
    /// A min heap based on the time the next timer state might be ready.
    heap: BinaryHeap<Reverse<(Instant, Slot)>>,
    /// Channel to send events on.
    event_sender: Sender<VotorEvent>,
}

impl Timers {
    pub(super) fn new(
        delta_timeout: Duration,
        delta_block: Duration,
        event_sender: Sender<VotorEvent>,
    ) -> Self {
        Self {
            delta_timeout,
            delta_block,
            timers: HashMap::new(),
            heap: BinaryHeap::new(),
            event_sender,
        }
    }

    /// Call to set timeouts for a new window of slots.
    pub(super) fn set_timeouts(&mut self, slot: Slot) {
        assert_eq!(self.heap.len(), self.timers.len());
        let (timer, next_fire) = TimerState::new(slot, self.delta_timeout);
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
                    if now < next_fire {
                        ret_timeout = Some(match ret_timeout {
                            None => next_fire,
                            Some(r) => std::cmp::min(r, next_fire),
                        });
                        self.heap.push(Reverse((next_fire, slot)));
                        break;
                    }

                    let mut timer = self.timers.remove(&slot).unwrap();
                    if let Some(event) = timer.progress(self.delta_block) {
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

#[cfg(test)]
mod tests {
    use {super::*, crossbeam_channel::unbounded, std::thread};

    #[test]
    fn timer_state_machine() {
        let slot = 0;
        let (mut timer_state, next_fire) = TimerState::new(slot, Duration::from_micros(1));

        let next_fire = next_fire.duration_since(Instant::now());
        let delta_block = Duration::from_micros(1);

        thread::sleep(next_fire);
        assert!(matches!(
            timer_state.progress(delta_block).unwrap(),
            VotorEvent::TimeoutCrashedLeader(0)
        ));

        thread::sleep(next_fire);
        assert!(matches!(
            timer_state.progress(delta_block).unwrap(),
            VotorEvent::Timeout(0)
        ));

        thread::sleep(next_fire);
        assert!(matches!(
            timer_state.progress(delta_block).unwrap(),
            VotorEvent::Timeout(1)
        ));

        thread::sleep(next_fire);
        assert!(matches!(
            timer_state.progress(delta_block).unwrap(),
            VotorEvent::Timeout(2)
        ));

        thread::sleep(next_fire);
        assert!(matches!(
            timer_state.progress(delta_block).unwrap(),
            VotorEvent::Timeout(3)
        ));
        assert!(timer_state.next_fire().is_none());
    }

    #[test]
    fn timers_progress() {
        // Ideally we could set this to a high value and test what would happen when
        // the timer thread sleeps for long enough and we get only a subset of
        // events.  However, that introduces flakiness in CI.  We could consider
        // using a mock timer instead in the future.
        let duration = Duration::from_nanos(1);
        let (sender, receiver) = unbounded();
        let mut timers = Timers::new(duration, duration, sender);
        assert!(timers.progress().is_none());
        assert!(receiver.try_recv().unwrap_err().is_empty());

        timers.set_timeouts(0);
        while timers.progress().is_some() {
            thread::sleep(duration);
        }
        let mut events = receiver.try_iter().collect::<Vec<_>>();

        assert!(matches!(
            events.remove(0),
            VotorEvent::TimeoutCrashedLeader(0)
        ));
        assert!(matches!(events.remove(0), VotorEvent::Timeout(0)));
        assert!(matches!(events.remove(0), VotorEvent::Timeout(1)));
        assert!(matches!(events.remove(0), VotorEvent::Timeout(2)));
        assert!(matches!(events.remove(0), VotorEvent::Timeout(3)));
        assert!(events.is_empty());
    }
}
