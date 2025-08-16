use {
    solana_metrics::datapoint_info,
    std::{
        sync::Mutex,
        time::{Duration, Instant},
    },
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

struct TimerManagerStatsInner {
    /// The maximum heap size of the timers since the last report
    max_heap_size: u16,
    /// The number of times `set_timeout` was called with a new timer.
    set_timeout_count: u16,
    /// The last time the stats were reported
    last_report: Instant,
}

impl TimerManagerStatsInner {
    fn new() -> Self {
        Self {
            max_heap_size: 0,
            set_timeout_count: 0,
            last_report: Instant::now(),
        }
    }
}

pub struct TimerManagerStats {
    inner: Mutex<TimerManagerStatsInner>,
}

impl TimerManagerStats {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TimerManagerStatsInner::new()),
        }
    }

    pub fn incr_timeout_count_with_heap_size(&self, size: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.set_timeout_count = inner.set_timeout_count.saturating_add(1);
        inner.max_heap_size = inner.max_heap_size.max(size as u16);
    }

    pub fn record_heap_size(&self, size: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.max_heap_size = inner.max_heap_size.max(size as u16);
    }

    pub fn maybe_report(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.last_report.elapsed() < STATS_REPORT_INTERVAL {
            return;
        }
        datapoint_info!(
            "votor_timer_manager",
            ("max_heap_size", inner.max_heap_size as i64, i64),
            ("set_timeout_count", inner.set_timeout_count as i64, i64),
        );
        *inner = TimerManagerStatsInner::new();
    }

    #[cfg(test)]
    pub fn get_numbers_for_tests(&self) -> (u16, u16) {
        let inner = self.inner.lock().unwrap();
        (inner.max_heap_size, inner.set_timeout_count)
    }
}
