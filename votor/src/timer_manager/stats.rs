use {
    solana_metrics::datapoint_info,
    std::{
        sync::Mutex,
        time::{Duration, Instant},
    },
};

const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

struct TimerManagerStatsInner {
    max_heap_size: u16,
    set_timeouts: u16,
    last_report: Instant,
}

impl TimerManagerStatsInner {
    pub fn new() -> Self {
        Self {
            max_heap_size: 0,
            set_timeouts: 0,
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

    pub fn set_timeout_with_heap_size(&self, size: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.set_timeouts = inner.set_timeouts.saturating_add(1);
        inner.max_heap_size = inner.max_heap_size.max(size as u16);
    }

    pub fn set_heap_size(&self, size: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.max_heap_size = inner.max_heap_size.max(size as u16);
    }

    pub fn maybe_report(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.last_report.elapsed() < STATS_REPORT_INTERVAL {
            return;
        }
        datapoint_info!(
            "timer_manager_stats",
            ("max_heap_size", inner.max_heap_size as i64, i64),
            ("set_timeouts", inner.set_timeouts as i64, i64),
        );
        *inner = TimerManagerStatsInner::new();
    }

    #[cfg(test)]
    pub fn get_numbers_for_tests(&self) -> (u16, u16) {
        let inner = self.inner.lock().unwrap();
        (inner.max_heap_size, inner.set_timeouts)
    }
}
