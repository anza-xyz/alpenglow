//! Stats about the block creation loop

use {solana_clock::Slot, solana_metrics::datapoint_info, solana_time_utils::timestamp};

pub(super) struct BlockCreationLoopMetrics {
    last_report: u64,
    /// Counts number of times the block creation loop iterated.
    //
    // When stats are reset, this field is not reset.
    pub(super) loop_count: u64,
    pub(super) bank_timeout_completion_count: u64,
    pub(super) skipped_window_behind_parent_ready_count: u64,
    pub(super) window_production_elapsed: u64,
    pub(super) bank_timeout_completion_elapsed_hist: histogram::Histogram,
}

impl Default for BlockCreationLoopMetrics {
    fn default() -> Self {
        Self {
            loop_count: 0,
            bank_timeout_completion_count: 0,
            skipped_window_behind_parent_ready_count: 0,
            window_production_elapsed: 0,
            bank_timeout_completion_elapsed_hist: histogram::Histogram::default(),
            last_report: timestamp(),
        }
    }
}

impl BlockCreationLoopMetrics {
    fn is_empty(&self) -> bool {
        let Self {
            loop_count,
            bank_timeout_completion_count,
            skipped_window_behind_parent_ready_count,
            window_production_elapsed,
            bank_timeout_completion_elapsed_hist,
            last_report: _,
        } = self;
        0 == *loop_count
            + *bank_timeout_completion_count
            + *window_production_elapsed
            + *skipped_window_behind_parent_ready_count
            + bank_timeout_completion_elapsed_hist.entries()
    }

    fn reset(&mut self) {
        let current_loop_count = self.loop_count;
        *self = Self::default();
        self.loop_count = current_loop_count;
    }

    pub(super) fn report(&mut self, report_interval_ms: u64) {
        // skip reporting metrics if stats is empty
        if self.is_empty() {
            return;
        }
        let Self {
            loop_count,
            bank_timeout_completion_count,
            skipped_window_behind_parent_ready_count,
            window_production_elapsed,
            bank_timeout_completion_elapsed_hist,
            last_report,
        } = self;

        let now = timestamp();
        let elapsed_ms = now - *last_report;

        if elapsed_ms > report_interval_ms {
            datapoint_info!(
                "block-creation-loop-metrics",
                ("loop_count", *loop_count, i64),
                (
                    "bank_timeout_completion_count",
                    *bank_timeout_completion_count,
                    i64
                ),
                ("window_production_elapsed", *window_production_elapsed, i64),
                (
                    "skipped_window_behind_parent_ready_count",
                    *skipped_window_behind_parent_ready_count,
                    i64
                ),
                (
                    "bank_timeout_completion_elapsed_90pct",
                    bank_timeout_completion_elapsed_hist
                        .percentile(90.0)
                        .unwrap_or(0),
                    i64
                ),
                (
                    "bank_timeout_completion_elapsed_mean",
                    bank_timeout_completion_elapsed_hist.mean().unwrap_or(0),
                    i64
                ),
                (
                    "bank_timeout_completion_elapsed_min",
                    bank_timeout_completion_elapsed_hist.minimum().unwrap_or(0),
                    i64
                ),
                (
                    "bank_timeout_completion_elapsed_max",
                    bank_timeout_completion_elapsed_hist.maximum().unwrap_or(0),
                    i64
                ),
            );

            self.reset();
        }
    }
}

/// Metrics on slots that we attempt to start a leader block for
#[derive(Default)]
pub(super) struct SlotMetrics {
    pub(super) attempt_count: u64,
    pub(super) replay_is_behind_count: u64,
    pub(super) already_have_bank_count: u64,

    pub(super) slot_delay_hist: histogram::Histogram,
    pub(super) replay_is_behind_cumulative_wait_elapsed: u64,
    pub(super) replay_is_behind_wait_elapsed_hist: histogram::Histogram,
}

impl SlotMetrics {
    pub(super) fn report(&mut self, slot: Slot) {
        let Self {
            attempt_count,
            replay_is_behind_count,
            already_have_bank_count,
            slot_delay_hist,
            replay_is_behind_cumulative_wait_elapsed,
            replay_is_behind_wait_elapsed_hist,
        } = self;
        datapoint_info!(
            "slot-metrics",
            ("slot", slot, i64),
            ("attempt_count", *attempt_count, i64),
            ("replay_is_behind_count", *replay_is_behind_count, i64),
            ("already_have_bank_count", *already_have_bank_count, i64),
            (
                "slot_delay_90pct",
                slot_delay_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            ("slot_delay_mean", slot_delay_hist.mean().unwrap_or(0), i64),
            (
                "slot_delay_min",
                slot_delay_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "slot_delay_max",
                slot_delay_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "replay_is_behind_cumulative_wait_elapsed",
                *replay_is_behind_cumulative_wait_elapsed,
                i64
            ),
            (
                "replay_is_behind_wait_elapsed_90pct",
                replay_is_behind_wait_elapsed_hist
                    .percentile(90.0)
                    .unwrap_or(0),
                i64
            ),
            (
                "replay_is_behind_wait_elapsed_mean",
                replay_is_behind_wait_elapsed_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "replay_is_behind_wait_elapsed_min",
                replay_is_behind_wait_elapsed_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "replay_is_behind_wait_elapsed_max",
                replay_is_behind_wait_elapsed_hist.maximum().unwrap_or(0),
                i64
            ),
        );

        // reset metrics
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_loop_count_not_reset() {
        let mut metrics = BlockCreationLoopMetrics::default();
        assert_eq!(metrics.loop_count, 0);
        metrics.loop_count = 10;
        metrics.reset();
        assert_eq!(metrics.loop_count, 10);
    }
}
