use {
    histogram::Histogram,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_votor_messages::vote::Vote,
    std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    },
};

/// Tracks metrics for a single [`Vote`]
#[derive(Debug, Clone)]
struct Metric {
    count: u64,
    histogram: Histogram,
}

/// Tracks all [`Vote`] metrics for a given node.
#[derive(Debug)]
struct NodeVoteMetrics {
    notar: Metric,
    notar_fallback: Metric,
    skip: Metric,
    skip_fallback: Metric,
    final_: Metric,
}

impl Default for NodeVoteMetrics {
    fn default() -> Self {
        let histogram = Histogram::new(7, 20).unwrap();
        let metric = Metric {
            count: 0,
            histogram,
        };
        Self {
            notar: metric.clone(),
            notar_fallback: metric.clone(),
            skip: metric.clone(),
            skip_fallback: metric.clone(),
            final_: metric,
        }
    }
}

impl NodeVoteMetrics {
    /// Records metrics for when `vote` was received after `elapsed` time has passed since the start of the slot.
    fn record_vote(&mut self, vote: &Vote, elapsed: Duration) {
        match vote {
            Vote::Notarize(_) => {
                self.notar
                    .histogram
                    .increment(elapsed.as_micros() as u64)
                    .unwrap();
                self.notar.count = self.notar.count.saturating_add(1);
            }
            Vote::NotarizeFallback(_) => {
                self.notar_fallback
                    .histogram
                    .increment(elapsed.as_micros() as u64)
                    .unwrap();
                self.notar_fallback.count = self.notar_fallback.count.saturating_add(1);
            }
            Vote::Skip(_) => {
                self.skip
                    .histogram
                    .increment(elapsed.as_micros() as u64)
                    .unwrap();
                self.skip.count = self.skip.count.saturating_add(1);
            }
            Vote::SkipFallback(_) => {
                self.skip_fallback
                    .histogram
                    .increment(elapsed.as_micros() as u64)
                    .unwrap();
                self.skip_fallback.count = self.skip_fallback.count.saturating_add(1);
            }
            Vote::Finalize(_) => {
                self.final_
                    .histogram
                    .increment(elapsed.as_micros() as u64)
                    .unwrap();
                self.final_.count = self.final_.count.saturating_add(1);
            }
        }
    }
}

/// Errors returned from [`AgMetrics::record_vote`].
#[derive(Debug)]
pub enum RecordVoteError {
    /// Could not find start of slot entry.
    SlotNotFound,
}

/// Errors returned from [`AgMetrics::record_block_hash_seen`].
#[derive(Debug)]
pub enum RecordBlockHashError {
    /// Could not find start of slot entry.
    SlotNotFound,
}

/// Tracks various Alpenglow related metrics.
pub struct AgMetrics {
    /// Used to track this node's view of how the other nodes on the network are voting.
    node_metrics: BTreeMap<Pubkey, NodeVoteMetrics>,
    /// Used to track when this node received blocks from different leaders in the network.
    leader_metrics: BTreeMap<Pubkey, Histogram>,
    /// Tracks when individual slots began.
    start_of_slot: BTreeMap<Slot, Instant>,
    /// Tracks the current epoch, used for end of epoch reporting.
    current_epoch: Epoch,
}

impl AgMetrics {
    pub fn new(epoch: Epoch) -> Self {
        Self {
            node_metrics: BTreeMap::default(),
            leader_metrics: BTreeMap::default(),
            start_of_slot: BTreeMap::default(),
            current_epoch: epoch,
        }
    }

    /// Records a `vote` from the node with `id`.
    pub fn record_vote(&mut self, id: Pubkey, vote: &Vote) -> Result<(), RecordVoteError> {
        let Some(start) = self.start_of_slot.get(&vote.slot()) else {
            return Err(RecordVoteError::SlotNotFound);
        };
        let node = self.node_metrics.entry(id).or_default();
        let elapsed = start.elapsed();
        node.record_vote(vote, elapsed);
        Ok(())
    }

    /// Records when a block for `slot` was seen and the `leader` is responsible for producing it.
    pub fn record_block_hash_seen(
        &mut self,
        leader: Pubkey,
        slot: Slot,
    ) -> Result<(), RecordBlockHashError> {
        let Some(start) = self.start_of_slot.get(&slot) else {
            return Err(RecordBlockHashError::SlotNotFound);
        };
        let elapsed = start.elapsed();
        let histogram = self
            .leader_metrics
            .entry(leader)
            .or_insert(Histogram::new(7, 20).unwrap());
        histogram.increment(elapsed.as_micros() as u64).unwrap();
        Ok(())
    }

    /// Records when a given slot started.
    pub fn record_start_of_slot(&mut self, slot: Slot) {
        self.start_of_slot.entry(slot).or_insert(Instant::now());
    }

    /// Performs end of epoch reporting and reset all the statistics for the subsequent epoch.
    fn end_of_epoch_reporting(&mut self) {
        // TODO: currently, just clearing the stats and not actually reporting
        self.node_metrics.clear();
        self.node_metrics.clear();
        self.start_of_slot.clear();
    }

    /// This function can be called if there is a new [`Epoch`] and it will carry out end of epoch reporting.
    pub fn maybe_new_epoch(&mut self, epoch: Epoch) {
        assert!(epoch >= self.current_epoch);
        if epoch != self.current_epoch {
            self.current_epoch = epoch;
            self.end_of_epoch_reporting();
        }
    }
}
