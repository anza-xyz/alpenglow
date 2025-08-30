use {
    histogram::Histogram,
    solana_pubkey::Pubkey,
    solana_votor_messages::vote::Vote,
    std::{collections::BTreeMap, time::Duration},
};

#[derive(Debug, Clone)]
struct Metric {
    count: u64,
    histogram: Histogram,
}

#[derive(Debug)]
struct NodeVoteMetrics {
    notar: Metric,
    notar_fallback: Metric,
    skip: Metric,
    skip_fallback: Metric,
    final_: Metric,
}

impl NodeVoteMetrics {
    fn new() -> Self {
        // TODO: pick sensible parameters for the buckets
        let histogram = Histogram::new(10, 10).unwrap();
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

    fn record_vote(&mut self, vote: &Vote, duration: Duration) {
        match vote {
            Vote::Notarize(_) => {
                self.notar
                    .histogram
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.notar.count = self.notar.count.saturating_add(1);
            }
            Vote::NotarizeFallback(_) => {
                self.notar_fallback
                    .histogram
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.notar_fallback.count = self.notar_fallback.count.saturating_add(1);
            }
            Vote::Skip(_) => {
                self.skip
                    .histogram
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.skip.count = self.skip.count.saturating_add(1);
            }
            Vote::SkipFallback(_) => {
                self.skip_fallback
                    .histogram
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.skip_fallback.count = self.skip_fallback.count.saturating_add(1);
            }
            Vote::Finalize(_) => {
                self.final_
                    .histogram
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.final_.count = self.final_.count.saturating_add(1);
            }
        }
    }
}

#[derive(Default)]
pub struct VoteMetrics {
    node_metrics: BTreeMap<Pubkey, NodeVoteMetrics>,
}

impl VoteMetrics {
    /// Called from consensus to record metrics for a given node.
    ///
    /// - id is the id of the sender of the vote.
    /// - vote is the type of vote the sender sent.
    /// - duration is the time difference from the start of the block timeout to when the vote was received.
    pub fn record_vote(&mut self, id: Pubkey, vote: &Vote, duration: Duration) {
        let node = self
            .node_metrics
            .entry(id)
            .or_insert(NodeVoteMetrics::new());
        node.record_vote(vote, duration);
    }

    pub fn end_of_epoch_reporting(&mut self) {
        unimplemented!()
    }
}

pub struct LeaderMetrics(BTreeMap<Pubkey, Histogram>);

impl LeaderMetrics {
    /// Called to record the duration of when the block hash was first seen when `leader` was the validator responsible for producing the block.
    ///
    /// The block hash might be first seen if the node collecting the statistics itself managed to reconstruct the block or when it received a vote that referenced the block.
    ///
    /// duration is the time difference from the start of the block timeout to when the block hash was first seen.
    pub fn record_block_hash_seen(&mut self, leader: Pubkey, duration: Duration) {
        // TODO: pick sensible parameters for the buckets
        let histogram = self
            .0
            .entry(leader)
            .or_insert(Histogram::new(10, 10).unwrap());
        histogram.increment(duration.as_micros() as u64).unwrap();
    }

    pub fn end_of_epoch_reporting(&mut self) {
        unimplemented!()
    }
}
