use {
    histogram::Histogram,
    solana_pubkey::Pubkey,
    solana_votor_messages::vote::Vote,
    std::{collections::BTreeMap, time::Duration},
};

#[derive(Debug)]
struct VoteMetrics {
    notar: (u64, Histogram),
    notar_fallback: (u64, Histogram),
    skip: (u64, Histogram),
    skip_fallback: (u64, Histogram),
    final_: (u64, Histogram),
}

impl VoteMetrics {
    fn new() -> Self {
        // TODO: pick sensible parameters for the buckets
        let histogram = Histogram::new(10, 10).unwrap();
        Self {
            notar: (0, histogram.clone()),
            notar_fallback: (0, histogram.clone()),
            skip: (0, histogram.clone()),
            skip_fallback: (0, histogram.clone()),
            final_: (0, histogram),
        }
    }

    fn record_vote(&mut self, vote: &Vote, duration: Duration) {
        match vote {
            Vote::Notarize(_) => {
                self.notar.1.increment(duration.as_micros() as u64).unwrap();
                self.notar.0 = self.notar.0.saturating_add(1);
            }
            Vote::NotarizeFallback(_) => {
                self.notar_fallback
                    .1
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.notar_fallback.0 = self.notar_fallback.0.saturating_add(1);
            }
            Vote::Skip(_) => {
                self.skip.1.increment(duration.as_micros() as u64).unwrap();
                self.skip.0 = self.skip.0.saturating_add(1);
            }
            Vote::SkipFallback(_) => {
                self.skip_fallback
                    .1
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.skip_fallback.0 = self.skip_fallback.0.saturating_add(1);
            }
            Vote::Finalize(_) => {
                self.final_
                    .1
                    .increment(duration.as_micros() as u64)
                    .unwrap();
                self.final_.0 = self.final_.0.saturating_add(1);
            }
        }
    }
}

/// The global metrics datastructure.
///
/// Each node has one of these.
#[derive(Default)]
pub struct AlpenglowMetrics {
    /// Stores the node's view of how the nodes are voting.
    vote_metrics: BTreeMap<Pubkey, VoteMetrics>,
    /// Stores the node's view of how the nodes are producing blocks.
    leader_metrics: BTreeMap<Pubkey, Histogram>,
}

impl AlpenglowMetrics {
    /// Called from consensus to record metrics for a given node.
    ///
    /// - id is the id of the sender of the vote.
    /// - vote is the type of vote the sender sent.
    /// - duration is the time difference from the start of the block timeout to when the vote was received.
    pub fn record_vote(&mut self, id: Pubkey, vote: &Vote, duration: Duration) {
        let node = self.vote_metrics.entry(id).or_insert(VoteMetrics::new());
        node.record_vote(vote, duration);
    }

    /// Called to record the duration of when the block hash was first seen when `leader` was the validator responsible for producing the block.
    ///
    /// The block hash might be first seen if the node collecting the statistics itself managed to reconstruct the block or when it received a vote that referenced the block.
    ///
    /// duration is the time difference from the start of the block timeout to when the block hash was first seen.
    pub fn record_block_hash_seen(&mut self, leader: Pubkey, duration: Duration) {
        // TODO: pick sensible parameters for the buckets
        let histogram = self
            .leader_metrics
            .entry(leader)
            .or_insert(Histogram::new(10, 10).unwrap());
        histogram.increment(duration.as_micros() as u64).unwrap();
    }

    pub fn end_of_epoch_reporting(&mut self) {
        // Report all the metrics to the collector.

        // Reset the statistics.

        for (validator, vote_metrics) in self.vote_metrics.iter() {
            println!("{:?} {:?}", validator, vote_metrics);
        }

        for (leader, histogram) in self.leader_metrics.iter() {
            println!("{:?} {:?}", leader, histogram);
        }

        unimplemented!()
    }
}
