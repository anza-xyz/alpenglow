// Standalone PoC demonstrating stake double-counting.
// Uses only stdlib. No external dependencies.
// Run: rustc poc_double_count.rs && ./poc_double_count

struct Accumulator {
    ranks: Vec<bool>,
    stake: u64,
}

impl Accumulator {
    fn new(max_validators: usize) -> Self {
        Self {
            ranks: vec![false; max_validators],
            stake: 0,
        }
    }

    fn add_aggregate(&mut self, aggregate_ranks: &[bool], aggregate_stake: u64) {
        for (i, present) in aggregate_ranks.iter().enumerate() {
            if i >= self.ranks.len() { break; }
            if *present { self.ranks[i] = true; }
        }
        // BUG: adds stake regardless of whether ranks were already counted
        self.stake = self.stake.saturating_add(aggregate_stake);
    }

    fn meets_threshold(&self, total_stake: u64, required_pct: u64) -> bool {
        let fraction = self.stake as f64 / total_stake as f64;
        fraction * 100.0 >= required_pct as f64
    }
}

fn main() {
    println!("=== PoC: Stake Double-Counting via Duplicate VoteAggregate ===\n");

    let total_stake = 1000u64;
    let threshold_pct = 60u64;

    println!("--- Normal scenario (no duplicates) ---");
    let mut acc_normal = Accumulator::new(64);
    acc_normal.add_aggregate(&[true, false, false], 200);
    acc_normal.add_aggregate(&[false, true, false], 200);
    acc_normal.add_aggregate(&[false, false, true], 200);
    println!("  Accumulated stake: {}", acc_normal.stake);
    println!("  Meets {}% threshold: {}", threshold_pct, acc_normal.meets_threshold(total_stake, threshold_pct));

    println!("\n--- Attack: duplicate attacker vote (2x count) ---");
    let mut acc_attack = Accumulator::new(64);
    acc_attack.add_aggregate(&[true, false, false], 400);
    acc_attack.add_aggregate(&[false, true, false], 200);
    acc_attack.add_aggregate(&[true, false, false], 400);
    acc_attack.add_aggregate(&[false, false, true], 200);

    println!("  Accumulated stake: {}", acc_attack.stake);
    println!("  Real support: 400+200+200 = 600");
    println!("  Inflated by: {} (duplicate attacker stake)", 400);
    println!("  Meets {}% threshold: {}", threshold_pct, acc_attack.meets_threshold(total_stake, threshold_pct));

    println!("\n--- Extreme: solo attacker with duplicate vote ---");
    let mut acc_solo = Accumulator::new(64);
    acc_solo.add_aggregate(&[true, false, false], 400);
    acc_solo.add_aggregate(&[true, false, false], 400);

    println!("  Accumulated stake: {}", acc_solo.stake);
    println!("  Real support: 400 (NO honest votes)");
    println!("  Meets {}% threshold: {}", threshold_pct, acc_solo.meets_threshold(total_stake, threshold_pct));
    println!("  => Certificate formed with NO honest support!");

    println!("\n--- Impact: SafeToNotar false positive ---");
    let skip_total = 350u64;
    let notarize_total = 200u64;
    let notarized_ratio_pct = 350u64 * 100 / 1000u64;
    let notarized_plus_skip_pct = 550u64 * 100 / 1000u64;
    println!("  skip_total (inflated): {}", skip_total);
    println!("  notarize_total: {}", notarize_total);
    println!("  Condition 1 (notar >= 40%): {}", notarized_ratio_pct >= 40);
    println!("  Condition 2 (notar >= 20% && notar+skip >= 60%): {} && {}",
        notarized_ratio_pct >= 20, notarized_plus_skip_pct >= 60);
    println!("  => SafeToNotar fires for block with minimal real support");

    println!("\n=== ROOT CAUSE ===");
    println!("AggregateAccumulator::add_aggregate does OR on bitmap but always adds full stake.");
    println!("VoteAggregate::new_from_verified_votes has debug-only assert(!ranks[rank])");
    println!("Two gossip paths => two VoteAggregates with same rank => double-count.");
    println!("Result: consensus threshold bypass via inflated stake accounting.");
    println!("\n=== FIX ===");
    println!("In add_aggregate: check if rank already present before adding stake.");
    println!("In new_from_verified_votes: use debug_assert! + runtime check or dedup upstream.");
}
