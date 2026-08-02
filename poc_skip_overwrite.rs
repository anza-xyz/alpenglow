// PoC: SlotStakeCounters::add_vote overwrites skip_total instead of summing
// Run: rustc poc_skip_overwrite.rs && ./poc_skip_overwrite

struct SlotStakeCounters {
    skip_total: u64,
    notarize_total: u64,
    top_notarized_stake: u64,
    total_stake: u64,
    my_first_vote: Option<u8>,
}

impl SlotStakeCounters {
    fn new(total_stake: u64) -> Self {
        Self {
            skip_total: 0,
            notarize_total: 0,
            top_notarized_stake: 0,
            total_stake,
            my_first_vote: None,
        }
    }

    fn add_vote_buggy(&mut self, vote_type: u8, entry_stake: u64) {
        match vote_type {
            0 => {
                self.skip_total = entry_stake; // BUG: overwrites!
            }
            1 => {
                self.notarize_total = self.notarize_total.saturating_add(entry_stake);
                self.top_notarized_stake = self.top_notarized_stake.max(entry_stake);
            }
            _ => {}
        }
    }

    fn add_vote_fixed(&mut self, vote_type: u8, entry_stake: u64) {
        match vote_type {
            0 => {
                self.skip_total = self.skip_total.saturating_add(entry_stake); // FIX: sum
            }
            1 => {
                self.notarize_total = self.notarize_total.saturating_add(entry_stake);
                self.top_notarized_stake = self.top_notarized_stake.max(entry_stake);
            }
            _ => {}
        }
    }

    fn is_safe_to_skip(&self) -> bool {
        if self.my_first_vote != Some(0) { return false; }
        let num_stake = self.skip_total.saturating_add(self.notarize_total.saturating_sub(self.top_notarized_stake));
        num_stake * 100 / self.total_stake >= 40
    }
}

fn main() {
    println!("=== PoC: skip_total Overwrite Bug ===\n");
    let total_stake = 1000u64;

    println!("--- Buggy implementation ---");
    let mut buggy = SlotStakeCounters::new(total_stake);
    buggy.my_first_vote = Some(0);
    buggy.add_vote_buggy(0, 400);
    println!("After validator A (Skip, 400): skip_total = {}", buggy.skip_total);
    buggy.add_vote_buggy(0, 350);
    println!("After validator B (Skip, 350): skip_total = {} (OVERWROTE!)", buggy.skip_total);
    println!("Real skip support: 750 (75%), SafeToSkip: {} (BUG!)", buggy.is_safe_to_skip());

    println!("\n--- Fixed implementation ---");
    let mut fixed = SlotStakeCounters::new(total_stake);
    fixed.my_first_vote = Some(0);
    fixed.add_vote_fixed(0, 400);
    println!("After A (Skip, 400): skip_total = {}", fixed.skip_total);
    fixed.add_vote_fixed(0, 350);
    println!("After B (Skip, 350): skip_total = {}", fixed.skip_total);
    println!("SafeToSkip: {} (correct)", fixed.is_safe_to_skip());

    println!("\n=== IMPACT ===");
    println!("SafeToSkip never fires if smaller Skip arrives after larger one.");
    println!("Consensus stalls on slots that should be skipped.");
    println!("Root cause: Vote::Skip => self.skip_total = entry_stake (should be +=)");
}
