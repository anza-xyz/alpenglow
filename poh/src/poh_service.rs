//! The `poh_service` module implements a service that records the passing of
//! "ticks", a measure of time in the PoH stream
use {
    crate::poh_recorder::{PohRecorder, Record},
    crossbeam_channel::Receiver,
    log::*,
    solana_clock::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_entry::poh::Poh,
    solana_ledger::leader_schedule_utils::{
        first_of_consecutive_leader_slots, last_of_consecutive_leader_slots, leader_slot_index,
    },
    solana_measure::{measure::Measure, measure_us},
    solana_poh_config::PohConfig,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub struct PohService {
    tick_producer: JoinHandle<()>,
}

// Number of hashes to batch together.
// * If this number is too small, PoH hash rate will suffer.
// * The larger this number is from 1, the speed of recording transactions will suffer due to lock
//   contention with the PoH hashing within `tick_producer()`.
//
// Can use test_poh_service to calibrate this
pub const DEFAULT_HASHES_PER_BATCH: u64 = 64;

pub const DEFAULT_PINNED_CPU_CORE: usize = 0;

const TARGET_SLOT_ADJUSTMENT_NS: u64 = 50_000_000;

/// Alpenglow block constants
/// The amount of time a leader has to build their block
pub const BLOCKTIME: Duration = Duration::from_millis(400);

/// The maximum message delay
pub const DELTA: Duration = Duration::from_millis(100);

/// The maximum delay a node can observe between entering the loop iteration
/// for a window and receiving any shred of the first block of the leader.
/// As a conservative global constant we set this to 3 * DELTA
pub const DELTA_TIMEOUT: Duration = DELTA.saturating_mul(3);

/// The timeout in ms for the leader block index within the leader window
#[inline]
pub fn skip_timeout(leader_block_index: usize) -> Duration {
    DELTA_TIMEOUT + (leader_block_index as u32 + 1) * BLOCKTIME + DELTA
}

/// Block timeout, when we should publish the final shred for the leader block index
/// within the leader window
#[inline]
pub fn block_timeout(leader_block_index: usize) -> Duration {
    // TODO: What should be a reasonable buffer for this?
    // Release the final shred `DELTA`ms before the skip timeout
    skip_timeout(leader_block_index).saturating_sub(DELTA)
}

#[derive(Debug)]
struct PohTiming {
    num_ticks: u64,
    num_hashes: u64,
    total_sleep_us: u64,
    total_lock_time_ns: u64,
    total_hash_time_ns: u64,
    total_tick_time_ns: u64,
    last_metric: Instant,
    total_record_time_us: u64,
    total_send_record_result_us: u64,
}

impl PohTiming {
    fn new() -> Self {
        Self {
            num_ticks: 0,
            num_hashes: 0,
            total_sleep_us: 0,
            total_lock_time_ns: 0,
            total_hash_time_ns: 0,
            total_tick_time_ns: 0,
            last_metric: Instant::now(),
            total_record_time_us: 0,
            total_send_record_result_us: 0,
        }
    }
    fn report(&mut self, ticks_per_slot: u64) {
        if self.last_metric.elapsed().as_millis() > 1000 {
            let elapsed_us = self.last_metric.elapsed().as_micros() as u64;
            let us_per_slot = (elapsed_us * ticks_per_slot) / self.num_ticks;
            datapoint_info!(
                "poh-service",
                ("ticks", self.num_ticks as i64, i64),
                ("hashes", self.num_hashes as i64, i64),
                ("elapsed_us", us_per_slot, i64),
                ("total_sleep_us", self.total_sleep_us, i64),
                ("total_tick_time_us", self.total_tick_time_ns / 1000, i64),
                ("total_lock_time_us", self.total_lock_time_ns / 1000, i64),
                ("total_hash_time_us", self.total_hash_time_ns / 1000, i64),
                ("total_record_time_us", self.total_record_time_us, i64),
                (
                    "total_send_record_result_us",
                    self.total_send_record_result_us,
                    i64
                ),
            );
            self.total_sleep_us = 0;
            self.num_ticks = 0;
            self.num_hashes = 0;
            self.total_tick_time_ns = 0;
            self.total_lock_time_ns = 0;
            self.total_hash_time_ns = 0;
            self.last_metric = Instant::now();
            self.total_record_time_us = 0;
            self.total_send_record_result_us = 0;
        }
    }
}

impl PohService {
    pub fn new(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_config: &PohConfig,
        poh_exit: Arc<AtomicBool>,
        ticks_per_slot: u64,
        pinned_cpu_core: usize,
        hashes_per_batch: u64,
        record_receiver: Receiver<Record>,
        track_transaction_indexes: bool,
    ) -> Self {
        let poh_config = poh_config.clone();
        let is_alpenglow_enabled = poh_recorder.read().unwrap().is_alpenglow_enabled;
        let tick_producer = Builder::new()
            .name("solPohTickProd".to_string())
            .spawn(move || {
                if !is_alpenglow_enabled {
                    if poh_config.hashes_per_tick.is_none() {
                        if poh_config.target_tick_count.is_none() {
                            Self::low_power_tick_producer(
                                poh_recorder.clone(),
                                &poh_config,
                                &poh_exit,
                                record_receiver.clone(),
                            );
                        } else {
                            Self::short_lived_low_power_tick_producer(
                                poh_recorder.clone(),
                                &poh_config,
                                &poh_exit,
                                record_receiver.clone(),
                            );
                        }
                    } else {
                        // PoH service runs in a tight loop, generating hashes as fast as possible.
                        // Let's dedicate one of the CPU cores to this thread so that it can gain
                        // from cache performance.
                        if let Some(cores) = core_affinity::get_core_ids() {
                            core_affinity::set_for_current(cores[pinned_cpu_core]);
                        }
                        Self::tick_producer(
                            poh_recorder.clone(),
                            &poh_exit,
                            ticks_per_slot,
                            hashes_per_batch,
                            record_receiver.clone(),
                            Self::target_ns_per_tick(
                                ticks_per_slot,
                                poh_config.target_tick_duration.as_nanos() as u64,
                            ),
                        );
                    }

                    // Migrate to alpenglow PoH
                    if !poh_exit.load(Ordering::Relaxed)
                    // Should be set by replay_stage after it sees a notarized
                    // block in the new alpenglow epoch
                    && poh_recorder.read().unwrap().is_alpenglow_enabled
                    {
                        info!("Migrating poh service to alpenglow tick producer");
                    } else {
                        poh_exit.store(true, Ordering::Relaxed);
                        return;
                    }
                }

                // Start alpenglow
                //
                // Important this is called *before* any new alpenglow
                // leaders call `set_bank()`, otherwise, the old PoH
                // tick producer will still tick in that alpenglow bank
                //
                // TODO: Can essentailly replace this with no ticks
                // once we properly remove poh/entry verification in replay
                {
                    let mut w_poh_recorder = poh_recorder.write().unwrap();
                    w_poh_recorder.migrate_to_alpenglow_poh();
                    w_poh_recorder.use_alpenglow_tick_producer = true;
                }
                Self::alpenglow_tick_producer(
                    poh_recorder,
                    &poh_exit,
                    bank_forks.as_ref(),
                    record_receiver,
                    track_transaction_indexes,
                );
                poh_exit.store(true, Ordering::Relaxed);
            })
            .unwrap();

        Self { tick_producer }
    }

    pub fn target_ns_per_tick(ticks_per_slot: u64, target_tick_duration_ns: u64) -> u64 {
        // Account for some extra time outside of PoH generation to account
        // for processing time outside PoH.
        let adjustment_per_tick = if ticks_per_slot > 0 {
            TARGET_SLOT_ADJUSTMENT_NS / ticks_per_slot
        } else {
            0
        };
        target_tick_duration_ns.saturating_sub(adjustment_per_tick)
    }

    /// The block creation loop. Although this lives in `poh_service` out of convinience, it does
    /// not use poh but rather a timer to produce blocks.
    ///
    /// The `alpenglow_consensus::voting_loop` tracks when it is our leader window, and populates
    /// an empty bank with certificates for the first block in the window. This loop takes on the
    /// remaining responsibility of clearing this block when complete or the timeout is reached,
    /// and creating the remaining three blocks on time.
    fn alpenglow_tick_producer(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        poh_exit: &AtomicBool,
        bank_forks: &RwLock<BankForks>,
        record_receiver: Receiver<Record>,
        track_transaction_indexes: bool,
    ) {
        // TODO: at some point we can move this to alpenglow_consensus
        info!("starting alpenglow tick producer");
        let leader_bank_notifier = poh_recorder.read().unwrap().new_leader_bank_notifier();
        let mut preempted_bank_and_timer: Option<(Arc<Bank>, Instant)> = None;

        loop {
            let (mut current_bank, skip_timer) = if let Some((bank, timer)) =
                preempted_bank_and_timer
            {
                // If our previous leader window was preempted, immediately start this new window
                preempted_bank_and_timer = None;
                (bank, timer)
            } else {
                // Wait for the first leader bank in the window to be set by the voting loop
                loop {
                    if poh_exit.load(Ordering::Relaxed) {
                        return;
                    }
                    // Ensure that we process any leftover records from previous banks
                    // to avoid clogging the channel
                    Self::read_record_receiver_and_process(
                        &poh_recorder,
                        &record_receiver,
                        // Don't block
                        Duration::from_millis(1),
                    );

                    // Wait for first leader bank to be set
                    let (leader_bank, skip_timer) = leader_bank_notifier
                        .get_or_wait_for_in_progress_with_timer(Duration::from_millis(50));

                    let Some(leader_bank) = leader_bank.upgrade() else {
                        continue;
                    };

                    let slot = leader_bank.slot();
                    trace!(
                        "{}: observed first leader bank for {}, skip_timer {}ms",
                        leader_bank.collector_id(),
                        slot,
                        skip_timer.elapsed().as_millis()
                    );
                    // Slot should be the first in the leader window with an exception for slot 1 as slot 0 is genesis
                    // TODO: we allow 2 here as well since GCE tests WFSM on 1, fix this when we actually incorporate WFSM
                    if slot % NUM_CONSECUTIVE_LEADER_SLOTS != 0 && slot != 1 && slot != 2 {
                        // TODO: Test hotswap failover in the middle of a leader window
                        panic!(
                            "Attempting to produce {slot} without producing {}. Something has gone wrong.",
                            first_of_consecutive_leader_slots(slot)
                        );
                    };
                    break (leader_bank, skip_timer);
                }
            };

            // Produce `NUM_CONSECUTIVE_LEADER_SLOTS` built off each other
            let mut leader_slot_index = leader_slot_index(current_bank.slot());
            loop {
                if poh_exit.load(Ordering::Relaxed) {
                    return;
                }

                // Calculate the block timeout
                let current_timeout = block_timeout(leader_slot_index);
                let mut remaining_slot_time = current_timeout;

                // Wait for block to finish or timeout
                // TODO: Add a test where we don't hit block timeout, but instead the block is completely full
                // and verify that this works.
                while !remaining_slot_time.is_zero()
                    && leader_bank_notifier.get_current_bank_id() == Some(current_bank.bank_id())
                {
                    remaining_slot_time = current_timeout.saturating_sub(skip_timer.elapsed());
                    trace!(
                        "{}: waiting for leader bank {} to finish, remaining time: {}",
                        current_bank.collector_id(),
                        current_bank.slot(),
                        remaining_slot_time.as_millis(),
                    );

                    // Process records
                    Self::read_record_receiver_and_process(
                        &poh_recorder,
                        &record_receiver,
                        remaining_slot_time,
                    );
                }

                // There are three possibilities here that we have to handle:
                // (1) We hit the block time out before the bank reached the max tick height.
                //      `current_bank` is still present in `poh_recorder`
                // (2) The bank has reached the max tick height, and has been cleared.
                //      No bank is present in `poh_recorder`
                // (3) We were too slow or experienced network issues and the entire window has
                //      been SkipCertified. We are also the leader for the next window and the
                //      `current_bank` has been cleared from `poh_recorder`, and the voting loop
                //      has inserted the first leader bank for the next window.
                //      In this case `poh_recorder.preempted_slot` will have been set
                //
                //  We hold the poh recorder write lock until the new bank is inserted
                let mut w_poh_recorder = poh_recorder.write().unwrap();
                match (w_poh_recorder.bank(), w_poh_recorder.preempted_slot.take()) {
                    (None, None) => {
                        trace!(
                            "{}: {} reached max tick height, moving to next block",
                            current_bank.collector_id(),
                            current_bank.slot()
                        );
                        // (2), Nothing to do we insert the next bank below
                    }
                    (Some(bank), None) => {
                        // (1) we must clear the `current_bank` which also publishes the last shred
                        // and finishes the block broadcast.
                        assert_eq!(bank.slot(), current_bank.slot());
                        trace!(
                            "{}: current bank {} has reached block timeout, ticking",
                            current_bank.collector_id(),
                            current_bank.slot()
                        );
                        let max_tick_height = current_bank.max_tick_height();
                        // Set the tick height for the bank to max_tick_height - 1, so that PohRecorder::flush_cache()
                        // will properly increment the tick_height to max_tick_height.
                        current_bank.set_tick_height(max_tick_height - 1);
                        // Write the single tick for this slot
                        // TODO: handle migration slot because we need to provide the PoH
                        // for slots from the previous epoch, but `tick_alpenglow()` will
                        // delete those ticks from the cache
                        w_poh_recorder.tick_alpenglow(max_tick_height);
                        assert!(!w_poh_recorder.has_bank());
                    }
                    (Some(bank), Some(preempted_slot)) => {
                        let slot = bank.slot();
                        assert_eq!(slot, preempted_slot);
                        assert_ne!(slot, current_bank.slot());
                        // (3), do not continue producing this leader window
                        warn!(
                            "We were too slow while producing block {} and have already reached our next leader window for slot {}. \
                            Abandoning further production of blocks in this window production in favor of window {}-{}",
                            current_bank.slot(),
                            slot,
                            first_of_consecutive_leader_slots(slot),
                            last_of_consecutive_leader_slots(slot)
                        );
                        // On the next iteration we will immediately start from this new leader window
                        preempted_bank_and_timer =
                            Some((bank, leader_bank_notifier.get_skip_timer()));
                        break;
                    }
                    (None, Some(preempted_slot)) => {
                        panic!(
                        "Programmer error: while building block {} we have been preempted by {preempted_slot},
                            however there is no working bank",
                            current_bank.slot(),
                        );
                    }
                };

                // Create the next bank if we have not reached the end
                leader_slot_index += 1;
                if leader_slot_index == NUM_CONSECUTIVE_LEADER_SLOTS as usize {
                    trace!(
                        "{}: Finished leader window, last slot {}",
                        current_bank.collector_id(),
                        current_bank.slot()
                    );
                    // Finished window
                    break;
                }

                // Create a new bank, note that set-identity is not allowed within a leader window
                trace!(
                    "{}: Creating new child bank {}",
                    current_bank.collector_id(),
                    current_bank.slot() + 1
                );
                // TODO: notify rpc / banking tracer integration
                let child_bank = Bank::new_from_parent(
                    current_bank.clone(),
                    current_bank.collector_id(),
                    current_bank.slot() + 1,
                );

                // Insert the child_bank and update the current bank
                let child_bank = bank_forks.write().unwrap().insert(child_bank);
                current_bank = child_bank.clone_without_scheduler();
                let poh_bank_start = w_poh_recorder.set_bank(
                    child_bank,
                    track_transaction_indexes,
                    // No need to set the skip timer since we already have it for this window
                    None,
                );

                // No certificates needed, banking_stage can start immediately
                poh_bank_start
                    .contains_valid_certificate
                    .store(true, Ordering::Relaxed);
            }
        }
    }

    fn low_power_tick_producer(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        poh_config: &PohConfig,
        poh_exit: &AtomicBool,
        record_receiver: Receiver<Record>,
    ) {
        let mut last_tick = Instant::now();
        while !poh_exit.load(Ordering::Relaxed) {
            let remaining_tick_time = poh_config
                .target_tick_duration
                .saturating_sub(last_tick.elapsed());
            Self::read_record_receiver_and_process(
                &poh_recorder,
                &record_receiver,
                remaining_tick_time,
            );
            if remaining_tick_time.is_zero() {
                last_tick = Instant::now();
                let mut w_poh_recorder = poh_recorder.write().unwrap();
                w_poh_recorder.tick();
                if w_poh_recorder.is_alpenglow_enabled {
                    info!("exiting tick_producer because alpenglow enabled");
                    break;
                }
            }
        }
    }

    pub fn read_record_receiver_and_process(
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        record_receiver: &Receiver<Record>,
        timeout: Duration,
    ) {
        let record = record_receiver.recv_timeout(timeout);
        if let Ok(record) = record {
            if record
                .sender
                .send(poh_recorder.write().unwrap().record(
                    record.slot,
                    record.mixin,
                    record.transactions,
                ))
                .is_err()
            {
                panic!("Error returning mixin hash");
            }
        }
    }

    fn short_lived_low_power_tick_producer(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        poh_config: &PohConfig,
        poh_exit: &AtomicBool,
        record_receiver: Receiver<Record>,
    ) {
        let mut warned = false;
        let mut elapsed_ticks = 0;
        let mut last_tick = Instant::now();
        let num_ticks = poh_config.target_tick_count.unwrap();
        while elapsed_ticks < num_ticks {
            let remaining_tick_time = poh_config
                .target_tick_duration
                .saturating_sub(last_tick.elapsed());
            Self::read_record_receiver_and_process(
                &poh_recorder,
                &record_receiver,
                Duration::from_millis(0),
            );
            if remaining_tick_time.is_zero() {
                last_tick = Instant::now();
                poh_recorder.write().unwrap().tick();
                elapsed_ticks += 1;
            }
            if poh_exit.load(Ordering::Relaxed) && !warned {
                warned = true;
                warn!("exit signal is ignored because PohService is scheduled to exit soon");
            }
        }
    }

    // returns true if we need to tick
    fn record_or_hash(
        next_record: &mut Option<Record>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        timing: &mut PohTiming,
        record_receiver: &Receiver<Record>,
        hashes_per_batch: u64,
        poh: &Arc<Mutex<Poh>>,
        target_ns_per_tick: u64,
    ) -> bool {
        match next_record.take() {
            Some(mut record) => {
                // received message to record
                // so, record for as long as we have queued up record requests
                let mut lock_time = Measure::start("lock");
                let mut poh_recorder_l = poh_recorder.write().unwrap();
                lock_time.stop();
                timing.total_lock_time_ns += lock_time.as_ns();
                let mut record_time = Measure::start("record");
                loop {
                    let res = poh_recorder_l.record(
                        record.slot,
                        record.mixin,
                        std::mem::take(&mut record.transactions),
                    );
                    let (send_res, send_record_result_us) = measure_us!(record.sender.send(res));
                    debug_assert!(send_res.is_ok(), "Record wasn't sent.");

                    timing.total_send_record_result_us += send_record_result_us;
                    timing.num_hashes += 1; // note: may have also ticked inside record
                    if let Ok(new_record) = record_receiver.try_recv() {
                        // we already have second request to record, so record again while we still have the mutex
                        record = new_record;
                    } else {
                        break;
                    }
                }
                record_time.stop();
                timing.total_record_time_us += record_time.as_us();
                // PohRecorder.record would have ticked if it needed to, so should_tick will be false
            }
            None => {
                // did not receive instructions to record, so hash until we notice we've been asked to record (or we need to tick) and then remember what to record
                let mut lock_time = Measure::start("lock");
                let mut poh_l = poh.lock().unwrap();
                lock_time.stop();
                timing.total_lock_time_ns += lock_time.as_ns();
                loop {
                    timing.num_hashes += hashes_per_batch;
                    let mut hash_time = Measure::start("hash");
                    let should_tick = poh_l.hash(hashes_per_batch);
                    let ideal_time = poh_l.target_poh_time(target_ns_per_tick);
                    hash_time.stop();
                    timing.total_hash_time_ns += hash_time.as_ns();
                    if should_tick {
                        // nothing else can be done. tick required.
                        return true;
                    }
                    // check to see if a record request has been sent
                    if let Ok(record) = record_receiver.try_recv() {
                        // remember the record we just received as the next record to occur
                        *next_record = Some(record);
                        break;
                    }
                    // check to see if we need to wait to catch up to ideal
                    let wait_start = Instant::now();
                    if ideal_time <= wait_start {
                        // no, keep hashing. We still hold the lock.
                        continue;
                    }

                    // busy wait, polling for new records and after dropping poh lock (reset can occur, for example)
                    drop(poh_l);
                    while ideal_time > Instant::now() {
                        // check to see if a record request has been sent
                        if let Ok(record) = record_receiver.try_recv() {
                            // remember the record we just received as the next record to occur
                            *next_record = Some(record);
                            break;
                        }
                    }
                    timing.total_sleep_us += wait_start.elapsed().as_micros() as u64;
                    break;
                }
            }
        };
        false // should_tick = false for all code that reaches here
    }

    fn tick_producer(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        poh_exit: &AtomicBool,
        ticks_per_slot: u64,
        hashes_per_batch: u64,
        record_receiver: Receiver<Record>,
        target_ns_per_tick: u64,
    ) {
        let poh = poh_recorder.read().unwrap().poh.clone();
        let mut timing = PohTiming::new();
        let mut next_record = None;
        loop {
            let should_tick = Self::record_or_hash(
                &mut next_record,
                &poh_recorder,
                &mut timing,
                &record_receiver,
                hashes_per_batch,
                &poh,
                target_ns_per_tick,
            );
            if should_tick {
                // Lock PohRecorder only for the final hash. record_or_hash will lock PohRecorder for record calls but not for hashing.
                {
                    let mut lock_time = Measure::start("lock");
                    let mut poh_recorder_l = poh_recorder.write().unwrap();
                    lock_time.stop();
                    timing.total_lock_time_ns += lock_time.as_ns();
                    if poh_recorder_l.is_alpenglow_enabled {
                        info!("exiting tick_producer because alpenglow enabled");
                        break;
                    }
                    let mut tick_time = Measure::start("tick");
                    poh_recorder_l.tick();
                    tick_time.stop();
                    timing.total_tick_time_ns += tick_time.as_ns();
                }
                timing.num_ticks += 1;

                timing.report(ticks_per_slot);
                if poh_exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.tick_producer.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::{thread_rng, Rng},
        solana_clock::DEFAULT_HASHES_PER_TICK,
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path_auto_delete,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_measure::measure::Measure,
        solana_perf::test_tx::test_tx,
        solana_runtime::bank::Bank,
        solana_sha256_hasher::hash,
        solana_transaction::versioned::VersionedTransaction,
        std::{thread::sleep, time::Duration},
    };

    #[test]
    #[ignore]
    fn test_poh_service() {
        solana_logger::setup();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let prev_hash = bank.last_blockhash();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");

        let default_target_tick_duration =
            PohConfig::default().target_tick_duration.as_micros() as u64;
        let target_tick_duration = Duration::from_micros(default_target_tick_duration);
        let poh_config = PohConfig {
            hashes_per_tick: Some(DEFAULT_HASHES_PER_TICK),
            target_tick_duration,
            target_tick_count: None,
        };
        let exit = Arc::new(AtomicBool::new(false));

        let ticks_per_slot = bank.ticks_per_slot();
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
        let blockstore = Arc::new(blockstore);
        let (poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
            bank.tick_height(),
            prev_hash,
            bank.clone(),
            Some((4, 4)),
            ticks_per_slot,
            blockstore,
            &leader_schedule_cache,
            &poh_config,
            exit.clone(),
        );
        let poh_recorder = Arc::new(RwLock::new(poh_recorder));
        let ticks_per_slot = bank.ticks_per_slot();
        let bank_slot = bank.slot();

        // specify RUN_TIME to run in a benchmark-like mode
        // to calibrate batch size
        let run_time = std::env::var("RUN_TIME")
            .map(|x| x.parse().unwrap())
            .unwrap_or(0);
        let is_test_run = run_time == 0;

        let entry_producer = {
            let poh_recorder = poh_recorder.clone();
            let exit = exit.clone();

            Builder::new()
                .name("solPohEntryProd".to_string())
                .spawn(move || {
                    let now = Instant::now();
                    let mut total_us = 0;
                    let mut total_times = 0;
                    let h1 = hash(b"hello world!");
                    let tx = VersionedTransaction::from(test_tx());
                    loop {
                        // send some data
                        let mut time = Measure::start("record");
                        let _ =
                            poh_recorder
                                .write()
                                .unwrap()
                                .record(bank_slot, h1, vec![tx.clone()]);
                        time.stop();
                        total_us += time.as_us();
                        total_times += 1;
                        if is_test_run && thread_rng().gen_ratio(1, 4) {
                            sleep(Duration::from_millis(200));
                        }

                        if exit.load(Ordering::Relaxed) {
                            info!(
                                "spent:{}ms record: {}ms entries recorded: {}",
                                now.elapsed().as_millis(),
                                total_us / 1000,
                                total_times,
                            );
                            break;
                        }
                    }
                })
                .unwrap()
        };

        let hashes_per_batch = std::env::var("HASHES_PER_BATCH")
            .map(|x| x.parse().unwrap())
            .unwrap_or(DEFAULT_HASHES_PER_BATCH);
        let poh_service = PohService::new(
            poh_recorder.clone(),
            bank_forks.clone(),
            &poh_config,
            exit.clone(),
            0,
            DEFAULT_PINNED_CPU_CORE,
            hashes_per_batch,
            record_receiver,
            false,
        );
        poh_recorder.write().unwrap().set_bank_for_test(bank);

        // get some events
        let mut hashes = 0;
        let mut need_tick = true;
        let mut need_entry = true;
        let mut need_partial = true;
        let mut num_ticks = 0;

        let time = Instant::now();
        while run_time != 0 || need_tick || need_entry || need_partial {
            let (_bank, (entry, _tick_height)) = entry_receiver.recv().unwrap();

            if entry.is_tick() {
                num_ticks += 1;
                assert!(
                    entry.num_hashes <= poh_config.hashes_per_tick.unwrap(),
                    "{} <= {}",
                    entry.num_hashes,
                    poh_config.hashes_per_tick.unwrap()
                );

                if entry.num_hashes == poh_config.hashes_per_tick.unwrap() {
                    need_tick = false;
                } else {
                    need_partial = false;
                }

                hashes += entry.num_hashes;

                assert_eq!(hashes, poh_config.hashes_per_tick.unwrap());

                hashes = 0;
            } else {
                assert!(entry.num_hashes >= 1);
                need_entry = false;
                hashes += entry.num_hashes;
            }

            if run_time != 0 {
                if time.elapsed().as_millis() > run_time {
                    break;
                }
            } else {
                assert!(
                    time.elapsed().as_secs() < 60,
                    "Test should not run for this long! {}s tick {} entry {} partial {}",
                    time.elapsed().as_secs(),
                    need_tick,
                    need_entry,
                    need_partial,
                );
            }
        }
        info!(
            "target_tick_duration: {} ticks_per_slot: {}",
            poh_config.target_tick_duration.as_nanos(),
            ticks_per_slot
        );
        let elapsed = time.elapsed();
        info!(
            "{} ticks in {}ms {}us/tick",
            num_ticks,
            elapsed.as_millis(),
            elapsed.as_micros() / num_ticks
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        entry_producer.join().unwrap();
    }
}
