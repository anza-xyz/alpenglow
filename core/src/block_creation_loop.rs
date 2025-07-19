//! The Alpenglow block creation loop
//! When our leader window is reached, attempts to create our leader blocks
//! within the block timeouts. Responsible for inserting empty banks for
//! banking stage to fill, and clearing banks once the timeout has been reached.
use {
    crate::{
        banking_trace::BankingTracer,
        replay_stage::{Finalizer, ReplayStage},
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::leader_slot_index,
    },
    solana_measure::measure::Measure,
    solana_metrics::datapoint_info,
    solana_poh::poh_recorder::{PohRecorder, Record, GRACE_TICKS_FACTOR, MAX_GRACE_SLOTS},
    solana_rpc::{rpc_subscriptions::RpcSubscriptions, slot_status_notifier::SlotStatusNotifier},
    solana_runtime::{
        bank::{Bank, NewBankOptions},
        bank_forks::BankForks,
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey, timing::AtomicInterval},
    solana_votor::{block_timeout, event::LeaderWindowInfo, voting_loop::LeaderWindowNotifier},
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        thread,
        time::{Duration, Instant},
    },
    thiserror::Error,
};

pub struct BlockCreationLoopConfig {
    pub exit: Arc<AtomicBool>,
    pub track_transaction_indexes: bool,

    // Shared state
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub blockstore: Arc<Blockstore>,
    pub cluster_info: Arc<ClusterInfo>,
    pub poh_recorder: Arc<RwLock<PohRecorder>>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub rpc_subscriptions: Arc<RpcSubscriptions>,

    // Notifiers
    pub banking_tracer: Arc<BankingTracer>,
    pub slot_status_notifier: Option<SlotStatusNotifier>,

    // Receivers / notifications from banking stage / replay / voting loop
    pub record_receiver: Receiver<Record>,
    pub leader_window_notifier: Arc<LeaderWindowNotifier>,
    pub replay_highest_frozen: Arc<ReplayHighestFrozen>,
}

struct LeaderContext {
    my_pubkey: Pubkey,
    blockstore: Arc<Blockstore>,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    bank_forks: Arc<RwLock<BankForks>>,
    rpc_subscriptions: Arc<RpcSubscriptions>,
    slot_status_notifier: Option<SlotStatusNotifier>,
    banking_tracer: Arc<BankingTracer>,
    track_transaction_indexes: bool,
    replay_highest_frozen: Arc<ReplayHighestFrozen>,
}

#[derive(Default)]
pub struct ReplayHighestFrozen {
    pub highest_frozen_slot: Mutex<Slot>,
    pub freeze_notification: Condvar,
}

#[derive(Default)]
struct BlockCreationLoopMetrics {
    last_report: AtomicInterval,
    loop_count: AtomicUsize,
    replay_is_behind_count: AtomicUsize,
    startup_verification_incomplete_count: AtomicUsize,
    already_have_bank_count: AtomicUsize,
    bank_timeout_completion_count: AtomicUsize,
    bank_filled_completion_count: AtomicUsize,

    replay_is_behind_wait_elapsed: AtomicU64,
    window_production_elapsed: AtomicU64,
    slot_production_elapsed_hist: histogram::Histogram,
    bank_completion_elapsed_hist: histogram::Histogram,
}

impl BlockCreationLoopMetrics {
    fn is_empty(&self) -> bool {
        0 == self.loop_count.load(Ordering::Relaxed) as u64
            + self.replay_is_behind_count.load(Ordering::Relaxed) as u64
            + self
                .startup_verification_incomplete_count
                .load(Ordering::Relaxed) as u64
            + self.already_have_bank_count.load(Ordering::Relaxed) as u64
            + self.bank_timeout_completion_count.load(Ordering::Relaxed) as u64
            + self.bank_filled_completion_count.load(Ordering::Relaxed) as u64
            + self.replay_is_behind_wait_elapsed.load(Ordering::Relaxed) as u64
            + self.window_production_elapsed.load(Ordering::Relaxed) as u64
            + self.slot_production_elapsed_hist.entries() as u64
            + self.bank_completion_elapsed_hist.entries() as u64
    }

    fn report(&mut self, report_interval_ms: u64) {
        // skip reporting metrics if stats is empty
        if self.is_empty() {
            return;
        }

        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "block-creation-loop-metrics",
                ("loop_count", self.loop_count.load(Ordering::Relaxed), i64),
                (
                    "replay_is_behind_count",
                    self.replay_is_behind_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "startup_verification_incomplete_count",
                    self.startup_verification_incomplete_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "already_have_bank_count",
                    self.already_have_bank_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "bank_timeout_completion_count",
                    self.bank_timeout_completion_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "bank_filled_completion_count",
                    self.bank_filled_completion_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "replay_is_behind_wait_elapsed",
                    self.replay_is_behind_wait_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "window_production_elapsed",
                    self.window_production_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "slot_production_elapsed_90pct",
                    self.slot_production_elapsed_hist
                        .percentile(90.0)
                        .unwrap_or(0),
                    i64
                ),
                (
                    "slot_production_elapsed_mean",
                    self.slot_production_elapsed_hist.mean().unwrap_or(0),
                    i64
                ),
                (
                    "slot_production_elapsed_min",
                    self.slot_production_elapsed_hist.minimum().unwrap_or(0),
                    i64
                ),
                (
                    "slot_production_elapsed_max",
                    self.slot_production_elapsed_hist.maximum().unwrap_or(0),
                    i64
                ),
                (
                    "bank_completion_elapsed_90pct",
                    self.bank_completion_elapsed_hist
                        .percentile(90.0)
                        .unwrap_or(0),
                    i64
                ),
                (
                    "bank_completion_elapsed_mean",
                    self.bank_completion_elapsed_hist.mean().unwrap_or(0),
                    i64
                ),
                (
                    "bank_completion_elapsed_min",
                    self.bank_completion_elapsed_hist.minimum().unwrap_or(0),
                    i64
                ),
                (
                    "bank_completion_elapsed_max",
                    self.bank_completion_elapsed_hist.maximum().unwrap_or(0),
                    i64
                ),
            );
        }
    }
}

#[derive(Debug, Error)]
enum StartLeaderError {
    /// Replay has not yet frozen the parent slot
    #[error("Replay is behind for parent slot {0}")]
    ReplayIsBehind(/* parent slot */ Slot),

    /// Startup verification is not yet complete
    #[error("Startup verification is incomplete on parent bank {0}")]
    StartupVerificationIncomplete(/* parent slot */ Slot),

    /// Bank forks already contains bank
    #[error("Already contain bank for leader slot {0}")]
    AlreadyHaveBank(/* leader slot */ Slot),

    /// Haven't landed a vote
    #[error("Have not rooted a block with our vote")]
    VoteNotRooted,
}

fn start_receive_and_record_loop(
    exit: Arc<AtomicBool>,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    record_receiver: Receiver<Record>,
) {
    while !exit.load(Ordering::Relaxed) {
        // We need a timeout here to check the exit flag, chose 400ms
        // for now but can be longer if needed.
        match record_receiver.recv_timeout(Duration::from_millis(400)) {
            Ok(record) => {
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
            Err(RecvTimeoutError::Disconnected) => {
                info!("Record receiver disconnected");
                return;
            }
            Err(RecvTimeoutError::Timeout) => (),
        }
    }
}

/// The block creation loop.
///
/// The `alpenglow_consensus::voting_loop` tracks when it is our leader window, and populates
/// communicates the skip timer and parent slot for our window. This loop takes the responsibility
/// of creating our `NUM_CONSECUTIVE_LEADER_SLOTS` blocks and finishing them within the required timeout.
pub fn start_loop(config: BlockCreationLoopConfig) {
    let BlockCreationLoopConfig {
        exit,
        track_transaction_indexes,
        bank_forks,
        blockstore,
        cluster_info,
        poh_recorder,
        leader_schedule_cache,
        rpc_subscriptions,
        banking_tracer,
        slot_status_notifier,
        leader_window_notifier,
        replay_highest_frozen,
        record_receiver,
    } = config;

    // Similar to the voting loop, if this loop dies kill the validator
    let _exit = Finalizer::new(exit.clone());

    // get latest identity pubkey during startup
    let mut my_pubkey = cluster_info.id();
    let leader_bank_notifier = poh_recorder.read().unwrap().new_leader_bank_notifier();

    let mut ctx = LeaderContext {
        my_pubkey,
        blockstore,
        poh_recorder: poh_recorder.clone(),
        leader_schedule_cache,
        bank_forks,
        rpc_subscriptions,
        slot_status_notifier,
        banking_tracer,
        track_transaction_indexes,
        replay_highest_frozen,
    };

    let mut metrics = BlockCreationLoopMetrics::default();

    // Setup poh
    reset_poh_recorder(&ctx.bank_forks.read().unwrap().working_bank(), &ctx);

    // Start receive and record loop
    let exit_c = exit.clone();
    let p_rec = poh_recorder.clone();
    let receive_record_loop = thread::spawn(move || {
        start_receive_and_record_loop(exit_c, p_rec, record_receiver);
    });

    while !exit.load(Ordering::Relaxed) {
        // Check if set-identity was called at each leader window start
        if my_pubkey != cluster_info.id() {
            // set-identity cli has been called during runtime
            let my_old_pubkey = my_pubkey;
            my_pubkey = cluster_info.id();
            ctx.my_pubkey = my_pubkey;

            warn!(
                "Identity changed from {} to {} during block creation loop",
                my_old_pubkey, my_pubkey
            );
        }

        // Wait for the voting loop to notify us
        let LeaderWindowInfo {
            start_slot,
            end_slot,
            // TODO: handle duplicate blocks by using the hash here
            parent_block: (parent_slot, _, _),
            skip_timer,
        } = {
            let window_info = leader_window_notifier.window_info.lock().unwrap();
            let (mut guard, timeout_res) = leader_window_notifier
                .window_notification
                .wait_timeout_while(window_info, Duration::from_secs(1), |wi| wi.is_none())
                .unwrap();
            if timeout_res.timed_out() {
                continue;
            }
            guard.take().unwrap()
        };

        trace!(
            "Received window notification for {start_slot} to {end_slot} \
            parent: {parent_slot}"
        );

        assert!(
            first_in_leader_window(start_slot),
            "{start_slot} was not first in leader window but voting loop notified us"
        );
        if let Err(e) =
            start_leader_retry_replay(start_slot, parent_slot, skip_timer, &ctx, &mut metrics)
        {
            // Give up on this leader window
            error!(
                "{my_pubkey}: Unable to produce first slot {start_slot}, skipping production of our entire leader window \
                {start_slot}-{end_slot}: {e:?}"
            );
            continue;
        }

        // Produce our window
        let mut window_production_start = Measure::start("window_production");
        let mut slot = start_slot;
        // TODO(ashwin): Handle preemption of leader window during this loop
        while !exit.load(Ordering::Relaxed) {
            let leader_index = leader_slot_index(slot);
            let timeout = block_timeout(leader_index);

            // Wait for either the block timeout or for the bank to be completed
            // The receive and record loop will fill the bank
            let remaining_slot_time = timeout.saturating_sub(skip_timer.elapsed());
            trace!(
                "{my_pubkey}: waiting for leader bank {slot} to finish, remaining time: {}",
                remaining_slot_time.as_millis(),
            );

            // Start measuring bank completion time
            let mut bank_completion_measure = Measure::start("bank_completion");

            leader_bank_notifier.wait_for_completed(remaining_slot_time);

            // Time to complete the bank, there are two possibilities:
            // (1) We hit the block timeout, the bank is still present we must clear it
            // (2) The bank has filled up and been cleared by banking stage
            {
                let mut w_poh_recorder = poh_recorder.write().unwrap();
                if let Some(bank) = w_poh_recorder.bank() {
                    assert_eq!(bank.slot(), slot);
                    trace!(
                        "{}: bank {} has reached block timeout, ticking",
                        bank.collector_id(),
                        bank.slot()
                    );

                    // Record timeout completion metric
                    metrics
                        .bank_timeout_completion_count
                        .fetch_add(1, Ordering::Relaxed);

                    let max_tick_height = bank.max_tick_height();
                    // Set the tick height for the bank to max_tick_height - 1, so that PohRecorder::flush_cache()
                    // will properly increment the tick_height to max_tick_height.
                    bank.set_tick_height(max_tick_height - 1);
                    // Write the single tick for this slot
                    // TODO: handle migration slot because we need to provide the PoH
                    // for slots from the previous epoch, but `tick_alpenglow()` will
                    // delete those ticks from the cache
                    drop(bank);
                    w_poh_recorder.tick_alpenglow(max_tick_height);
                } else {
                    trace!("{my_pubkey}: {slot} reached max tick height, moving to next block");

                    // Record filled completion metric
                    metrics
                        .bank_filled_completion_count
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            // Assert that the bank has been cleared
            assert!(!poh_recorder.read().unwrap().has_bank());

            // Record bank completion time
            bank_completion_measure.stop();
            let _ = metrics
                .bank_completion_elapsed_hist
                .increment(bank_completion_measure.as_us());

            // Produce our next slot
            slot += 1;
            if slot > end_slot {
                trace!("{my_pubkey}: finished leader window {start_slot}-{end_slot}");
                break;
            }
            let mut slot_production_start = Measure::start("slot_production");

            // Although `slot - 1`has been cleared from `poh_recorder`, it might not have finished processing in
            // `replay_stage`, which is why we use `start_leader_retry_replay`
            if let Err(e) =
                start_leader_retry_replay(slot, slot - 1, skip_timer, &ctx, &mut metrics)
            {
                error!("{my_pubkey}: Unable to produce {slot}, skipping rest of leader window {slot} - {end_slot}: {e:?}");
                break;
            }

            // Record individual slot production time
            slot_production_start.stop();
            let _ = metrics
                .slot_production_elapsed_hist
                .increment(slot_production_start.as_us());
        }
        window_production_start.stop();
        metrics
            .window_production_elapsed
            .fetch_add(window_production_start.as_us(), Ordering::Relaxed);
        metrics.loop_count.fetch_add(1, Ordering::Relaxed);
        metrics.report(1000);
    }

    receive_record_loop.join().unwrap();
}

/// Is `slot` the first of leader window, accounts for (TODO) WFSM and genesis
fn first_in_leader_window(slot: Slot) -> bool {
    leader_slot_index(slot) == 0
        || slot == 1
        // TODO: figure out the WFSM hack properly
        || slot == 2
    // TODO: also test for restarting in middle of leader window
}

/// Resets poh recorder
fn reset_poh_recorder(bank: &Arc<Bank>, ctx: &LeaderContext) {
    trace!("{}: resetting poh to {}", ctx.my_pubkey, bank.slot());
    let next_leader_slot = ctx.leader_schedule_cache.next_leader_slot(
        &ctx.my_pubkey,
        bank.slot(),
        bank,
        Some(ctx.blockstore.as_ref()),
        GRACE_TICKS_FACTOR * MAX_GRACE_SLOTS,
    );

    ctx.poh_recorder
        .write()
        .unwrap()
        .reset(bank.clone(), next_leader_slot);
}

/// Similar to `maybe_start_leader`, however if replay is lagging we retry
/// until either replay finishes or we hit the block timeout.
fn start_leader_retry_replay(
    slot: Slot,
    parent_slot: Slot,
    skip_timer: Instant,
    ctx: &LeaderContext,
    metrics: &mut BlockCreationLoopMetrics,
) -> Result<(), StartLeaderError> {
    let my_pubkey = ctx.my_pubkey;
    let timeout = block_timeout(leader_slot_index(slot));
    while !timeout.saturating_sub(skip_timer.elapsed()).is_zero() {
        match maybe_start_leader(slot, parent_slot, ctx, metrics) {
            Ok(()) => {
                return Ok(());
            }
            Err(StartLeaderError::ReplayIsBehind(_)) => {
                metrics
                    .replay_is_behind_count
                    .fetch_add(1, Ordering::Relaxed);

                trace!(
                    "{my_pubkey}: Attempting to produce slot {slot}, however replay of the \
                    the parent {parent_slot} is not yet finished, waiting. Skip timer {}",
                    skip_timer.elapsed().as_millis()
                );
                let highest_frozen_slot = ctx
                    .replay_highest_frozen
                    .highest_frozen_slot
                    .lock()
                    .unwrap();

                // We wait until either we finish replay of the parent or the skip timer finishes
                let mut wait_start = Measure::start("replay_is_behind");
                let _unused = ctx
                    .replay_highest_frozen
                    .freeze_notification
                    .wait_timeout_while(
                        highest_frozen_slot,
                        timeout.saturating_sub(skip_timer.elapsed()),
                        |hfs| *hfs < parent_slot,
                    )
                    .unwrap();
                wait_start.stop();
                metrics
                    .replay_is_behind_wait_elapsed
                    .fetch_add(wait_start.as_us(), Ordering::Relaxed);
            }
            Err(e) => return Err(e),
        }
    }

    metrics
        .replay_is_behind_count
        .fetch_add(1, Ordering::Relaxed);
    error!(
        "{my_pubkey}: Skipping production of {slot}: \
        Unable to replay parent {parent_slot} in time"
    );
    Err(StartLeaderError::ReplayIsBehind(parent_slot))
}

/// Checks if we are set to produce a leader block for `slot`:
/// - Is the highest notarization/finalized slot from `cert_pool` frozen
/// - Startup verification is complete
/// - Bank forks does not already contain a bank for `slot`
///
/// If checks pass we return `Ok(())` and:
/// - Reset poh to the `parent_slot`
/// - Create a new bank for `slot` with parent `parent_slot`
/// - Insert into bank_forks and poh recorder
fn maybe_start_leader(
    slot: Slot,
    parent_slot: Slot,
    ctx: &LeaderContext,
    metrics: &mut BlockCreationLoopMetrics,
) -> Result<(), StartLeaderError> {
    if ctx.bank_forks.read().unwrap().get(slot).is_some() {
        metrics
            .already_have_bank_count
            .fetch_add(1, Ordering::Relaxed);
        return Err(StartLeaderError::AlreadyHaveBank(slot));
    }

    let Some(parent_bank) = ctx.bank_forks.read().unwrap().get(parent_slot) else {
        metrics
            .replay_is_behind_count
            .fetch_add(1, Ordering::Relaxed);
        return Err(StartLeaderError::ReplayIsBehind(parent_slot));
    };

    if !parent_bank.is_frozen() {
        metrics
            .replay_is_behind_count
            .fetch_add(1, Ordering::Relaxed);
        return Err(StartLeaderError::ReplayIsBehind(parent_slot));
    }

    if !parent_bank.is_startup_verification_complete() {
        metrics
            .startup_verification_incomplete_count
            .fetch_add(1, Ordering::Relaxed);
        return Err(StartLeaderError::StartupVerificationIncomplete(parent_slot));
    }

    // TODO(ashwin): plug this in from replay
    let has_new_vote_been_rooted = true;
    if !has_new_vote_been_rooted {
        return Err(StartLeaderError::VoteNotRooted);
    }

    // Create and insert the bank
    create_and_insert_leader_bank(slot, parent_bank, ctx);
    Ok(())
}

/// Creates and inserts the leader bank `slot` of this window with
/// parent `parent_bank`
fn create_and_insert_leader_bank(slot: Slot, parent_bank: Arc<Bank>, ctx: &LeaderContext) {
    let parent_slot = parent_bank.slot();
    let root_slot = ctx.bank_forks.read().unwrap().root();

    if let Some(bank) = ctx.poh_recorder.read().unwrap().bank() {
        panic!(
            "{}: Attempting to produce a block for {slot}, however we still are in production of \
            {}. Something has gone wrong with the block creation loop. exiting",
            ctx.my_pubkey,
            bank.slot(),
        );
    }

    if ctx.poh_recorder.read().unwrap().start_slot() != parent_slot {
        // Important to keep Poh somewhat accurate for
        // parts of the system relying on PohRecorder::would_be_leader()
        //
        // TODO: On migration need to keep the ticks around for parent slots in previous epoch
        // because reset below will delete those ticks
        reset_poh_recorder(&parent_bank, ctx);
    }

    let tpu_bank = ReplayStage::new_bank_from_parent_with_notify(
        parent_bank.clone(),
        slot,
        root_slot,
        &ctx.my_pubkey,
        &ctx.rpc_subscriptions,
        &ctx.slot_status_notifier,
        NewBankOptions::default(),
    );
    // make sure parent is frozen for finalized hashes via the above
    // new()-ing of its child bank
    ctx.banking_tracer.hash_event(
        parent_slot,
        &parent_bank.last_blockhash(),
        &parent_bank.hash(),
    );

    // Insert the bank
    let tpu_bank = ctx.bank_forks.write().unwrap().insert(tpu_bank);
    let poh_bank_start = ctx
        .poh_recorder
        .write()
        .unwrap()
        .set_bank(tpu_bank, ctx.track_transaction_indexes);
    // TODO: cleanup, this is no longer needed
    poh_bank_start
        .contains_valid_certificate
        .store(true, Ordering::Relaxed);

    info!(
        "{}: new fork:{} parent:{} (leader) root:{}",
        ctx.my_pubkey, slot, parent_slot, root_slot
    );
}
