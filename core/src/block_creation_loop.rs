//! The block creation loop
//! When our leader window is reached, attempts to create our leader blocks
//! within the block timeouts. Responsible for inserting empty banks for
//! banking stage to fill, and clearing banks once the timeout has been reached.
//!
//! Once alpenglow is active, this is the only thread that will touch the [`PohRecorder`].
use {
    crate::{
        banking_trace::BankingTracer,
        replay_stage::{Finalizer, ReplayStage},
    },
    crossbeam_channel::Receiver,
    solana_clock::Slot,
    solana_entry::block_component::{BlockFooterV1, GenesisCertificate, VersionedBlockMarker},
    solana_gossip::cluster_info::ClusterInfo,
    solana_hash::Hash,
    solana_ledger::{
        blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::{last_of_consecutive_leader_slots, leader_slot_index},
    },
    solana_measure::measure::Measure,
    solana_poh::{
        poh_recorder::{PohRecorder, PohRecorderError, GRACE_TICKS_FACTOR, MAX_GRACE_SLOTS},
        record_channels::RecordReceiver,
    },
    solana_pubkey::Pubkey,
    solana_rpc::{rpc_subscriptions::RpcSubscriptions, slot_status_notifier::SlotStatusNotifier},
    solana_runtime::{
        bank::{Bank, NewBankOptions},
        bank_forks::BankForks,
        block_component_processor::BlockComponentProcessor,
    },
    solana_version::version,
    solana_votor::{common::block_timeout, event::LeaderWindowInfo},
    stats::{BlockCreationLoopMetrics, SlotMetrics},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    thiserror::Error,
};

mod stats;

pub struct BlockCreationLoop {
    thread: JoinHandle<()>,
}

impl BlockCreationLoop {
    pub fn new(config: BlockCreationLoopConfig) -> Self {
        let thread = Builder::new()
            .name("solBlockCreationLoop".to_string())
            .spawn(move || {
                start_loop(config);
            })
            .unwrap();

        Self { thread }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread.join()
    }
}

pub struct BlockCreationLoopConfig {
    pub exit: Arc<AtomicBool>,

    // Shared state
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub blockstore: Arc<Blockstore>,
    pub cluster_info: Arc<ClusterInfo>,
    pub poh_recorder: Arc<RwLock<PohRecorder>>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub rpc_subscriptions: Option<Arc<RpcSubscriptions>>,

    // Notifiers
    pub banking_tracer: Arc<BankingTracer>,
    pub slot_status_notifier: Option<SlotStatusNotifier>,

    // Receivers / notifications from banking stage / replay / votor
    pub leader_window_info_receiver: Receiver<LeaderWindowInfo>,
    pub replay_highest_frozen: Arc<ReplayHighestFrozen>,
    pub highest_parent_ready: Arc<RwLock<(Slot, (Slot, Hash))>>,

    // Channel to receive RecordReceiver from PohService
    pub record_receiver_receiver: Receiver<RecordReceiver>,
    pub optimistic_parent_receiver: Receiver<LeaderWindowInfo>,
}

struct LeaderContext {
    exit: Arc<AtomicBool>,
    my_pubkey: Pubkey,
    leader_window_info_receiver: Receiver<LeaderWindowInfo>,
    highest_parent_ready: Arc<RwLock<(Slot, (Slot, Hash))>>,

    blockstore: Arc<Blockstore>,
    record_receiver: RecordReceiver,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    bank_forks: Arc<RwLock<BankForks>>,
    rpc_subscriptions: Option<Arc<RpcSubscriptions>>,
    slot_status_notifier: Option<SlotStatusNotifier>,
    banking_tracer: Arc<BankingTracer>,
    replay_highest_frozen: Arc<ReplayHighestFrozen>,

    // Metrics
    metrics: BlockCreationLoopMetrics,
    slot_metrics: SlotMetrics,

    // Migration information
    genesis_cert: GenesisCertificate,
}

#[derive(Default)]
pub struct ReplayHighestFrozen {
    pub highest_frozen_slot: Mutex<Slot>,
    pub freeze_notification: Condvar,
}

#[derive(Debug, Error)]
enum StartLeaderError {
    /// Replay has not yet frozen the parent slot
    #[error("Replay is behind for parent slot {0} for leader slot {1}")]
    ReplayIsBehind(/* parent slot */ Slot, /* leader slot */ Slot),

    /// Bank forks already contains bank
    #[error("Already contain bank for leader slot {0}")]
    AlreadyHaveBank(/* leader slot */ Slot),

    /// Cluster has certified blocks after our leader window
    #[error("Cluster has certified blocks before {0} which is after our leader slot {1}")]
    ClusterCertifiedBlocksAfterWindow(
        /* parent ready slot */ Slot,
        /* leader slot */ Slot,
    ),
}

/// The block creation loop.
///
/// The `votor::consensus_pool_service` tracks when it is our leader window, and
/// communicates the skip timer and parent slot for our window. This loop takes the responsibility
/// of creating our `NUM_CONSECUTIVE_LEADER_SLOTS` blocks and finishing them within the required timeout.
fn start_loop(config: BlockCreationLoopConfig) {
    let BlockCreationLoopConfig {
        exit,
        bank_forks,
        blockstore,
        cluster_info,
        poh_recorder,
        leader_schedule_cache,
        rpc_subscriptions,
        banking_tracer,
        slot_status_notifier,
        record_receiver_receiver,
        leader_window_info_receiver,
        replay_highest_frozen,
        highest_parent_ready,
        optimistic_parent_receiver,
    } = config;

    // Similar to Votor, if this loop dies kill the validator
    let _exit = Finalizer::new(exit.clone());

    // get latest identity pubkey during startup
    let mut my_pubkey = cluster_info.id();

    info!("{my_pubkey}: Block creation loop initialized");

    // Wait for PohService to be shutdown
    let record_receiver = match record_receiver_receiver.recv() {
        Ok(receiver) => receiver,
        Err(e) => {
            error!("{my_pubkey}: Failed to receive RecordReceiver from PohService. Exiting: {e:?}",);
            return;
        }
    };

    let genesis_cert = bank_forks
        .read()
        .unwrap()
        .migration_status()
        .genesis_certificate()
        .expect("Migration complete, genesis certificate must exist");
    let genesis_cert = GenesisCertificate::try_from((*genesis_cert).clone())
        .expect("Genesis certificate must be valid");

    info!("{my_pubkey}: Block creation loop starting");

    let mut ctx = LeaderContext {
        exit,
        my_pubkey,
        highest_parent_ready,
        leader_window_info_receiver,
        blockstore,
        poh_recorder: poh_recorder.clone(),
        record_receiver,
        leader_schedule_cache,
        bank_forks,
        rpc_subscriptions,
        slot_status_notifier,
        banking_tracer,
        replay_highest_frozen,
        metrics: BlockCreationLoopMetrics::default(),
        slot_metrics: SlotMetrics::default(),
        genesis_cert,
    };

    // Setup poh
    // Important this is called *before* any new alpenglow
    // leaders call `set_bank()`, otherwise, the old PoH
    // tick producer will still tick in that alpenglow bank
    // Since `wait_for_migration_or_exit` succeeded above we know that PohService has shutdown
    // AND that replay no longer touches poh recorder. At this point BlockCreationLoop is the sole
    // modifier of poh_recorder
    {
        let mut w_poh_recorder = ctx.poh_recorder.write().unwrap();
        w_poh_recorder.enable_alpenglow();
    }
    reset_poh_recorder(&ctx.bank_forks.read().unwrap().working_bank(), &ctx);

    while !ctx.exit.load(Ordering::Relaxed) {
        // Check if set-identity was called at each leader window start
        if my_pubkey != cluster_info.id() {
            let my_old_pubkey = my_pubkey;
            my_pubkey = cluster_info.id();
            ctx.my_pubkey = my_pubkey;

            warn!(
                "Identity changed from {my_old_pubkey} to {my_pubkey} during block creation loop"
            );
        }

        // Wait for the voting loop to notify us, draining all pending messages and keeping the highest slot
        let LeaderWindowInfo {
            start_slot,
            end_slot,
            // TODO: handle duplicate blocks by using the hash here
            parent_block: (parent_slot, _),
            skip_timer,
        } = {
            // Drain all pending messages and keep the latest one
            let Some(info) = ctx
                .leader_window_info_receiver
                .recv_timeout(Duration::from_secs(1))
                .ok()
                .and_then(|window| {
                    ctx.leader_window_info_receiver
                        .try_iter()
                        .last()
                        .or(Some(window))
                })
            else {
                continue;
            };

            info
        };

        // TODO(ksn): fast leader handover. Once we can stream parent ready events over a channel,
        // we'll have the two channels race each other to determine what to do.
        //
        // For now, just drain the channel and don't do anything with optimistic parents.
        while let Ok(_optimistic_parent) = optimistic_parent_receiver.try_recv() {}

        trace!("Received window notification for {start_slot} to {end_slot} parent: {parent_slot}");
        if let Err(e) = produce_window(start_slot, end_slot, parent_slot, skip_timer, &mut ctx) {
            // Give up on this leader window
            error!(
                "{my_pubkey}: Unable to produce window {start_slot}-{end_slot}, skipping window: \
                 {e:?}"
            );
            continue;
        }

        ctx.metrics.loop_count += 1;
        ctx.metrics.report(1000);
    }

    info!("{my_pubkey}: Block creation loop shutting down");
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

/// Produces the leader window from `start_slot` -> `end_slot` using parent
/// `parent_slot` while abiding to the `skip_timer`
fn produce_window(
    start_slot: Slot,
    end_slot: Slot,
    parent_slot: Slot,
    skip_timer: Instant,
    ctx: &mut LeaderContext,
) -> Result<(), StartLeaderError> {
    // Insert the first bank
    start_leader_retry_replay(start_slot, parent_slot, skip_timer, ctx)?;

    let my_pubkey = ctx.my_pubkey;
    let mut window_production_start = Measure::start("window_production");
    let mut slot = start_slot;
    while !ctx.exit.load(Ordering::Relaxed) {
        let leader_index = leader_slot_index(slot);
        let timeout = block_timeout(leader_index);
        trace!(
            "{my_pubkey}: waiting for leader bank {slot} to finish, remaining time: {}",
            timeout.saturating_sub(skip_timer.elapsed()).as_millis(),
        );

        let mut bank_completion_measure = Measure::start("bank_completion");
        if let Err(e) = record_and_complete_block(
            ctx.poh_recorder.as_ref(),
            &mut ctx.record_receiver,
            skip_timer,
            timeout,
        ) {
            panic!("PohRecorder record failed: {e:?}");
        }
        assert!(!ctx.poh_recorder.read().unwrap().has_bank());
        bank_completion_measure.stop();

        ctx.metrics.bank_timeout_completion_count += 1;
        let _ = ctx
            .metrics
            .bank_timeout_completion_elapsed_hist
            .increment(bank_completion_measure.as_us());

        // Produce our next slot
        slot += 1;
        if slot > end_slot {
            trace!("{my_pubkey}: finished leader window {start_slot}-{end_slot}");
            break;
        }

        // Although `slot - 1`has been cleared from `poh_recorder`, it might not have finished processing in
        // `replay_stage`, which is why we use `start_leader_retry_replay`
        start_leader_retry_replay(slot, slot - 1, skip_timer, ctx)?;
    }

    window_production_start.stop();
    ctx.metrics.window_production_elapsed += window_production_start.as_us();
    Ok(())
}

/// Clamps the block producer timestamp to ensure that the leader produces a timestamp that conforms
/// to Alpenglow clock bounds.
fn skew_block_producer_time_nanos(
    parent_slot: Slot,
    parent_time_nanos: i64,
    working_bank_slot: Slot,
    working_bank_time_nanos: i64,
) -> i64 {
    let (min_working_bank_time, max_working_bank_time) =
        BlockComponentProcessor::nanosecond_time_bounds(
            parent_slot,
            parent_time_nanos,
            working_bank_slot,
        );

    working_bank_time_nanos
        .max(min_working_bank_time)
        .min(max_working_bank_time)
}

/// Produces a block footer with the current timestamp and version information.
/// The bank_hash field is left as default and will be filled in after the bank freezes.
fn produce_block_footer(bank: Arc<Bank>) -> BlockFooterV1 {
    let mut block_producer_time_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Misconfigured system clock; couldn't measure block producer time.")
        .as_nanos() as i64;

    let slot = bank.slot();

    if let Some(parent_bank) = bank.parent() {
        // Get parent time from alpenglow clock (nanoseconds) or fall back to clock sysvar (seconds -> nanoseconds)
        let parent_time_nanos = bank
            .get_nanosecond_clock()
            .unwrap_or_else(|| bank.clock().unix_timestamp.saturating_mul(1_000_000_000));
        let parent_slot = parent_bank.slot();

        block_producer_time_nanos = skew_block_producer_time_nanos(
            parent_slot,
            parent_time_nanos,
            slot,
            block_producer_time_nanos,
        );
    }

    BlockFooterV1 {
        bank_hash: Hash::default(),
        block_producer_time_nanos: block_producer_time_nanos as u64,
        block_user_agent: format!("agave/{}", version!()).into_bytes(),
        // TODO(ksn, wen): fill this field
        final_cert: None,
        // TODO(akhi): include reward certs
        skip_reward_cert: None,
        notar_reward_cert: None,
    }
}

/// Shutdowns the record receiver and drains any remaining records.
fn shutdown_and_drain_record_receiver(
    poh_recorder: &RwLock<PohRecorder>,
    record_receiver: &mut RecordReceiver,
) -> Result<(), PohRecorderError> {
    record_receiver.shutdown();

    while !record_receiver.is_safe_to_restart() {
        let Ok(record) = record_receiver.recv_timeout(Duration::ZERO) else {
            continue;
        };
        poh_recorder.write().unwrap().record(
            record.slot,
            record.mixins,
            record.transaction_batches,
        )?;
    }

    Ok(())
}

/// Records incoming transactions until we reach the block timeout.
/// Afterwards:
/// - Shutdown the record receiver
/// - Clear any inflight records
/// - Insert the block footer
/// - Insert the alpentick
/// - Clear the working bank
fn record_and_complete_block(
    poh_recorder: &RwLock<PohRecorder>,
    record_receiver: &mut RecordReceiver,
    block_timer: Instant,
    block_timeout: Duration,
) -> Result<(), PohRecorderError> {
    loop {
        let remaining_slot_time = block_timeout.saturating_sub(block_timer.elapsed());
        if remaining_slot_time.is_zero() {
            break;
        }

        let Ok(record) = record_receiver.recv_timeout(remaining_slot_time) else {
            continue;
        };
        poh_recorder.write().unwrap().record(
            record.slot,
            record.mixins,
            record.transaction_batches,
        )?;
    }

    // Shutdown and clear any inflight records
    // TODO: do we need to lower the block timeout from 400ms to account for this / insertion of the block footer
    shutdown_and_drain_record_receiver(poh_recorder, record_receiver)?;

    // Alpentick and clear bank
    let mut w_poh_recorder = poh_recorder.write().unwrap();
    let bank = w_poh_recorder
        .bank()
        .expect("Bank cannot have been cleared as BlockCreationLoop is the only modifier");

    trace!(
        "{}: bank {} has reached block timeout, ticking",
        bank.collector_id(),
        bank.slot()
    );

    let max_tick_height = bank.max_tick_height();
    // Set the tick height for the bank to max_tick_height - 1, so that PohRecorder::flush_cache()
    // will properly increment the tick_height to max_tick_height.
    bank.set_tick_height(max_tick_height - 1);
    // Write the single tick for this slot

    // Produce the footer with the current timestamp
    let working_bank = w_poh_recorder.working_bank().unwrap();
    let footer = produce_block_footer(working_bank.bank.clone_without_scheduler());

    BlockComponentProcessor::update_bank_with_footer(
        working_bank.bank.clone_without_scheduler(),
        &footer,
    );

    drop(bank);
    w_poh_recorder.tick_alpenglow(max_tick_height, footer);

    Ok(())
}

/// Similar to `maybe_start_leader`, however if replay is lagging we retry
/// until either replay finishes or we hit the block timeout.
fn start_leader_retry_replay(
    slot: Slot,
    parent_slot: Slot,
    skip_timer: Instant,
    ctx: &mut LeaderContext,
) -> Result<(), StartLeaderError> {
    trace!(
        "{}: Attempting to start leader slot {slot} parent {parent_slot}",
        ctx.my_pubkey
    );
    let my_pubkey = ctx.my_pubkey;
    let timeout = block_timeout(leader_slot_index(slot));
    let end_slot = last_of_consecutive_leader_slots(slot);

    let mut slot_delay_start = Measure::start("slot_delay");
    while !timeout.saturating_sub(skip_timer.elapsed()).is_zero() {
        ctx.slot_metrics.attempt_count += 1;

        // Check if the entire window is skipped.
        let highest_parent_ready_slot = ctx.highest_parent_ready.read().unwrap().0;
        if highest_parent_ready_slot > end_slot {
            trace!(
                "{my_pubkey}: Skipping production of {slot} because highest parent ready slot is \
                 {highest_parent_ready_slot} > end slot {end_slot}"
            );
            ctx.metrics.skipped_window_behind_parent_ready_count += 1;
            return Err(StartLeaderError::ClusterCertifiedBlocksAfterWindow(
                highest_parent_ready_slot,
                slot,
            ));
        }

        match maybe_start_leader(slot, parent_slot, ctx) {
            Ok(()) => {
                slot_delay_start.stop();
                let _ = ctx
                    .slot_metrics
                    .slot_delay_hist
                    .increment(slot_delay_start.as_us());

                ctx.slot_metrics.report();

                return Ok(());
            }
            Err(StartLeaderError::ReplayIsBehind(_, _)) => {
                trace!(
                    "{my_pubkey}: Attempting to produce slot {slot}, however replay of the the \
                     parent {parent_slot} is not yet finished, waiting. Skip timer {}",
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
                ctx.slot_metrics.replay_is_behind_cumulative_wait_elapsed += wait_start.as_us();
                let _ = ctx
                    .slot_metrics
                    .replay_is_behind_wait_elapsed_hist
                    .increment(wait_start.as_us());
            }
            Err(e) => return Err(e),
        }
    }

    trace!(
        "{my_pubkey}: Skipping production of {slot}: Unable to replay parent {parent_slot} in time"
    );
    Err(StartLeaderError::ReplayIsBehind(parent_slot, slot))
}

/// Checks if we are set to produce a leader block for `slot`:
/// - Is the highest notarization/finalized slot from `consensus_pool` frozen
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
    ctx: &mut LeaderContext,
) -> Result<(), StartLeaderError> {
    if ctx.bank_forks.read().unwrap().get(slot).is_some() {
        ctx.slot_metrics.already_have_bank_count += 1;
        return Err(StartLeaderError::AlreadyHaveBank(slot));
    }

    let Some(parent_bank) = ctx.bank_forks.read().unwrap().get(parent_slot) else {
        ctx.slot_metrics.replay_is_behind_count += 1;
        return Err(StartLeaderError::ReplayIsBehind(parent_slot, slot));
    };

    if !parent_bank.is_frozen() {
        ctx.slot_metrics.replay_is_behind_count += 1;
        return Err(StartLeaderError::ReplayIsBehind(parent_slot, slot));
    }

    // Create and insert the bank
    create_and_insert_leader_bank(slot, parent_bank, ctx);
    Ok(())
}

/// Creates and inserts the leader bank `slot` of this window with
/// parent `parent_bank`
fn create_and_insert_leader_bank(slot: Slot, parent_bank: Arc<Bank>, ctx: &mut LeaderContext) {
    let parent_slot = parent_bank.slot();
    let root_slot = ctx.bank_forks.read().unwrap().root();
    trace!(
        "{}: Creating and inserting leader slot {slot} parent {parent_slot} root {root_slot}",
        ctx.my_pubkey
    );

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
        reset_poh_recorder(&parent_bank, ctx);
    }

    let tpu_bank = ReplayStage::new_bank_from_parent_with_notify(
        parent_bank.clone(),
        slot,
        root_slot,
        &ctx.my_pubkey,
        ctx.rpc_subscriptions.as_deref(),
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
    ctx.poh_recorder.write().unwrap().set_bank(tpu_bank);

    // If this is the first alpenglow block, emit the genesis certificate marker
    maybe_include_genesis_certificate(parent_slot, ctx);

    // Wakeup banking stage
    ctx.record_receiver.restart(slot);

    info!(
        "{}: new fork:{} parent:{} (leader) root:{}",
        ctx.my_pubkey, slot, parent_slot, root_slot
    );
}

///  If this the very first alpenglow block, include the genesis certificate
///  Note: if the alpenglow genesis is 0, then this is a test cluster with Alpenglow enabled
///  by default. No need to put in the genesis marker as the genesis account is already populated
///  during cluster creation.
fn maybe_include_genesis_certificate(parent_slot: Slot, ctx: &LeaderContext) {
    if parent_slot != ctx.genesis_cert.slot || parent_slot == 0 {
        return;
    }

    // Send the genesis certificate
    let genesis_marker = VersionedBlockMarker::new_genesis_certificate(ctx.genesis_cert.clone());
    let mut poh_recorder = ctx.poh_recorder.write().unwrap();
    poh_recorder
        .send_marker(genesis_marker)
        .expect("Max tick height cannot have been reached");

    // Process the genesis certificate
    let bank = poh_recorder.bank().expect("Bank cannot have been cleared");
    let processor = bank.block_component_processor.read().unwrap();
    processor
        .on_genesis_certificate(
            bank.clone(),
            ctx.genesis_cert.clone(),
            &ctx.bank_forks.read().unwrap().migration_status(),
        )
        .expect("Recording genesis certificate should not fail");
}
