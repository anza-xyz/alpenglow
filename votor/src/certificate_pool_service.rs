//! Service in charge of ingesting new messages into the certificate pool
//! and notifying votor of new events that occur

use {
    crate::{
        certificate_pool::{
            self, parent_ready_tracker::BlockProductionParent, AddVoteError, CertificatePool,
        },
        commitment::AlpenglowCommitmentAggregationData,
        event::{LeaderWindowInfo, VotorEvent},
        voting_utils,
        votor::Votor,
        CertificateId, STANDSTILL_TIMEOUT,
    },
    alpenglow_vote::bls_message::CertificateMessage,
    crossbeam_channel::{select, RecvError, Sender},
    solana_ledger::{
        blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::last_of_consecutive_leader_slots,
    },
    solana_pubkey::Pubkey,
    solana_runtime::{
        root_bank_cache::RootBankCache, vote_sender_types::BLSVerifiedMessageReceiver,
    },
    solana_sdk::clock::Slot,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Condvar, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

/// Inputs for the certificate pool thread
pub(crate) struct CertificatePoolContext {
    pub(crate) exit: Arc<AtomicBool>,
    pub(crate) start: Arc<(Mutex<bool>, Condvar)>,

    pub(crate) my_pubkey: Pubkey,
    pub(crate) my_vote_pubkey: Pubkey,
    pub(crate) blockstore: Arc<Blockstore>,
    pub(crate) root_bank_cache: RootBankCache,
    pub(crate) leader_schedule_cache: Arc<LeaderScheduleCache>,

    // TODO: for now we ingest our own votes into the certificate pool
    // just like regular votes. However do we need to convert
    // Vote -> BLSMessage -> Vote?
    // consider adding a separate pathway in cert_pool.add_transaction for ingesting own votes
    pub(crate) own_vote_receiver: BLSVerifiedMessageReceiver,
    pub(crate) bls_receiver: BLSVerifiedMessageReceiver,
    pub(crate) event_sender: Sender<VotorEvent>,
    pub(crate) commitment_sender: Sender<AlpenglowCommitmentAggregationData>,
    pub(crate) certificate_sender: Sender<(CertificateId, CertificateMessage)>,
}

pub(crate) struct CertificatePoolService {
    t_ingest: JoinHandle<()>,
}

impl CertificatePoolService {
    pub(crate) fn new(ctx: CertificatePoolContext) -> Self {
        let exit = ctx.exit.clone();
        let t_ingest = Builder::new()
            .name("solCertPoolIngest".to_string())
            .spawn(move || {
                if let Err(e) = Self::certificate_pool_ingest(ctx) {
                    info!("Certificate pool service exited with error: {e:?}. Shutting down");
                    exit.store(true, Ordering::Relaxed);
                }
            })
            .unwrap();

        Self { t_ingest }
    }

    // TODO: properly bubble up errors
    fn certificate_pool_ingest(mut ctx: CertificatePoolContext) -> Result<(), RecvError> {
        let mut events = vec![];
        let mut cert_pool = certificate_pool::load_from_blockstore(
            &ctx.my_pubkey,
            &ctx.root_bank_cache.root_bank(),
            ctx.blockstore.as_ref(),
            Some(ctx.certificate_sender.clone()),
            &mut events,
        );
        let mut highest_parent_ready = cert_pool.parent_ready_tracker.highest_parent_ready();

        // Wait until migration has completed
        Votor::wait_for_migration_or_exit(&ctx.exit, &ctx.start);
        let mut current_root = ctx.root_bank_cache.root_bank().slot();

        // Standstill tracking
        let mut standstill_timer = Instant::now();
        let mut highest_finalized_slot = cert_pool.highest_finalized_slot();

        // Ingest votes into certificate pool and notify voting loop of new events
        loop {
            if ctx.exit.load(Ordering::Relaxed) {
                return Ok(());
            }

            if standstill_timer.elapsed() > STANDSTILL_TIMEOUT {
                events.push(VotorEvent::Standstill(highest_finalized_slot));
                standstill_timer = Instant::now();
            }

            if events
                .drain(..)
                .try_for_each(|event| ctx.event_sender.send(event))
                .is_err()
            {
                // Shutdown
                info!(
                    "{}: Votor event receiver disconnected. Exiting.",
                    ctx.my_pubkey
                );
                ctx.exit.store(true, Ordering::Relaxed);
                return Ok(());
            }

            let bls_messages = select! {
                recv(ctx.own_vote_receiver) -> msg => {
                    std::iter::once(msg?).chain(ctx.own_vote_receiver.try_iter())
                },
                recv(ctx.bls_receiver) -> msg => {
                    std::iter::once(msg?).chain(ctx.bls_receiver.try_iter())
                },
                default(Duration::from_secs(1)) => continue
            };

            for message in bls_messages {
                match voting_utils::add_message_and_maybe_update_commitment(
                    &ctx.my_pubkey,
                    &ctx.my_vote_pubkey,
                    &message,
                    &mut cert_pool,
                    &mut events,
                    &ctx.commitment_sender,
                ) {
                    Ok(Some(finalized_slot)) => {
                        // Reset standstill timer
                        debug_assert!(finalized_slot > highest_finalized_slot);
                        highest_finalized_slot = finalized_slot;
                        standstill_timer = Instant::now();
                        // Set root
                        let root_bank = ctx.root_bank_cache.root_bank();
                        if root_bank.slot() > current_root {
                            current_root = root_bank.slot();
                            cert_pool.handle_new_root(root_bank);
                        }
                    }
                    Ok(_) => (),
                    Err(AddVoteError::CertificateSenderError) => {
                        // Shutdown
                        info!(
                            "{}: Certificate receiver disconnected. Exiting.",
                            ctx.my_pubkey
                        );
                        ctx.exit.store(true, Ordering::Relaxed);
                        return Ok(());
                    }
                    Err(e) => {
                        // This is a non critical error, a duplicate vote for example
                        trace!("{}: unable to push vote into pool {}", ctx.my_pubkey, e);
                        continue;
                    }
                };
            }

            // TODO: we need set identity here as well
            Self::add_produce_block_event(
                &mut highest_parent_ready,
                &cert_pool,
                &mut ctx,
                &mut events,
            );
        }
    }

    fn add_produce_block_event(
        highest_parent_ready: &mut Slot,
        cert_pool: &CertificatePool,
        ctx: &mut CertificatePoolContext,
        events: &mut Vec<VotorEvent>,
    ) {
        let Some(new_highest_parent_ready) = events
            .iter()
            .filter_map(|event| match event {
                VotorEvent::ParentReady { slot, .. } => Some(slot),
                _ => None,
            })
            .max()
            .copied()
        else {
            return;
        };

        if new_highest_parent_ready <= *highest_parent_ready {
            return;
        }
        *highest_parent_ready = new_highest_parent_ready;

        // TODO: Add blockstore check
        let Some(leader_pubkey) = ctx
            .leader_schedule_cache
            .slot_leader_at(*highest_parent_ready, Some(&ctx.root_bank_cache.root_bank()))
        else {
            error!("Unable to compute the leader at slot {highest_parent_ready}. Something is wrong, exiting");
            ctx.exit.store(true, Ordering::Relaxed);
            return;
        };

        if leader_pubkey != ctx.my_pubkey {
            return;
        }

        let start_slot = *highest_parent_ready;
        let end_slot = last_of_consecutive_leader_slots(start_slot);

        match cert_pool
            .parent_ready_tracker
            .block_production_parent(start_slot)
        {
            BlockProductionParent::MissedWindow => {
                warn!(
                    "{}: Leader slot {start_slot} has already been certified, \
                    skipping production of {start_slot}-{end_slot}",
                    ctx.my_pubkey,
                );
            }
            BlockProductionParent::ParentNotReady => {
                // This can't happen, place holder depending on how we hook up optimistic
                ctx.exit.store(true, Ordering::Relaxed);
                panic!(
                    "Must have a block production parent: {:#?}",
                    cert_pool.parent_ready_tracker
                );
            }
            BlockProductionParent::Parent(parent_block) => {
                events.push(VotorEvent::ProduceWindow(LeaderWindowInfo {
                    start_slot,
                    end_slot,
                    parent_block,
                    // TODO: we can just remove this
                    skip_timer: Instant::now(),
                }))
            }
        }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        self.t_ingest.join()
    }
}
