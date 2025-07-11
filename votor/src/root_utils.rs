use {
    crate::{certificate_pool::CertificatePool, vote_history::VoteHistory},
    crossbeam_channel::Sender,
    log::{error, info, trace, warn},
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_rpc::{
        optimistically_confirmed_bank_tracker::{BankNotification, BankNotificationSenderConfig},
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender,
        bank_forks::{BankForks, SetRootError},
        installed_scheduler_pool::BankWithScheduler,
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature, timing::timestamp},
    std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
    },
};

/// Banks that have completed replay, but are yet to be voted on
type PendingBlocks = BTreeMap<Slot, Arc<solana_runtime::bank::Bank>>;

/// Checks if any slots between `vote_history`'s current root
/// and `slot` have received a finalization certificate and are frozen
///
/// If so, set the root as the highest slot that fits these conditions
/// and return the root
#[allow(clippy::too_many_arguments)]
pub fn maybe_set_root(
    slot: Slot,
    cert_pool: &mut CertificatePool,
    pending_blocks: &mut PendingBlocks,
    accounts_background_request_sender: &AbsRequestSender,
    bank_notification_sender: &Option<BankNotificationSenderConfig>,
    drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
    blockstore: &Arc<Blockstore>,
    leader_schedule_cache: &Arc<LeaderScheduleCache>,
    bank_forks: &Arc<RwLock<BankForks>>,
    rpc_subscriptions: &Arc<RpcSubscriptions>,
    my_pubkey: &Pubkey,
    vote_history: &mut VoteHistory,
    has_new_vote_been_rooted: &mut bool,
    voted_signatures: &mut Vec<Signature>,
) -> Option<Slot> {
    let old_root = vote_history.root();
    info!(
        "{}: Checking for finalization certificates between {old_root} and {slot}",
        my_pubkey
    );
    let new_root = (old_root.saturating_add(1)..=slot).rev().find(|slot| {
        cert_pool.is_finalized(*slot) && bank_forks.read().unwrap().is_frozen(*slot)
    })?;
    trace!("{}: Attempting to set new root {new_root}", my_pubkey);
    vote_history.set_root(new_root);
    cert_pool.handle_new_root(bank_forks.read().unwrap().get(new_root).unwrap());
    *pending_blocks = pending_blocks.split_off(&new_root);
    if let Err(e) = check_and_handle_new_root(
        slot,
        new_root,
        accounts_background_request_sender,
        Some(new_root),
        bank_notification_sender,
        drop_bank_sender,
        blockstore,
        leader_schedule_cache,
        bank_forks,
        rpc_subscriptions,
        my_pubkey,
        has_new_vote_been_rooted,
        voted_signatures,
    ) {
        error!("Unable to set root: {e:?}");
        return None;
    }

    // Distinguish between duplicate versions of same slot
    let hash = bank_forks.read().unwrap().bank_hash(new_root).unwrap();
    if let Err(e) =
        blockstore.insert_optimistic_slot(new_root, &hash, timestamp().try_into().unwrap())
    {
        error!(
            "failed to record optimistic slot in blockstore: slot={}: {:?}",
            new_root, &e
        );
    }
    // It is critical to send the OC notification in order to keep compatibility with
    // the RPC API. Additionally the PrioritizationFeeCache relies on this notification
    // in order to perform cleanup. In the future we will look to deprecate OC and remove
    // these code paths.
    if let Some(config) = bank_notification_sender {
        config
            .sender
            .send(BankNotification::OptimisticallyConfirmed(new_root))
            .unwrap();
    }

    Some(new_root)
}

/// Sets the new root, this should be kept one-to-one with
/// ReplayStage::check_and_handle_new_root
#[allow(clippy::too_many_arguments)]
pub fn check_and_handle_new_root(
    parent_slot: Slot,
    new_root: Slot,
    accounts_background_request_sender: &AbsRequestSender,
    highest_super_majority_root: Option<Slot>,
    bank_notification_sender: &Option<BankNotificationSenderConfig>,
    drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
    blockstore: &Arc<Blockstore>,
    leader_schedule_cache: &Arc<LeaderScheduleCache>,
    bank_forks: &Arc<RwLock<BankForks>>,
    rpc_subscriptions: &Arc<RpcSubscriptions>,
    my_pubkey: &Pubkey,
    has_new_vote_been_rooted: &mut bool,
    voted_signatures: &mut Vec<Signature>,
) -> Result<(), SetRootError> {
    // get the root bank before squash
    let root_bank = bank_forks
        .read()
        .unwrap()
        .get(new_root)
        .expect("Root bank doesn't exist");
    let mut rooted_banks = root_bank.parents();
    let oldest_parent = rooted_banks.last().map(|last| last.parent_slot());
    rooted_banks.push(root_bank.clone());
    let rooted_slots: Vec<_> = rooted_banks.iter().map(|bank| bank.slot()).collect();
    // The following differs from rooted_slots by including the parent slot of the oldest parent bank.
    let rooted_slots_with_parents = bank_notification_sender
        .as_ref()
        .is_some_and(|sender| sender.should_send_parents)
        .then(|| {
            let mut new_chain = rooted_slots.clone();
            new_chain.push(oldest_parent.unwrap_or(parent_slot));
            new_chain
        });

    // Call leader schedule_cache.set_root() before blockstore.set_root() because
    // bank_forks.root is consumed by repair_service to update gossip, so we don't want to
    // get shreds for repair on gossip before we update leader schedule, otherwise they may
    // get dropped.
    leader_schedule_cache.set_root(rooted_banks.last().unwrap());
    blockstore
        .set_roots(rooted_slots.iter())
        .expect("Ledger set roots failed");
    set_bank_forks_root(
        new_root,
        bank_forks,
        accounts_background_request_sender,
        highest_super_majority_root,
        has_new_vote_been_rooted,
        voted_signatures,
        drop_bank_sender,
    )?;
    blockstore.slots_stats.mark_rooted(new_root);
    rpc_subscriptions.notify_roots(rooted_slots);
    if let Some(sender) = bank_notification_sender {
        sender
            .sender
            .send(BankNotification::NewRootBank(root_bank))
            .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));

        if let Some(new_chain) = rooted_slots_with_parents {
            sender
                .sender
                .send(BankNotification::NewRootedChain(new_chain))
                .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));
        }
    }
    info!("{} new root {}", my_pubkey, new_root);
    Ok(())
}

/// Sets the bank forks root:
/// - Prune the program cache
/// - Prune bank forks and drop the removed banks
pub fn set_bank_forks_root(
    new_root: Slot,
    bank_forks: &RwLock<BankForks>,
    accounts_background_request_sender: &AbsRequestSender,
    highest_super_majority_root: Option<Slot>,
    has_new_vote_been_rooted: &mut bool,
    voted_signatures: &mut Vec<Signature>,
    drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
) -> Result<(), SetRootError> {
    bank_forks.read().unwrap().prune_program_cache(new_root);
    let removed_banks = bank_forks.write().unwrap().set_root(
        new_root,
        accounts_background_request_sender,
        highest_super_majority_root,
    )?;

    drop_bank_sender
        .send(removed_banks)
        .unwrap_or_else(|err| warn!("bank drop failed: {:?}", err));

    // Dropping the bank_forks write lock and reacquiring as a read lock is
    // safe because updates to bank_forks are only made by a single thread.
    // TODO(ashwin): Once PR #254 lands move this back to ReplayStage
    let r_bank_forks = bank_forks.read().unwrap();
    let new_root_bank = &r_bank_forks[new_root];
    if !*has_new_vote_been_rooted {
        for signature in voted_signatures.iter() {
            if new_root_bank.get_signature_status(signature).is_some() {
                *has_new_vote_been_rooted = true;
                break;
            }
        }
        if *has_new_vote_been_rooted {
            std::mem::take(voted_signatures);
        }
    }
    Ok(())
}
