use {
    crate::{bank::Bank, epoch_stakes::BLSPubkeyToRankMap, epoch_state::EpochState},
    crossbeam_channel::Receiver,
    log::warn,
    parking_lot::RwLock as PlRwLock,
    solana_sdk::clock::Slot,
    std::{sync::Arc, thread},
};

/// A service that regularly updates the epoch stakes state from `Bank`s
/// and exposes various methods to access the state.
pub struct EpochStakesService {
    state: Arc<PlRwLock<EpochState>>,
}

impl EpochStakesService {
    pub fn new(bank: &Bank, new_bank_receiver: Receiver<Arc<Bank>>) -> Self {
        let state = Arc::new(PlRwLock::new(EpochState::new(bank)));
        {
            let state = state.clone();
            thread::spawn(move || loop {
                let bank = match new_bank_receiver.recv() {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("recv() returned {e:?}.  Exiting.");
                        break;
                    }
                };
                let can_update = state.read().can_update(&bank);
                if can_update {
                    state.write().update(&bank);
                }
            });
        }
        Self { state }
    }

    pub fn get_key_to_rank_map(&self, slot: Slot) -> Option<Arc<BLSPubkeyToRankMap>> {
        let guard = self.state.read();
        let epoch = guard.schedule.get_epoch(slot);
        guard
            .stakes_map
            .get(&epoch)
            .map(|stake| Arc::clone(stake.bls_pubkey_to_rank_map()))
    }
}
