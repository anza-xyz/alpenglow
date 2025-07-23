use {
    crate::{
        bank::Bank,
        epoch_stakes::{BLSPubkeyToRankMap, EpochStakes},
    },
    crossbeam_channel::Receiver,
    log::warn,
    parking_lot::RwLock as PlRwLock,
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
    },
    std::{collections::HashMap, sync::Arc, thread},
};

struct State {
    stakes: HashMap<Epoch, EpochStakes>,
    epoch_schedule: EpochSchedule,
}

/// A service that regularly updates the epoch stakes state from the bank forks
/// and exposes various methods to access the state.
pub struct EpochStakesService {
    state: Arc<PlRwLock<State>>,
}

impl EpochStakesService {
    pub fn new(new_bank_receiver: Receiver<Arc<Bank>>) -> Self {
        let mut prev_epoch = Epoch::default();
        let state = Arc::new(PlRwLock::new(State {
            stakes: HashMap::new(),
            epoch_schedule: EpochSchedule::default(),
        }));

        {
            let state = state.clone();
            thread::spawn(move || loop {
                let new_bank = match new_bank_receiver.recv() {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("new_epoch_receiver.recv() returned {e:?}.  Exiting.");
                        break;
                    }
                };
                let new_epoch = new_bank.epoch();
                if new_epoch > prev_epoch {
                    prev_epoch = new_epoch;
                    *state.write() = State {
                        stakes: new_bank.epoch_stakes_map().clone(),
                        epoch_schedule: new_bank.epoch_schedule().clone(),
                    };
                }
            });
        }
        Self { state }
    }

    pub fn get_key_to_rank_map(&self, slot: Slot) -> Option<Arc<BLSPubkeyToRankMap>> {
        let guard = self.state.read();
        let epoch = guard.epoch_schedule.get_epoch(slot);
        guard
            .stakes
            .get(&epoch)
            .map(|stake| Arc::clone(stake.bls_pubkey_to_rank_map()))
    }
}
