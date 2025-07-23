use {
    crate::{
        epoch_stakes::{BLSPubkeyToRankMap, EpochStakes},
        root_bank_cache::RootBankCache,
    },
    crossbeam_channel::Receiver,
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

/// Updates the epoch stakes state from the bank forks.
///
/// If new state was found, returns the corresponding root epoch and the state.
fn update_state(root_bank_cache: &mut RootBankCache, epoch: Epoch) -> Option<(Epoch, State)> {
    let root_bank = root_bank_cache.root_bank();
    let root_epoch = root_bank.epoch();
    (root_epoch > epoch).then(|| {
        (
            root_epoch,
            State {
                stakes: root_bank.epoch_stakes_map().clone(),
                epoch_schedule: root_bank.epoch_schedule().clone(),
            },
        )
    })
}

/// A service that regularly updates the epoch stakes state from the bank forks
/// and exposes various methods to access the state.
pub struct EpochStakesService {
    state: Arc<PlRwLock<State>>,
}

impl EpochStakesService {
    pub fn new(new_epoch_receiver: Receiver<()>, mut root_bank_cache: RootBankCache) -> Self {
        let mut prev_epoch = Epoch::default();
        let state = Arc::new(PlRwLock::new(State {
            stakes: HashMap::new(),
            epoch_schedule: EpochSchedule::default(),
        }));

        {
            let state = state.clone();
            thread::spawn(move || loop {
                let () = new_epoch_receiver.recv().unwrap();
                if let Some((new_epoch, update)) = update_state(&mut root_bank_cache, prev_epoch) {
                    prev_epoch = new_epoch;
                    *state.write() = update;
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
