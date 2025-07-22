use {
    crate::{
        epoch_stakes::{BLSPubkeyToRankMap, EpochStakes},
        root_bank_cache::RootBankCache,
    },
    parking_lot::RwLock as PlRwLock,
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
    },
    std::{collections::HashMap, sync::Arc, thread, time::Duration},
};

const EPOCH_STAKES_QUERY_INTERVAL: Duration = Duration::from_secs(60);

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
    pub fn new(mut root_bank_cache: RootBankCache) -> Self {
        let mut last_update_epoch = Epoch::default();
        let state = Arc::new(PlRwLock::new(State {
            stakes: HashMap::new(),
            epoch_schedule: EpochSchedule::default(),
        }));

        {
            let state = state.clone();
            thread::spawn(move || loop {
                thread::sleep(EPOCH_STAKES_QUERY_INTERVAL);
                if let Some((new_update_epoch, update)) =
                    update_state(&mut root_bank_cache, last_update_epoch)
                {
                    last_update_epoch = new_update_epoch;
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
