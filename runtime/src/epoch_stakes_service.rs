use {
    crate::{
        bank::Bank,
        epoch_stakes::{BLSPubkeyToRankMap, EpochStakes},
    },
    crossbeam_channel::Receiver,
    log::warn,
    parking_lot::RwLock as PlRwLock,
    solana_pubkey::Pubkey,
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

impl State {
    fn new(bank: Arc<Bank>) -> Self {
        Self {
            stakes: bank.epoch_stakes_map().clone(),
            epoch_schedule: bank.epoch_schedule().clone(),
        }
    }
}

#[derive(Debug)]
pub enum GetKeyStakesError {
    EpochStakesNotFound,
    PubKeyNotFound,
}

/// A service that regularly updates the epoch stakes state from `Bank`s
/// and exposes various methods to access the state.
pub struct EpochStakesService {
    state: Arc<PlRwLock<State>>,
}

impl EpochStakesService {
    pub fn new(bank: Arc<Bank>, epoch: Epoch, new_bank_receiver: Receiver<Arc<Bank>>) -> Self {
        let mut prev_epoch = epoch;
        let state = Arc::new(PlRwLock::new(State::new(bank)));
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
                let new_epoch = bank.epoch();
                if new_epoch > prev_epoch {
                    prev_epoch = new_epoch;
                    *state.write() = State::new(bank)
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

    pub fn total_stake(&self, slot: Slot) -> Option<u64> {
        let guard = self.state.read();
        let epoch = guard.epoch_schedule.get_epoch(slot);
        guard.stakes.get(&epoch).map(|s| s.total_stake())
    }

    pub fn get_key_and_stakes(
        &self,
        slot: Slot,
        rank: u16,
    ) -> Result<(Pubkey, u64, u64), GetKeyStakesError> {
        let guard = self.state.read();
        let epoch = guard.epoch_schedule.get_epoch(slot);
        let epoch_stakes = guard
            .stakes
            .get(&epoch)
            .ok_or(GetKeyStakesError::EpochStakesNotFound)?;
        let (vote_key, _) = epoch_stakes
            .bls_pubkey_to_rank_map()
            .get_pubkey(rank as usize)
            .ok_or(GetKeyStakesError::PubKeyNotFound)?;
        let stake = epoch_stakes.vote_account_stake(vote_key);
        Ok((*vote_key, stake, epoch_stakes.total_stake()))
    }
}
