use {
    crate::{bank::Bank, epoch_stakes::EpochStakes},
    solana_sdk::{clock::Epoch, epoch_schedule::EpochSchedule},
    std::collections::HashMap,
};

pub struct EpochState {
    pub schedule: EpochSchedule,
    pub stakes_map: HashMap<Epoch, EpochStakes>,
    epoch: Epoch,
}

impl EpochState {
    pub fn new(bank: &Bank) -> Self {
        Self {
            schedule: bank.epoch_schedule().clone(),
            stakes_map: bank.epoch_stakes_map().clone(),
            epoch: bank.epoch(),
        }
    }

    pub fn update(&mut self, bank: &Bank) {
        if self.can_update(bank) {
            *self = Self::new(bank)
        }
    }

    pub fn can_update(&self, bank: &Bank) -> bool {
        bank.epoch() > self.epoch
    }
}
