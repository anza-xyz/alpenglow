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

impl State {
    fn new(bank: Arc<Bank>) -> Self {
        Self {
            stakes: bank.epoch_stakes_map().clone(),
            epoch_schedule: bank.epoch_schedule().clone(),
        }
    }
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
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::genesis_utils::{
            create_genesis_config_with_alpenglow_vote_accounts_no_program, ValidatorVoteKeypairs,
        },
        crossbeam_channel::unbounded,
        solana_pubkey::Pubkey,
        solana_sdk::clock::DEFAULT_SLOTS_PER_EPOCH,
        std::time::Duration,
        thread::sleep,
    };

    fn get_bank(n_key_pairs: usize) -> Arc<Bank> {
        let validator_keypairs = (0..n_key_pairs)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let stakes_vec = (0..validator_keypairs.len())
            .map(|i| 1_000 - i as u64)
            .collect::<Vec<_>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts_no_program(
            1_000_000_000,
            &validator_keypairs,
            stakes_vec,
        );
        Arc::new(Bank::new_for_tests(&genesis.genesis_config))
    }

    // Ensures that the initial and updated state in the service is as expected.
    #[test]
    fn verify_initial_and_updated_state() {
        let n_key_pairs = 10;
        let bank = get_bank(n_key_pairs);
        let epoch = bank.epoch();
        let slot = bank.slot();
        let (tx, rx) = unbounded();
        let service = Arc::new(EpochStakesService::new(bank.clone(), epoch, rx));
        let map = service.get_key_to_rank_map(slot).unwrap();
        assert_eq!(map.len(), n_key_pairs);

        // Get a new bank sufficiently far in the future to ensure that the epoch has changed.
        let new_bank = Arc::new(Bank::warp_from_parent(
            bank,
            &Pubkey::default(),
            DEFAULT_SLOTS_PER_EPOCH * 2,
            solana_accounts_db::accounts_db::CalcAccountsHashDataSource::IndexForTests,
        ));

        tx.send(new_bank.clone()).unwrap();
        sleep(Duration::from_millis(10));

        let slot = new_bank.slot();
        let map = service.get_key_to_rank_map(slot).unwrap();
        assert_eq!(map.len(), n_key_pairs);
    }
}
