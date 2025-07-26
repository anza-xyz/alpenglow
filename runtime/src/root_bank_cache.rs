use {
    crate::bank::Bank,
    crossbeam_channel::Receiver,
    log::warn,
    parking_lot::RwLock,
    std::{
        sync::{Arc, Weak},
        thread,
    },
};

#[derive(Clone)]
pub struct RootBankCache {
    bank: Arc<RwLock<Arc<Bank>>>,
    weak_bank: Arc<RwLock<Weak<Bank>>>,
}

impl RootBankCache {
    pub fn new(bank: Arc<Bank>, new_bank_receiver: Receiver<Arc<Bank>>) -> Self {
        let weak_bank = Arc::new(RwLock::new(Arc::downgrade(&bank)));
        let bank = Arc::new(RwLock::new(bank));
        {
            let bank = bank.clone();
            thread::spawn(move || loop {
                match new_bank_receiver.recv() {
                    Ok(b) => *bank.write() = b,
                    Err(e) => {
                        warn!("recv() returned {e:?}.  Exiting.");
                        break;
                    }
                }
            });
        }
        Self { bank, weak_bank }
    }

    pub fn root_bank(&self) -> Arc<Bank> {
        if let Some(b) = self.weak_bank.read().upgrade() {
            return b;
        }
        let bank = self.bank.read().clone();
        *self.weak_bank.write() = Arc::downgrade(&bank);
        bank
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            accounts_background_service::AbsRequestSender,
            bank_forks::BankForks,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
        },
        solana_pubkey::Pubkey,
    };

    #[test]
    fn test_root_bank_cache() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank);

        let mut root_bank_cache = RootBankCache::new(bank_forks.clone());

        let bank = bank_forks.read().unwrap().root_bank();
        assert_eq!(bank, root_bank_cache.root_bank());

        {
            let child_bank = Bank::new_from_parent(bank.clone(), &Pubkey::default(), 1);
            bank_forks.write().unwrap().insert(child_bank);

            // cached slot is still 0 since we have not set root
            let cached_root_bank = root_bank_cache.cached_root_bank.upgrade().unwrap();
            assert_eq!(bank.slot(), cached_root_bank.slot());
        }
        {
            bank_forks
                .write()
                .unwrap()
                .set_root(1, &AbsRequestSender::default(), None)
                .unwrap();
            let bank = bank_forks.read().unwrap().root_bank();

            // cached slot and bank are not updated until we call `root_bank()`
            let cached_root_bank = root_bank_cache.cached_root_bank.upgrade().unwrap();
            assert!(bank.slot() != cached_root_bank.slot());
            assert!(bank != cached_root_bank);
            assert_eq!(bank, root_bank_cache.root_bank());

            // cached slot and bank are updated
            let cached_root_bank = root_bank_cache.cached_root_bank.upgrade().unwrap();
            assert_eq!(bank.slot(), cached_root_bank.slot());
            assert_eq!(bank, cached_root_bank);
            assert_eq!(bank, root_bank_cache.root_bank());
        }
    }
}
