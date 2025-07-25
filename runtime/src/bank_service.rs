use {
    crate::bank::Bank,
    crossbeam_channel::Receiver,
    log::warn,
    parking_lot::RwLock as PlRwLock,
    std::{sync::Arc, thread},
};

pub struct BankService {
    bank: Arc<PlRwLock<Arc<Bank>>>,
}

impl BankService {
    pub fn new(bank: Arc<Bank>, new_bank_receiver: Receiver<Arc<Bank>>) -> Self {
        let bank = Arc::new(PlRwLock::new(bank));
        {
            let bank = bank.clone();
            thread::spawn(move || loop {
                match new_bank_receiver.recv() {
                    Ok(b) => *bank.write() = b,
                    Err(e) => {
                        warn!("recv() returned {e:?}.  Exiting.");
                        break;
                    }
                };
            });
        }
        Self { bank }
    }

    pub fn get_bank(&self) -> Arc<Bank> {
        self.bank.read().clone()
    }
}
