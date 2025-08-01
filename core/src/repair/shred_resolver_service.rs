//! The shred resolver service listens to events emitted by
//!
//! Blockstore:
//! - FEC set has been ingested or successfully completed
//! - A conflicting shred has been ingested
//!
//! Votor:
//! - A block has reached "canonical" status, where canonical
//!   means that it is the unique block of interest in the slot.
//!   This is a result of the block or a descendant receiving
//!   a Notarize, FastFinalizeation, or SlowFinalization certificate
//! - An alternate version of a block has been requested in order to
//!   progress replay or check safe to notar conditions. We must fetch
//!   these shreds, but it is not yet clear if this is the canonical block.
//!
//! Using these events it plans how to fetch the correct shreds and resolves
//! any incorrect shreds ingested. It interfaces with repair to send out
//! the appropriate requests.

use {
    super::repair_service::OutstandingShredRepairs,
    solana_ledger::{blockstore::Blockstore, shred_event::ShredEventReceiver},
    std::{
        sync::{Arc, RwLock},
        thread::{self, JoinHandle},
    },
};

pub struct ShredResolverService {
    t_listen: JoinHandle<()>,
}

impl ShredResolverService {
    pub fn new(
        _blockstore: Arc<Blockstore>,
        event_receiver: ShredEventReceiver,
        _outstanding_repairs: Arc<RwLock<OutstandingShredRepairs>>,
    ) -> Self {
        let t_listen = thread::spawn(move || loop {
            let Ok(_events) = event_receiver.recv() else {
                break;
            };
        });

        Self { t_listen }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_listen.join()
    }
}
