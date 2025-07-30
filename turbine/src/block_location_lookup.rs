//! Lookup from a repair nonce to the location where this shred should be stored
//! After fetching the shred and verifying that the nonce matches the repair request,
//! we insert the (Nonce, Location) pair here. Once the shred passes sigverify checks,
//! the Location is fetched before insertion into blockstore.
//!
//! This lookup avoids having to pass the location information around, or polluting
//! the OutstandingRequests cache with this extra information and lock contention.

use {
    lru::LruCache,
    solana_ledger::{blockstore_meta::BlockLocation, shred::Nonce},
    std::sync::{Arc, RwLock},
};

pub struct BlockLocationLookup {
    locations: RwLock<LruCache<Nonce, BlockLocation>>,
}

impl BlockLocationLookup {
    pub fn new_arc() -> Arc<Self> {
        let lookup = BlockLocationLookup {
            // Matches OutstandingRequests cache size
            locations: RwLock::new(LruCache::new(16 * 1024)),
        };
        Arc::new(lookup)
    }

    pub fn add_location(&self, nonce: Nonce, location: BlockLocation) {
        self.locations.write().unwrap().put(nonce, location);
    }

    pub fn get_location_and_remove(&self, nonce: Nonce) -> Option<BlockLocation> {
        self.locations.write().unwrap().pop(&nonce)
    }
}
