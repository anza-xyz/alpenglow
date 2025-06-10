use {
    lru::LruCache,
    solana_clock::{Epoch, Slot},
    solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol},
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::BankForks,
    std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

struct StakedValidatorsCacheEntry {
    /// Sockets associated with the staked validators
    validator_sockets: Vec<SocketAddr>,

    /// The time at which this entry was created
    creation_time: Instant,
}

/// Maintain `SocketAddr`s associated with all staked validators for a particular protocol (e.g.,
/// UDP, QUIC) over number of epochs.
///
/// We employ an LRU cache with capped size, mapping Epoch to cache entries that store the socket
/// information. We also track cache entry times, forcing recalculations of cache entries that are
/// accessed after a specified TTL.
pub struct StakedValidatorsCache {
    /// key: the epoch for which we have cached our stake validators list
    /// value: the cache entry
    cache: LruCache<Epoch, StakedValidatorsCacheEntry>,

    /// Time to live for cache entries
    ttl: Duration,

    /// Bank forks
    bank_forks: Arc<RwLock<BankForks>>,

    /// Protocol
    protocol: Protocol,

    /// Whether to include the running validator's socket address in cache entries
    include_self: bool,
}

impl StakedValidatorsCache {
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        protocol: Protocol,
        ttl: Duration,
        max_cache_size: usize,
        include_self: bool,
    ) -> Self {
        Self {
            cache: LruCache::new(max_cache_size),
            ttl,
            bank_forks,
            protocol,
            include_self,
        }
    }

    #[inline]
    fn cur_epoch(&self, slot: Slot) -> Epoch {
        self.bank_forks
            .read()
            .unwrap()
            .working_bank()
            .epoch_schedule()
            .get_epoch(slot)
    }

    fn refresh_cache_entry(
        &mut self,
        epoch: Epoch,
        cluster_info: &ClusterInfo,
        update_time: Instant,
    ) {
        let banks = {
            let bank_forks = self.bank_forks.read().unwrap();
            [bank_forks.root_bank(), bank_forks.working_bank()]
        };

        let epoch_staked_nodes = banks.iter().find_map(|bank| bank.epoch_staked_nodes(epoch)).unwrap_or_else(|| {
            error!("StakedValidatorsCache::get: unknown Bank::epoch_staked_nodes for epoch: {epoch}");
            Arc::<HashMap<Pubkey, u64>>::default()
        });

        struct Node {
            stake: u64,
            tpu_socket: SocketAddr,
        }

        let mut nodes: Vec<_> = epoch_staked_nodes
            .iter()
            .filter(|(pubkey, stake)| {
                let positive_stake = **stake > 0;
                let not_self = pubkey != &&cluster_info.id();

                positive_stake && (self.include_self || not_self)
            })
            .filter_map(|(pubkey, stake)| {
                cluster_info
                    .lookup_contact_info(pubkey, |node| node.tpu_vote(self.protocol))?
                    .map(|socket_addr| Node {
                        stake: *stake,
                        tpu_socket: socket_addr,
                    })
            })
            .collect();

        nodes.dedup_by_key(|node| node.tpu_socket);
        nodes.sort_unstable_by(|a, b| a.stake.cmp(&b.stake));

        let validator_sockets = nodes.into_iter().map(|node| node.tpu_socket).collect();

        self.cache.push(
            epoch,
            StakedValidatorsCacheEntry {
                validator_sockets,
                creation_time: update_time,
            },
        );
    }

    pub fn get_staked_validators_by_slot(
        &mut self,
        slot: Slot,
        cluster_info: &ClusterInfo,
        access_time: Instant,
    ) -> (&[SocketAddr], bool) {
        self.get_staked_validators_by_epoch(self.cur_epoch(slot), cluster_info, access_time)
    }

    pub fn get_staked_validators_by_epoch(
        &mut self,
        epoch: Epoch,
        cluster_info: &ClusterInfo,
        access_time: Instant,
    ) -> (&[SocketAddr], bool) {
        // For a given epoch, if we either:
        //
        // (1) have a cache entry that has expired
        // (2) have no existing cache entry
        //
        // then update the cache.
        let refresh_cache = self
            .cache
            .get(&epoch)
            .map(|v| access_time > v.creation_time + self.ttl)
            .unwrap_or(true);

        if refresh_cache {
            self.refresh_cache_entry(epoch, cluster_info, access_time);
        }

        (
            // Unwrapping is fine here, since update_cache guarantees that we push a cache entry to
            // self.cache[epoch].
            self.cache
                .get(&epoch)
                .map(|v| &*v.validator_sockets)
                .unwrap(),
            refresh_cache,
        )
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

#[cfg(test)]
mod tests {}
