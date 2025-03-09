use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol};
use solana_pubkey::Pubkey;
use solana_runtime::bank_forks::BankForks;
use solana_sdk::clock::{Epoch, Slot};

/// Maintain `SocketAddr`s associated with all staked validators for a particular epoch and
/// protocol (e.g., UDP, QUIC).
pub struct StakedValidatorsCache {
    /// The epoch for which we have cached our stake validators list
    cached_epoch: Epoch,

    /// The staked validators list for `cached_epoch`
    staked_validator_tpu_sockets: Vec<SocketAddr>,

    /// Bank forks
    bank_forks: Arc<RwLock<BankForks>>,

    /// Protocol
    protocol: Protocol,
}

impl StakedValidatorsCache {
    pub fn new(bank_forks: Arc<RwLock<BankForks>>, protocol: Protocol) -> Self {
        Self {
            cached_epoch: 0_u64,
            staked_validator_tpu_sockets: Vec::default(),
            bank_forks,
            protocol,
        }
    }

    fn refresh(&mut self, slot: Slot, cluster_info: &ClusterInfo) {
        let bank_forks = self.bank_forks.read().unwrap();
        let epoch = self.cached_epoch;

        let epoch_staked_nodes = [bank_forks.root_bank(), bank_forks.working_bank()].iter().find_map(|bank| bank.epoch_staked_nodes(self.cached_epoch)).unwrap_or_else(|| {
            error!("StakedValidatorsCache::get: unknown Bank::epoch_staked_nodes for epoch: {epoch}, slot: {slot}");
            Arc::<HashMap<Pubkey, u64>>::default()
        });

        struct Node {
            stake: u64,
            tpu_socket: SocketAddr,
        }

        let mut nodes: Vec<_> = epoch_staked_nodes
            .iter()
            .filter(|(_, stake)| **stake > 0)
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

        self.staked_validator_tpu_sockets = nodes.into_iter().map(|node| node.tpu_socket).collect();
    }

    pub fn update(&mut self, slot: Slot, cluster_info: &ClusterInfo) {
        let cur_epoch = self
            .bank_forks
            .read()
            .unwrap()
            .working_bank()
            .epoch_schedule()
            .get_epoch(slot);

        if cur_epoch != self.cached_epoch {
            self.cached_epoch = cur_epoch;
            self.refresh(slot, cluster_info);
        }
    }

    pub fn get_staked_validators(&self) -> &[SocketAddr] {
        &self.staked_validator_tpu_sockets
    }
}
