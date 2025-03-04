use {
    crate::banking_stage::LikeClusterInfo,
    itertools::Itertools,
    solana_client::connection_cache::ConnectionCache,
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{ContactInfoQuery, Protocol},
    },
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{clock::FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET, pubkey::Pubkey},
    std::{
        net::SocketAddr,
        sync::{Arc, RwLock},
    },
};

/// Returns a deduplicated list of TVU sockets for all TVU peers, with the next few leaders
/// prioritized first.
pub(crate) fn leader_prioritized_tvu_peer_sockets(
    cluster_info: &ClusterInfo,
    poh_recorder: &RwLock<PohRecorder>,
    connection_cache: &Arc<ConnectionCache>,
    fanout_slots: u64,
) -> Vec<SocketAddr> {
    // Get the keys for the upcoming few leaders
    let poh_recorder = poh_recorder.read().unwrap();
    let leader_keys =
        (0..fanout_slots).filter_map(|n_slots| poh_recorder.leader_after_n_slots(n_slots));

    // Then, get the keys for all eligible validators; this will likely include keys from the
    // upcoming few leaders as well.
    let tvu_peer_keys = cluster_info
        .all_tvu_peers()
        .into_iter()
        .map(|peer| *peer.pubkey());

    // Collect a deduplicated list of all validator sockets we send votes to, prioritizing
    // those associated with leaders for the next few slots.
    leader_keys
        .into_iter()
        .chain(tvu_peer_keys)
        .filter_map(|pubkey| {
            cluster_info
                .lookup_contact_info(&pubkey, |node| node.tpu_vote(connection_cache.protocol()))?
        })
        .unique()
        .collect()
}

pub(crate) fn next_leader_tpu_vote(
    cluster_info: &impl LikeClusterInfo,
    poh_recorder: &RwLock<PohRecorder>,
) -> Option<(Pubkey, SocketAddr)> {
    next_leader(cluster_info, poh_recorder, |node| {
        node.tpu_vote(Protocol::UDP)
    })
}

pub(crate) fn next_leader(
    cluster_info: &impl LikeClusterInfo,
    poh_recorder: &RwLock<PohRecorder>,
    port_selector: impl ContactInfoQuery<Option<SocketAddr>>,
) -> Option<(Pubkey, SocketAddr)> {
    let leader_pubkey = poh_recorder
        .read()
        .unwrap()
        .leader_after_n_slots(FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET)?;
    cluster_info
        .lookup_contact_info(&leader_pubkey, port_selector)?
        .map(|addr| (leader_pubkey, addr))
}
