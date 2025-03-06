use {
    crate::banking_stage::LikeClusterInfo,
    solana_gossip::contact_info::{ContactInfoQuery, Protocol},
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{clock::FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET, pubkey::Pubkey},
    std::{net::SocketAddr, sync::RwLock},
};

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
