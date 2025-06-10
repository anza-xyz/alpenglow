use {
    crate::{
        alpenglow_consensus::vote_history_storage::{SavedVoteHistoryVersions, VoteHistoryStorage},
        consensus::tower_storage::{SavedTowerVersions, TowerStorage},
        next_leader::upcoming_leader_tpu_vote_sockets,
        staked_validators_cache::StakedValidatorsCache,
    },
    bincode::serialize,
    crossbeam_channel::Receiver,
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::measure::Measure,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        clock::{Slot, FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET},
        transaction::Transaction,
        transport::TransportError,
    },
    std::{
        net::SocketAddr,
        sync::{Arc, RwLock},
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    thiserror::Error,
};

const STAKED_VALIDATORS_CACHE_TTL_S: u64 = 5;
const STAKED_VALIDATORS_CACHE_NUM_EPOCH_CAP: usize = 5;

pub enum VoteOp {
    PushVote {
        tx: Transaction,
        tower_slots: Vec<Slot>,
        saved_tower: SavedTowerVersions,
    },
    PushAlpenglowVote {
        tx: Transaction,
        slot: Slot,
        saved_vote_history: SavedVoteHistoryVersions,
    },
    RefreshVote {
        tx: Transaction,
        last_voted_slot: Slot,
    },
}

#[derive(Debug, Error)]
enum SendVoteError {
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),
    #[error("Invalid TPU address")]
    InvalidTpuAddress,
    #[error(transparent)]
    TransportError(#[from] TransportError),
}

fn send_vote_transaction(
    cluster_info: &ClusterInfo,
    transaction: &Transaction,
    tpu: Option<SocketAddr>,
    connection_cache: &Arc<ConnectionCache>,
) -> Result<(), SendVoteError> {
    let tpu = tpu
        .or_else(|| {
            cluster_info
                .my_contact_info()
                .tpu(connection_cache.protocol())
        })
        .ok_or(SendVoteError::InvalidTpuAddress)?;
    let buf = serialize(transaction)?;
    let client = connection_cache.get_connection(&tpu);

    client.send_data_async(buf).map_err(|err| {
        trace!("Ran into an error when sending vote: {err:?} to {tpu:?}");
        SendVoteError::from(err)
    })
}

pub struct VotingService {
    thread_hdl: JoinHandle<()>,
}

impl VotingService {
    pub fn new(
        vote_receiver: Receiver<VoteOp>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        tower_storage: Arc<dyn TowerStorage>,
        vote_history_storage: Arc<dyn VoteHistoryStorage>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
        additional_listeners: Option<Vec<SocketAddr>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("solVoteService".to_string())
            .spawn(move || {
                let mut staked_validators_cache = StakedValidatorsCache::new(
                    bank_forks.clone(),
                    connection_cache.protocol(),
                    Duration::from_secs(STAKED_VALIDATORS_CACHE_TTL_S),
                    STAKED_VALIDATORS_CACHE_NUM_EPOCH_CAP,
                    false,
                );

                for vote_op in vote_receiver.iter() {
                    Self::handle_vote(
                        &cluster_info,
                        &poh_recorder,
                        tower_storage.as_ref(),
                        vote_history_storage.as_ref(),
                        vote_op,
                        connection_cache.clone(),
                        additional_listeners.as_ref(),
                        &mut staked_validators_cache,
                    );
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    fn broadcast_tower_vote(
        cluster_info: &ClusterInfo,
        poh_recorder: &RwLock<PohRecorder>,
        tx: &Transaction,
        connection_cache: &Arc<ConnectionCache>,
    ) {
        // Attempt to send our vote transaction to the leaders for the next few
        // slots. From the current slot to the forwarding slot offset
        // (inclusive).
        const UPCOMING_LEADER_FANOUT_SLOTS: u64 =
            FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET.saturating_add(1);
        #[cfg(test)]
        static_assertions::const_assert_eq!(UPCOMING_LEADER_FANOUT_SLOTS, 3);

        let leader_fanout = UPCOMING_LEADER_FANOUT_SLOTS;

        let upcoming_leader_sockets = upcoming_leader_tpu_vote_sockets(
            cluster_info,
            poh_recorder,
            leader_fanout,
            connection_cache.protocol(),
        );

        if !upcoming_leader_sockets.is_empty() {
            for tpu_vote_socket in upcoming_leader_sockets {
                let _ = send_vote_transaction(
                    cluster_info,
                    tx,
                    Some(tpu_vote_socket),
                    connection_cache,
                );
            }
        } else {
            // Send to our own tpu vote socket if we cannot find a leader to send to
            let _ = send_vote_transaction(cluster_info, tx, None, connection_cache);
        }
    }

    fn broadcast_alpenglow_vote(
        slot: Slot,
        cluster_info: &ClusterInfo,
        tx: &Transaction,
        connection_cache: Arc<ConnectionCache>,
        additional_listeners: Option<&Vec<SocketAddr>>,
        staked_validators_cache: &mut StakedValidatorsCache,
    ) {
        let (staked_validator_tpu_sockets, _) = staked_validators_cache
            .get_staked_validators_by_slot(slot, cluster_info, Instant::now());

        if staked_validator_tpu_sockets.is_empty() {
            let _ = send_vote_transaction(cluster_info, tx, None, &connection_cache);
        } else {
            let sockets = additional_listeners
                .map(|v| v.as_slice())
                .unwrap_or(&[])
                .iter()
                .chain(staked_validator_tpu_sockets.iter());

            for tpu_vote_socket in sockets {
                let _ = send_vote_transaction(
                    cluster_info,
                    tx,
                    Some(*tpu_vote_socket),
                    &connection_cache,
                );
            }
        }
    }

    pub fn handle_vote(
        cluster_info: &ClusterInfo,
        poh_recorder: &RwLock<PohRecorder>,
        tower_storage: &dyn TowerStorage,
        vote_history_storage: &dyn VoteHistoryStorage,
        vote_op: VoteOp,
        connection_cache: Arc<ConnectionCache>,
        additional_listeners: Option<&Vec<SocketAddr>>,
        staked_validators_cache: &mut StakedValidatorsCache,
    ) {
        match vote_op {
            VoteOp::PushVote {
                tx,
                tower_slots,
                saved_tower,
            } => {
                let mut measure = Measure::start("tower storage save");
                if let Err(err) = tower_storage.store(&saved_tower) {
                    error!("Unable to save tower to storage: {:?}", err);
                    std::process::exit(1);
                }
                measure.stop();
                trace!("{measure}");

                Self::broadcast_tower_vote(cluster_info, poh_recorder, &tx, &connection_cache);

                cluster_info.push_vote(&tower_slots, tx);
            }
            VoteOp::PushAlpenglowVote {
                tx,
                slot,
                saved_vote_history,
            } => {
                let mut measure = Measure::start("alpenglow vote history save");
                if let Err(err) = vote_history_storage.store(&saved_vote_history) {
                    error!("Unable to save vote history to storage: {:?}", err);
                    std::process::exit(1);
                }
                measure.stop();
                trace!("{measure}");

                Self::broadcast_alpenglow_vote(
                    slot,
                    cluster_info,
                    &tx,
                    connection_cache,
                    additional_listeners,
                    staked_validators_cache,
                );

                // TODO: Test that no important votes are overwritten
                cluster_info.push_alpenglow_vote(tx);
            }
            VoteOp::RefreshVote {
                tx,
                last_voted_slot,
            } => {
                cluster_info.refresh_vote(tx, last_voted_slot, false);
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
