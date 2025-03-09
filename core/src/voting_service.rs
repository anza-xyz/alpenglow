use {
    crate::{
        alpenglow_consensus::vote_history_storage::{SavedVoteHistoryVersions, VoteHistoryStorage},
        consensus::tower_storage::{SavedTowerVersions, TowerStorage},
        staked_validators_cache::StakedValidatorsCache,
    },
    bincode::serialize,
    crossbeam_channel::Receiver,
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::measure::Measure,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{clock::Slot, transaction::Transaction, transport::TransportError},
    std::{
        net::SocketAddr,
        sync::{Arc, RwLock},
        thread::{self, Builder, JoinHandle},
    },
    thiserror::Error,
};

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

impl VoteOp {
    fn tx(&self) -> &Transaction {
        match self {
            VoteOp::PushVote { tx, .. } => tx,
            VoteOp::PushAlpenglowVote { tx, .. } => tx,
            VoteOp::RefreshVote { tx, .. } => tx,
        }
    }
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
        tower_storage: Arc<dyn TowerStorage>,
        vote_history_storage: Arc<dyn VoteHistoryStorage>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("solVoteService".to_string())
            .spawn(move || {
                let mut staked_validators_cache =
                    StakedValidatorsCache::new(bank_forks.clone(), connection_cache.protocol());

                for vote_op in vote_receiver.iter() {
                    Self::handle_vote(
                        &cluster_info,
                        tower_storage.as_ref(),
                        vote_history_storage.as_ref(),
                        vote_op,
                        connection_cache.clone(),
                        &mut staked_validators_cache,
                    );
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    pub fn handle_vote(
        cluster_info: &ClusterInfo,
        tower_storage: &dyn TowerStorage,
        vote_history_storage: &dyn VoteHistoryStorage,
        vote_op: VoteOp,
        connection_cache: Arc<ConnectionCache>,
        staked_validators_cache: &mut StakedValidatorsCache,
    ) {
        if let VoteOp::PushVote { saved_tower, .. } = &vote_op {
            let mut measure = Measure::start("tower storage save");
            if let Err(err) = tower_storage.store(saved_tower) {
                error!("Unable to save tower to storage: {:?}", err);
                std::process::exit(1);
            }
            measure.stop();
            trace!("{measure}");
        }

        if let VoteOp::PushAlpenglowVote {
            slot,
            saved_vote_history,
            ..
        } = &vote_op
        {
            let mut measure = Measure::start("alpenglow vote history save");
            if let Err(err) = vote_history_storage.store(saved_vote_history) {
                error!("Unable to save vote history to storage: {:?}", err);
                std::process::exit(1);
            }
            measure.stop();
            trace!("{measure}");

            staked_validators_cache.update(*slot, cluster_info);
        }

        // Attempt to send our vote transaction to all other staked validators.
        let staked_validator_tpu_sockets = staked_validators_cache.get_staked_validators();

        if staked_validator_tpu_sockets.is_empty() {
            let _ = send_vote_transaction(cluster_info, vote_op.tx(), None, &connection_cache);
        } else {
            for tpu_vote_socket in staked_validator_tpu_sockets {
                let _ = send_vote_transaction(
                    cluster_info,
                    vote_op.tx(),
                    Some(*tpu_vote_socket),
                    &connection_cache,
                );
            }
        }

        match vote_op {
            VoteOp::PushVote {
                tx, tower_slots, ..
            } => {
                cluster_info.push_vote(&tower_slots, tx);
            }
            VoteOp::PushAlpenglowVote { tx, .. } => {
                // TODO: Test that no important votes are overwritten
                cluster_info.push_alpenglow_vote(tx);
            }
            VoteOp::RefreshVote {
                tx,
                last_voted_slot,
            } => {
                cluster_info.refresh_vote(tx, last_voted_slot);
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
