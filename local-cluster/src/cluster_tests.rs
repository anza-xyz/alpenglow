/// Cluster independent integration tests
///
/// All tests must start from an entry point and a funding keypair and
/// discover the rest of the network.
use log::*;
use {
    crate::{cluster::QuicTpuClient, local_cluster::LocalCluster},
    rand::{thread_rng, Rng},
    rayon::{prelude::*, ThreadPool},
    solana_client::connection_cache::ConnectionCache,
    solana_core::consensus::VOTE_THRESHOLD_DEPTH,
    solana_entry::entry::{self, Entry, EntrySlice},
    solana_gossip::{
        cluster_info::{self, ClusterInfo},
        contact_info::ContactInfo,
        crds::Cursor,
        crds_data::{self, CrdsData},
        crds_value::{CrdsValue, CrdsValueLabel},
        gossip_error::GossipError,
        gossip_service::{self, discover_cluster, GossipService},
    },
    solana_ledger::blockstore::Blockstore,
    solana_rpc_client::rpc_client::RpcClient,
    solana_sdk::{
        clock::{self, Slot},
        commitment_config::CommitmentConfig,
        epoch_schedule::MINIMUM_SLOTS_PER_EPOCH,
        exit::Exit,
        hash::Hash,
        poh_config::PohConfig,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        system_transaction,
        timing::timestamp,
        transaction::Transaction,
        transport::TransportError,
    },
    solana_streamer::socket::SocketAddrSpace,
    solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig, TpuSenderError},
    solana_vote::{
        vote_parser::ParsedVoteTransaction,
        vote_transaction::{self},
    },
    solana_vote_program::vote_state::TowerSync,
    solana_votor_messages::bls_message::BLSMessage,
    std::{
        collections::{HashMap, HashSet, VecDeque},
        net::{SocketAddr, TcpListener, UdpSocket},
        path::Path,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{sleep, JoinHandle},
        time::{Duration, Instant},
    },
};
#[cfg(feature = "dev-context-only-utils")]
use {
    solana_core::consensus::tower_storage::{
        FileTowerStorage, SavedTower, SavedTowerVersions, TowerStorage,
    },
    std::path::PathBuf,
};

/// Spend and verify from every node in the network
pub fn spend_and_verify_all_nodes<S: ::std::hash::BuildHasher + Sync + Send>(
    entry_point_info: &ContactInfo,
    funding_keypair: &Keypair,
    nodes: usize,
    ignore_nodes: HashSet<Pubkey, S>,
    socket_addr_space: SocketAddrSpace,
    connection_cache: &Arc<ConnectionCache>,
) {
    let cluster_nodes = discover_cluster(
        &entry_point_info.gossip().unwrap(),
        nodes,
        socket_addr_space,
    )
    .unwrap();
    assert!(cluster_nodes.len() >= nodes);
    let ignore_nodes = Arc::new(ignore_nodes);
    cluster_nodes.par_iter().for_each(|ingress_node| {
        if ignore_nodes.contains(ingress_node.pubkey()) {
            return;
        }
        let random_keypair = Keypair::new();
        let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
        let bal = client
            .rpc_client()
            .poll_get_balance_with_commitment(
                &funding_keypair.pubkey(),
                CommitmentConfig::processed(),
            )
            .expect("balance in source");
        assert!(bal > 0);
        let (blockhash, _) = client
            .rpc_client()
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .unwrap();
        let mut transaction =
            system_transaction::transfer(funding_keypair, &random_keypair.pubkey(), 1, blockhash);
        let confs = VOTE_THRESHOLD_DEPTH + 1;
        LocalCluster::send_transaction_with_retries(
            &client,
            &[funding_keypair],
            &mut transaction,
            10,
            confs,
        )
        .unwrap();
        for validator in &cluster_nodes {
            if ignore_nodes.contains(validator.pubkey()) {
                continue;
            }
            let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
            client
                .rpc_client()
                .poll_for_signature_confirmation(&transaction.signatures[0], confs)
                .unwrap();
        }
    });
}

pub fn verify_balances<S: ::std::hash::BuildHasher>(
    expected_balances: HashMap<Pubkey, u64, S>,
    node: &ContactInfo,
    connection_cache: Arc<ConnectionCache>,
) {
    let client = new_tpu_quic_client(node, connection_cache.clone()).unwrap();
    for (pk, b) in expected_balances {
        let bal = client
            .rpc_client()
            .poll_get_balance_with_commitment(&pk, CommitmentConfig::processed())
            .expect("balance in source");
        assert_eq!(bal, b);
    }
}

pub fn send_many_transactions(
    node: &ContactInfo,
    funding_keypair: &Keypair,
    connection_cache: &Arc<ConnectionCache>,
    max_tokens_per_transfer: u64,
    num_txs: u64,
) -> HashMap<Pubkey, u64> {
    let client = new_tpu_quic_client(node, connection_cache.clone()).unwrap();
    let mut expected_balances = HashMap::new();
    for _ in 0..num_txs {
        let random_keypair = Keypair::new();
        let bal = client
            .rpc_client()
            .poll_get_balance_with_commitment(
                &funding_keypair.pubkey(),
                CommitmentConfig::processed(),
            )
            .expect("balance in source");
        assert!(bal > 0);
        let (blockhash, _) = client
            .rpc_client()
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .unwrap();
        let transfer_amount = thread_rng().gen_range(1..max_tokens_per_transfer);

        let mut transaction = system_transaction::transfer(
            funding_keypair,
            &random_keypair.pubkey(),
            transfer_amount,
            blockhash,
        );

        LocalCluster::send_transaction_with_retries(
            &client,
            &[funding_keypair],
            &mut transaction,
            5,
            0,
        )
        .unwrap();

        expected_balances.insert(random_keypair.pubkey(), transfer_amount);
    }

    expected_balances
}

pub fn verify_ledger_ticks(ledger_path: &Path, ticks_per_slot: usize) {
    let ledger = Blockstore::open(ledger_path).unwrap();
    let thread_pool = entry::thread_pool_for_tests();

    let zeroth_slot = ledger.get_slot_entries(0, 0).unwrap();
    let last_id = zeroth_slot.last().unwrap().hash;
    let next_slots = ledger.get_slots_since(&[0]).unwrap().remove(&0).unwrap();
    let mut pending_slots: Vec<_> = next_slots
        .into_iter()
        .map(|slot| (slot, 0, last_id))
        .collect();
    while let Some((slot, parent_slot, last_id)) = pending_slots.pop() {
        let next_slots = ledger
            .get_slots_since(&[slot])
            .unwrap()
            .remove(&slot)
            .unwrap();

        // If you're not the last slot, you should have a full set of ticks
        let should_verify_ticks = if !next_slots.is_empty() {
            Some((slot - parent_slot) as usize * ticks_per_slot)
        } else {
            None
        };

        let last_id = verify_slot_ticks(&ledger, &thread_pool, slot, &last_id, should_verify_ticks);
        pending_slots.extend(
            next_slots
                .into_iter()
                .map(|child_slot| (child_slot, slot, last_id)),
        );
    }
}

pub fn sleep_n_epochs(
    num_epochs: f64,
    config: &PohConfig,
    ticks_per_slot: u64,
    slots_per_epoch: u64,
) {
    let num_ticks_per_second = config.target_tick_duration.as_secs_f64().recip();
    let num_ticks_to_sleep = num_epochs * ticks_per_slot as f64 * slots_per_epoch as f64;
    let secs = ((num_ticks_to_sleep + num_ticks_per_second - 1.0) / num_ticks_per_second) as u64;
    warn!("sleep_n_epochs: {} seconds", secs);
    sleep(Duration::from_secs(secs));
}

pub fn kill_entry_and_spend_and_verify_rest(
    entry_point_info: &ContactInfo,
    entry_point_validator_exit: &Arc<RwLock<Exit>>,
    funding_keypair: &Keypair,
    connection_cache: &Arc<ConnectionCache>,
    nodes: usize,
    slot_millis: u64,
    socket_addr_space: SocketAddrSpace,
) {
    info!("kill_entry_and_spend_and_verify_rest...");

    // Ensure all nodes have spun up and are funded.
    let cluster_nodes = discover_cluster(
        &entry_point_info.gossip().unwrap(),
        nodes,
        socket_addr_space,
    )
    .unwrap();
    assert!(cluster_nodes.len() >= nodes);
    let client = new_tpu_quic_client(entry_point_info, connection_cache.clone()).unwrap();
    for ingress_node in &cluster_nodes {
        client
            .rpc_client()
            .poll_get_balance_with_commitment(ingress_node.pubkey(), CommitmentConfig::processed())
            .unwrap_or_else(|err| panic!("Node {} has no balance: {}", ingress_node.pubkey(), err));
    }

    // Kill the entry point node and wait for it to die.
    info!("killing entry point: {}", entry_point_info.pubkey());
    entry_point_validator_exit.write().unwrap().exit();
    info!("sleeping for some time to let entry point exit and partitions to resolve...");
    sleep(Duration::from_millis(slot_millis * MINIMUM_SLOTS_PER_EPOCH));
    info!("done sleeping");

    // Ensure all other nodes are still alive and able to ingest and confirm
    // transactions.
    for ingress_node in &cluster_nodes {
        if ingress_node.pubkey() == entry_point_info.pubkey() {
            info!("ingress_node.id == entry_point_info.id, continuing...");
            continue;
        }

        // Ensure the current ingress node is still funded.
        let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
        let balance = client
            .rpc_client()
            .poll_get_balance_with_commitment(
                &funding_keypair.pubkey(),
                CommitmentConfig::processed(),
            )
            .expect("balance in source");
        assert_ne!(balance, 0);

        let mut result = Ok(());
        let mut retries = 0;

        // Retry sending a transaction to the current ingress node until it is
        // observed by the entire cluster or we exhaust all retries.
        loop {
            retries += 1;
            if retries > 5 {
                result.unwrap();
            }

            // Send a simple transfer transaction to the current ingress node.
            let random_keypair = Keypair::new();
            let (blockhash, _) = client
                .rpc_client()
                .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
                .unwrap();
            let mut transaction = system_transaction::transfer(
                funding_keypair,
                &random_keypair.pubkey(),
                1,
                blockhash,
            );
            let confs = VOTE_THRESHOLD_DEPTH + 1;
            let sig = {
                let sig = LocalCluster::send_transaction_with_retries(
                    &client,
                    &[funding_keypair],
                    &mut transaction,
                    5,
                    confs,
                );
                match sig {
                    Err(e) => {
                        result = Err(e);
                        continue;
                    }

                    Ok(sig) => sig,
                }
            };

            // Ensure all non-entry point nodes are able to confirm the
            // transaction.
            info!("poll_all_nodes_for_signature()");
            match poll_all_nodes_for_signature(
                entry_point_info,
                &cluster_nodes,
                connection_cache,
                &sig,
                confs,
            ) {
                Err(e) => {
                    info!("poll_all_nodes_for_signature() failed {:?}", e);
                    result = Err(e);
                }
                Ok(()) => {
                    info!("poll_all_nodes_for_signature() succeeded, done.");
                    break;
                }
            }
        }
    }
}

#[cfg(feature = "dev-context-only-utils")]
pub fn apply_votes_to_tower(node_keypair: &Keypair, votes: Vec<(Slot, Hash)>, tower_path: PathBuf) {
    let tower_storage = FileTowerStorage::new(tower_path);
    let mut tower = tower_storage.load(&node_keypair.pubkey()).unwrap();
    for (slot, hash) in votes {
        tower.record_vote(slot, hash);
    }
    let saved_tower = SavedTowerVersions::from(SavedTower::new(&tower, node_keypair).unwrap());
    tower_storage.store(&saved_tower).unwrap();
}

pub fn check_min_slot_is_rooted(
    min_slot: Slot,
    contact_infos: &[ContactInfo],
    connection_cache: &Arc<ConnectionCache>,
    test_name: &str,
) {
    let mut last_print = Instant::now();
    let loop_start = Instant::now();
    let loop_timeout = Duration::from_secs(180);
    for ingress_node in contact_infos.iter() {
        let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
        loop {
            let root_slot = client
                .rpc_client()
                .get_slot_with_commitment(CommitmentConfig::finalized())
                .unwrap_or(0);
            if root_slot >= min_slot || last_print.elapsed().as_secs() > 3 {
                info!(
                    "{} waiting for node {} to see root >= {}.. observed latest root: {}",
                    test_name,
                    ingress_node.pubkey(),
                    min_slot,
                    root_slot
                );
                last_print = Instant::now();
                if root_slot >= min_slot {
                    break;
                }
            }
            sleep(Duration::from_millis(clock::DEFAULT_MS_PER_SLOT / 2));
            assert!(loop_start.elapsed() < loop_timeout);
        }
    }
}

pub fn check_for_new_roots(
    num_new_roots: usize,
    contact_infos: &[ContactInfo],
    connection_cache: &Arc<ConnectionCache>,
    test_name: &str,
) {
    check_for_new_commitment_slots(
        num_new_roots,
        contact_infos,
        connection_cache,
        test_name,
        CommitmentConfig::finalized(),
    );
}

/// For alpenglow, CommitmentConfig::processed() refers to the current voting loop slot,
/// so this is more accurate for determining that each node is voting when stake distribution is
/// uneven
pub fn check_for_new_processed(
    num_new_processed: usize,
    contact_infos: &[ContactInfo],
    connection_cache: &Arc<ConnectionCache>,
    test_name: &str,
) {
    check_for_new_commitment_slots(
        num_new_processed,
        contact_infos,
        connection_cache,
        test_name,
        CommitmentConfig::processed(),
    );
}

fn check_for_new_commitment_slots(
    num_new_slots: usize,
    contact_infos: &[ContactInfo],
    connection_cache: &Arc<ConnectionCache>,
    test_name: &str,
    commitment: CommitmentConfig,
) {
    let mut slots = vec![HashSet::new(); contact_infos.len()];
    let mut done = false;
    let mut last_print = Instant::now();
    let loop_start = Instant::now();
    let loop_timeout = Duration::from_secs(180);
    let mut num_slots_map = HashMap::new();
    while !done {
        assert!(loop_start.elapsed() < loop_timeout);

        for (i, ingress_node) in contact_infos.iter().enumerate() {
            let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
            let root_slot = client
                .rpc_client()
                .get_slot_with_commitment(commitment)
                .unwrap_or(0);
            slots[i].insert(root_slot);
            num_slots_map.insert(*ingress_node.pubkey(), slots[i].len());
            let num_slots = slots.iter().map(|r| r.len()).min().unwrap();
            done = num_slots >= num_new_slots;
            if done || last_print.elapsed().as_secs() > 3 {
                info!(
                    "{} waiting for {} new {:?} slots.. observed: {:?}",
                    test_name, num_new_slots, commitment.commitment, num_slots_map
                );
                last_print = Instant::now();
            }
        }
        sleep(Duration::from_millis(clock::DEFAULT_MS_PER_SLOT / 2));
    }
}

pub fn check_no_new_roots(
    num_slots_to_wait: usize,
    contact_infos: &[&ContactInfo],
    connection_cache: &Arc<ConnectionCache>,
    test_name: &str,
) {
    assert!(!contact_infos.is_empty());
    let mut roots = vec![0; contact_infos.len()];
    let max_slot = contact_infos
        .iter()
        .enumerate()
        .map(|(i, ingress_node)| {
            let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
            let initial_root = client
                .rpc_client()
                .get_slot()
                .unwrap_or_else(|_| panic!("get_slot for {} failed", ingress_node.pubkey()));
            roots[i] = initial_root;
            client
                .rpc_client()
                .get_slot_with_commitment(CommitmentConfig::processed())
                .unwrap_or_else(|_| panic!("get_slot for {} failed", ingress_node.pubkey()))
        })
        .max()
        .unwrap();

    let end_slot = max_slot + num_slots_to_wait as u64;
    let mut current_slot;
    let mut last_print = Instant::now();
    let mut reached_end_slot = false;
    loop {
        for contact_info in contact_infos {
            let client = new_tpu_quic_client(contact_info, connection_cache.clone()).unwrap();
            current_slot = client
                .rpc_client()
                .get_slot_with_commitment(CommitmentConfig::processed())
                .unwrap_or_else(|_| panic!("get_slot for {} failed", contact_infos[0].pubkey()));
            if current_slot > end_slot {
                reached_end_slot = true;
                break;
            }
            if last_print.elapsed().as_secs() > 3 {
                info!(
                    "{} current slot: {} on validator: {}, waiting for any validator with slot: {}",
                    test_name,
                    current_slot,
                    contact_info.pubkey(),
                    end_slot
                );
                last_print = Instant::now();
            }
        }
        if reached_end_slot {
            break;
        }
    }

    for (i, ingress_node) in contact_infos.iter().enumerate() {
        let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
        assert_eq!(
            client
                .rpc_client()
                .get_slot()
                .unwrap_or_else(|_| panic!("get_slot for {} failed", ingress_node.pubkey())),
            roots[i]
        );
    }
}

pub fn check_for_new_notarized_votes(
    num_new_votes: usize,
    contact_infos: &[ContactInfo],
    connection_cache: &Arc<ConnectionCache>,
    test_name: &str,
    vote_listener: UdpSocket,
) {
    let loop_start = Instant::now();
    let loop_timeout = Duration::from_secs(180);
    // First get the current max root.
    let Some(current_root) = contact_infos
        .iter()
        .map(|ingress_node| {
            let client = new_tpu_quic_client(ingress_node, connection_cache.clone()).unwrap();
            let root_slot = client
                .rpc_client()
                .get_slot_with_commitment(CommitmentConfig::processed())
                .unwrap_or(0);
            root_slot
        })
        .max()
    else {
        panic!("No nodes found to get current root");
    };

    // Clone data for thread
    let contact_infos_owned: Vec<ContactInfo> = contact_infos.to_vec();
    let test_name_owned = test_name.to_string();

    // Now start vote listener and wait for new notarized votes.
    let vote_listener = std::thread::spawn({
        let mut buf = [0_u8; 65_535];
        let mut num_new_notarized_votes = contact_infos_owned.iter().map(|_| 0).collect::<Vec<_>>();
        let mut last_notarized = contact_infos_owned
            .iter()
            .map(|_| current_root)
            .collect::<Vec<_>>();
        let mut last_print = Instant::now();
        let mut done = false;

        move || {
            while !done {
                assert!(loop_start.elapsed() < loop_timeout);
                let n_bytes = vote_listener
                    .recv_from(&mut buf)
                    .expect("Failed to receive vote message")
                    .0;
                let bls_message = bincode::deserialize::<BLSMessage>(&buf[0..n_bytes]).unwrap();
                let BLSMessage::Vote(vote_message) = bls_message else {
                    continue;
                };
                let vote = vote_message.vote;
                if !vote.is_notarization() {
                    continue;
                }
                let rank = vote_message.rank;
                if rank >= contact_infos_owned.len() as u16 {
                    warn!(
                        "Received vote with rank {} which is greater than number of nodes {}",
                        rank,
                        contact_infos_owned.len()
                    );
                    continue;
                }
                let slot = vote.slot();
                if slot <= last_notarized[rank as usize] {
                    continue;
                }
                last_notarized[rank as usize] = slot;
                num_new_notarized_votes[rank as usize] += 1;
                done = num_new_notarized_votes.iter().all(|&x| x > num_new_votes);
                if done || last_print.elapsed().as_secs() > 3 {
                    info!(
                        "{} waiting for {} new notarized votes.. observed: {:?}",
                        test_name_owned, num_new_votes, num_new_notarized_votes
                    );
                    last_print = Instant::now();
                }
            }
        }
    });
    vote_listener.join().expect("Vote listener thread panicked");
}

fn poll_all_nodes_for_signature(
    entry_point_info: &ContactInfo,
    cluster_nodes: &[ContactInfo],
    connection_cache: &Arc<ConnectionCache>,
    sig: &Signature,
    confs: usize,
) -> Result<(), TransportError> {
    for validator in cluster_nodes {
        if validator.pubkey() == entry_point_info.pubkey() {
            continue;
        }
        let client = new_tpu_quic_client(validator, connection_cache.clone()).unwrap();
        client
            .rpc_client()
            .poll_for_signature_confirmation(sig, confs)?;
    }

    Ok(())
}

/// Represents a service that monitors the gossip network for votes, processes them according to
/// provided filters and callbacks, and maintains a connection to the gossip network. Often used as
/// a "spy" representing a Byzantine node in a cluster.
pub struct GossipVoter {
    pub gossip_service: GossipService,
    pub tcp_listener: Option<TcpListener>,
    pub cluster_info: Arc<ClusterInfo>,
    pub t_voter: JoinHandle<()>,
    pub exit: Arc<AtomicBool>,
}

impl GossipVoter {
    pub fn close(self) {
        self.exit.store(true, Ordering::Relaxed);
        self.t_voter.join().unwrap();
        self.gossip_service.join().unwrap();
    }
}

/// Creates and starts a gossip voter service that monitors the gossip network for votes.
/// This service:
/// 1. Connects to the gossip network at the specified address using the node's keypair
/// 2. Waits for a specified number of peers to join before becoming active
/// 3. Continuously polls for new votes in the network
/// 4. Filters incoming votes through the provided `vote_filter` function
/// 5. Processes filtered votes using the `process_vote_tx` callback
/// 6. Maintains a queue of recent votes and periodically refreshes them
/// 7. Returns a GossipVoter struct that can be used to control and shut down the service
pub fn start_gossip_voter(
    gossip_addr: &SocketAddr,
    node_keypair: &Keypair,
    vote_filter: impl Fn((CrdsValueLabel, Transaction)) -> Option<(ParsedVoteTransaction, Transaction)>
        + std::marker::Send
        + 'static,
    mut process_vote_tx: impl FnMut(Slot, &Transaction, &ParsedVoteTransaction, &ClusterInfo)
        + std::marker::Send
        + 'static,
    sleep_ms: u64,
    num_expected_peers: usize,
    refresh_ms: u64,
    max_votes_to_refresh: usize,
) -> GossipVoter {
    let exit = Arc::new(AtomicBool::new(false));
    let (gossip_service, tcp_listener, cluster_info) = gossip_service::make_gossip_node(
        // Need to use our validator's keypair to gossip EpochSlots and votes for our
        // node later.
        node_keypair.insecure_clone(),
        Some(gossip_addr),
        exit.clone(),
        None,
        0,
        false,
        SocketAddrSpace::Unspecified,
    );

    // Wait for peer discovery
    while cluster_info.gossip_peers().len() < num_expected_peers {
        sleep(Duration::from_millis(sleep_ms));
    }

    let mut latest_voted_slot = 0;
    let mut refreshable_votes: VecDeque<(Transaction, ParsedVoteTransaction)> = VecDeque::new();
    let mut latest_push_attempt = Instant::now();

    let t_voter = {
        let exit = exit.clone();
        let cluster_info = cluster_info.clone();
        std::thread::spawn(move || {
            let mut cursor = Cursor::default();
            loop {
                if exit.load(Ordering::Relaxed) {
                    return;
                }

                let (labels, votes) = cluster_info.get_votes_with_labels(&mut cursor);
                if labels.is_empty() {
                    if latest_push_attempt.elapsed() > Duration::from_millis(refresh_ms) {
                        for (leader_vote_tx, parsed_vote) in refreshable_votes.iter().rev() {
                            let vote_slot = parsed_vote.last_voted_slot().unwrap();
                            info!("gossip voter refreshing vote {}", vote_slot);
                            process_vote_tx(vote_slot, leader_vote_tx, parsed_vote, &cluster_info);
                            latest_push_attempt = Instant::now();
                        }
                    }
                    sleep(Duration::from_millis(sleep_ms));
                    continue;
                }
                let mut parsed_vote_iter: Vec<_> = labels
                    .into_iter()
                    .zip(votes)
                    .filter_map(&vote_filter)
                    .collect();

                parsed_vote_iter.sort_by(|(vote, _), (vote2, _)| {
                    vote.last_voted_slot()
                        .unwrap()
                        .cmp(&vote2.last_voted_slot().unwrap())
                });

                for (parsed_vote, leader_vote_tx) in &parsed_vote_iter {
                    if let Some(vote_slot) = parsed_vote.last_voted_slot() {
                        info!("received vote for {}", vote_slot);
                        if vote_slot > latest_voted_slot {
                            latest_voted_slot = vote_slot;
                            refreshable_votes
                                .push_front((leader_vote_tx.clone(), parsed_vote.clone()));
                            refreshable_votes.truncate(max_votes_to_refresh);
                        }
                        process_vote_tx(vote_slot, leader_vote_tx, parsed_vote, &cluster_info);
                        latest_push_attempt = Instant::now();
                    }
                    // Give vote some time to propagate
                    sleep(Duration::from_millis(sleep_ms));
                }
            }
        })
    };

    GossipVoter {
        gossip_service,
        tcp_listener,
        cluster_info,
        t_voter,
        exit,
    }
}

fn get_and_verify_slot_entries(
    blockstore: &Blockstore,
    thread_pool: &ThreadPool,
    slot: Slot,
    last_entry: &Hash,
) -> Vec<Entry> {
    let entries = blockstore.get_slot_entries(slot, 0).unwrap();
    assert!(entries.verify(last_entry, thread_pool));
    entries
}

fn verify_slot_ticks(
    blockstore: &Blockstore,
    thread_pool: &ThreadPool,
    slot: Slot,
    last_entry: &Hash,
    expected_num_ticks: Option<usize>,
) -> Hash {
    let entries = get_and_verify_slot_entries(blockstore, thread_pool, slot, last_entry);
    let num_ticks: usize = entries.iter().map(|entry| entry.is_tick() as usize).sum();
    if let Some(expected_num_ticks) = expected_num_ticks {
        assert_eq!(num_ticks, expected_num_ticks);
    }
    entries.last().unwrap().hash
}

pub fn submit_vote_to_cluster_gossip(
    node_keypair: &Keypair,
    vote_keypair: &Keypair,
    vote_slot: Slot,
    vote_hash: Hash,
    blockhash: Hash,
    gossip_addr: SocketAddr,
    socket_addr_space: &SocketAddrSpace,
) -> Result<(), GossipError> {
    let tower_sync = TowerSync::new_from_slots(vec![vote_slot], vote_hash, None);
    let vote_tx = vote_transaction::new_tower_sync_transaction(
        tower_sync,
        blockhash,
        node_keypair,
        vote_keypair,
        vote_keypair,
        None,
    );

    cluster_info::push_messages_to_peer_for_tests(
        vec![CrdsValue::new(
            CrdsData::Vote(
                0,
                crds_data::Vote::new(node_keypair.pubkey(), vote_tx, timestamp()).unwrap(),
            ),
            node_keypair,
        )],
        node_keypair.pubkey(),
        gossip_addr,
        socket_addr_space,
    )
}

pub fn new_tpu_quic_client(
    contact_info: &ContactInfo,
    connection_cache: Arc<ConnectionCache>,
) -> Result<QuicTpuClient, TpuSenderError> {
    let rpc_pubsub_url = format!("ws://{}/", contact_info.rpc_pubsub().unwrap());
    let rpc_url = format!("http://{}", contact_info.rpc().unwrap());

    let cache = match &*connection_cache {
        ConnectionCache::Quic(cache) => cache,
        ConnectionCache::Udp(_) => panic!("Expected a Quic ConnectionCache. Got UDP"),
    };

    TpuClient::new_with_connection_cache(
        Arc::new(RpcClient::new(rpc_url)),
        rpc_pubsub_url.as_str(),
        TpuClientConfig::default(),
        cache.clone(),
    )
}
