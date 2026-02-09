//! Service responsible for fetching alternate versions of blocks through informed repair.
//! Receives [`RepairEvent`]s from votor / replay and engages in a repair process to fetch
//! the block to the alternate blockstore column.
//!
//! Sends and processes [`BlockIdRepairType`] requests through a separate socket for block and
//! fec set metadata. Additionally sends [`ShredRepairType`] requests through the main socket
//! to fetch shreds.

mod stats;

use {
    super::{
        repair_service::{OutstandingShredRepairs, REPAIR_REQUEST_TIMEOUT_MS},
        serve_repair::{RepairPeers, ServeRepair, ShredRepairType, REPAIR_PEERS_CACHE_CAPACITY},
        standard_repair_handler::StandardRepairHandler,
    },
    crate::repair::{
        outstanding_requests::OutstandingRequests,
        packet_threshold::DynamicPacketToProcessThreshold,
        repair_service::{RepairInfo, RepairStats, REPAIR_MS},
        serve_repair::{
            BlockIdRepairResponse, BlockIdRepairType, RepairProtocol, RepairRequestProtocol,
        },
    },
    crossbeam_channel::{select, unbounded},
    log::{debug, info},
    lru::LruCache,
    solana_clock::Slot,
    solana_gossip::ping_pong::Pong,
    solana_ledger::{
        blockstore::{Blockstore, CompletedSlotsReceiver},
        blockstore_meta::BlockLocation,
        shred::DATA_SHREDS_PER_FEC_BLOCK,
    },
    solana_perf::{
        packet::{deserialize_from_with_limit, PacketRef},
        recycler::Recycler,
    },
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::SharableBanks,
    solana_streamer::{
        sendmmsg::{batch_send, SendPktsError},
        streamer::{self, PacketBatchReceiver, StreamerReceiveStats},
    },
    solana_time_utils::timestamp,
    solana_votor::event::{RepairEvent, RepairEventReceiver},
    solana_votor_messages::{consensus_message::Block, migration::MigrationStatus},
    stats::{BlockIdRepairRequestsStats, BlockIdRepairResponsesStats},
    std::{
        collections::{BinaryHeap, HashMap, HashSet},
        io::Cursor,
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

type OutstandingBlockIdRepairs = OutstandingRequests<BlockIdRepairType>;

const MAX_REPAIR_REQUESTS_PER_ITERATION: usize = 200;

/// The type of requests that this service will send
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum RepairRequest {
    /// Pong response to a ping challenge.
    /// Contains serialized pong bytes and destination address.
    /// The original request will be retried by retry_timed_out_requests.
    Pong {
        pong_bytes: Vec<u8>,
        addr: SocketAddr,
    },

    /// Metadata requests
    Metadata(BlockIdRepairType),

    /// Shred request
    Shred(ShredRepairType),
}

impl RepairRequest {
    fn slot(&self) -> Slot {
        match self {
            // Pong is always highest priority and handled separately in Ord,
            // so this should never be called. Return 0 as a fallback.
            RepairRequest::Pong { .. } => unimplemented!("Pong requests do not have a slot"),
            RepairRequest::Metadata(block_id_repair_type) => block_id_repair_type.slot(),
            RepairRequest::Shred(shred_repair_type) => shred_repair_type.slot(),
        }
    }
}

/// We prioritize Pong first (to respond to ping challenges), then requests with
/// lower slot #s, and then prefer metadata requests before shred requests.
impl Ord for RepairRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use {
            std::cmp::Ordering, BlockIdRepairType::*, RepairRequest::*,
            ShredRepairType::ShredForBlockId,
        };

        // pong has highest priority - must respond to ping challenges immediately
        match (&self, &other) {
            (Pong { .. }, Pong { .. }) => return Ordering::Equal,
            (Pong { .. }, _) => return Ordering::Greater,
            (_, Pong { .. }) => return Ordering::Less,
            _ => {}
        }

        if self.slot() != other.slot() {
            // lower slot is higher priority
            return other.slot().cmp(&self.slot());
        }

        match (&self, &other) {
            // prioritize metadata requests
            (Metadata(_), Shred(_)) => Ordering::Greater,
            (Shred(_), Metadata(_)) => Ordering::Less,

            // prioritize top level metadata request
            (Metadata(ParentAndFecSetCount { .. }), _) => Ordering::Greater,
            (_, Metadata(ParentAndFecSetCount { .. })) => Ordering::Less,

            // prioritize lower shred indices
            (
                Metadata(FecSetRoot {
                    fec_set_index: a, ..
                }),
                Metadata(FecSetRoot {
                    fec_set_index: b, ..
                }),
            ) => b.cmp(a),
            (Shred(ShredForBlockId { index: a, .. }), Shred(ShredForBlockId { index: b, .. })) => {
                b.cmp(a)
            }

            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for RepairRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct RepairState {
    /// Request builder
    serve_repair: ServeRepair,
    /// Repair peers cache
    peers_cache: LruCache<u64, RepairPeers>,

    /// Metadata requests sent to the cluster
    outstanding_requests: OutstandingBlockIdRepairs,
    /// Shred requests sent to the cluster
    outstanding_shred_requests: Arc<RwLock<OutstandingShredRepairs>>,

    /// Repair requests waiting to be sent to the cluster
    pending_repair_requests: BinaryHeap<RepairRequest>,

    /// Repair events that are pending because Turbine/Eager repair hasn't completed yet.
    /// These are re-processed each iteration until Turbine/Eager repair completes or marks the slot dead.
    pending_repair_events: Vec<RepairEvent>,

    /// Requests that have been sent, mapped to the timestamp they were sent.
    /// Used for retry logic - requests that exceed REPAIR_REQUEST_TIMEOUT_MS
    /// are moved back to pending_repair_requests. We track this separately from the
    /// outstanding_requests maps as those are used for verifying response validity.
    sent_requests: HashMap<RepairRequest, u64>,

    /// Blocks we've previously requested. Used to avoid re-initiating repair for an in progress block.
    requested_blocks: HashSet<Block>,

    // Stats
    response_stats: BlockIdRepairResponsesStats,
    request_stats: BlockIdRepairRequestsStats,
}

pub struct BlockIdRepairChannels {
    pub repair_event_receiver: RepairEventReceiver,
    pub completed_slots_receiver: CompletedSlotsReceiver,
}

pub struct BlockIdRepairService {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl BlockIdRepairService {
    pub fn new(
        exit: Arc<AtomicBool>,
        blockstore: Arc<Blockstore>,
        block_id_repair_socket: Arc<UdpSocket>,
        repair_socket: Arc<UdpSocket>,
        block_id_repair_channels: BlockIdRepairChannels,
        repair_info: RepairInfo,
        outstanding_shred_requests: Arc<RwLock<OutstandingShredRepairs>>,
    ) -> Self {
        let (response_sender, response_receiver) = unbounded();

        let BlockIdRepairChannels {
            repair_event_receiver,
            completed_slots_receiver,
        } = block_id_repair_channels;

        // UDP receiver thread
        let t_receiver = streamer::receiver(
            "solRcvrBlockId".to_string(),
            block_id_repair_socket.clone(),
            exit.clone(),
            response_sender.clone(),
            Recycler::default(),
            Arc::new(StreamerReceiveStats::new(
                "block_id_repair_response_receiver",
            )),
            None,  // coalesce
            false, // use_pinned_memory
            None,  // in_vote_only_mode
            false, // is_staked_service
        );

        let t_block_id_repair = Self::run(
            exit,
            response_receiver,
            repair_event_receiver,
            completed_slots_receiver,
            blockstore,
            block_id_repair_socket,
            repair_socket,
            repair_info,
            outstanding_shred_requests,
        );

        Self {
            thread_hdls: vec![t_receiver, t_block_id_repair],
        }
    }

    /// Main thread that processes responses and sends requests
    /// - Listens for responses to our block ID repair requests
    /// - Listens for new repair events from votor / replay
    /// - Generates new block id repair requests to send to the cluster
    #[allow(clippy::too_many_arguments)]
    fn run(
        exit: Arc<AtomicBool>,
        response_receiver: PacketBatchReceiver,
        repair_event_receiver: RepairEventReceiver,
        completed_slots_receiver: CompletedSlotsReceiver,
        blockstore: Arc<Blockstore>,
        block_id_repair_socket: Arc<UdpSocket>,
        repair_socket: Arc<UdpSocket>,
        repair_info: RepairInfo,
        outstanding_shred_requests: Arc<RwLock<OutstandingShredRepairs>>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("solBlockIdRep".to_string())
            .spawn(move || {
                info!("BlockIdRepairService started");
                let sharable_banks = repair_info.bank_forks.read().unwrap().sharable_banks();
                let mut state = RepairState {
                    // One day we'll actually split out the build request functionality from the full ServeRepair :'(
                    serve_repair: ServeRepair::new(
                        repair_info.cluster_info.clone(),
                        sharable_banks.clone(),
                        repair_info.repair_whitelist.clone(),
                        Box::new(StandardRepairHandler::new(blockstore.clone())),
                        // Doesn't matter this serve_repair isn't used to respond
                        Arc::new(MigrationStatus::default()),
                    ),
                    peers_cache: LruCache::new(REPAIR_PEERS_CACHE_CAPACITY),
                    outstanding_requests: OutstandingBlockIdRepairs::default(),
                    outstanding_shred_requests,
                    pending_repair_requests: BinaryHeap::default(),
                    sent_requests: HashMap::default(),
                    requested_blocks: HashSet::default(),
                    pending_repair_events: Vec::default(),
                    response_stats: BlockIdRepairResponsesStats::default(),
                    request_stats: BlockIdRepairRequestsStats::default(),
                };

                let mut last_stats_report = Instant::now();
                // throttle starts at 1024 responses => 1 second of compute
                let mut throttle = DynamicPacketToProcessThreshold::default();

                while !exit.load(Ordering::Relaxed) {
                    if last_stats_report.elapsed().as_secs() >= 10 {
                        state.response_stats.report();
                        state.request_stats.report();
                        last_stats_report = Instant::now();
                    }

                    if !state.pending_repair_requests.is_empty() {
                        // We have requests we need to send out, minimal sleep
                        std::thread::sleep(Duration::from_millis(REPAIR_MS));
                    } else {
                        // Otherwise wait for either a new block repair request from votor,
                        // or wait for a turbine block full event from replay,
                        // for repair events that are currently deferred
                        select! {
                            recv(completed_slots_receiver) -> result => match result {
                                Ok(_) => (),
                                Err(_) => break,
                            },
                            recv(repair_event_receiver) -> result => match result {
                                Ok(event) => state.pending_repair_events.push(event),
                                Err(_) => break,
                            },
                            default(Duration::from_secs(1)) => continue,
                        }
                    }

                    let root = sharable_banks.root().slot();

                    // Clean up old request tracking
                    state.requested_blocks.retain(|(slot, _)| *slot > root);

                    // Process responses (including pings), generate new requests / repair events
                    Self::process_responses(
                        repair_info.cluster_info.id(),
                        &response_receiver,
                        &mut state,
                        &mut throttle,
                        &repair_info.cluster_info.keypair(),
                    );

                    // Receive new repair events from votor / replay
                    state
                        .pending_repair_events
                        .extend(repair_event_receiver.try_iter());

                    // Generate repair requests for repair events
                    let events_to_process = std::mem::take(&mut state.pending_repair_events);
                    for event in events_to_process {
                        Self::process_repair_event(
                            repair_info.cluster_info.id(),
                            event,
                            &sharable_banks,
                            &blockstore,
                            &mut state,
                        );
                    }

                    // Retry requests that have timed out
                    Self::retry_timed_out_requests(&blockstore, &mut state, timestamp());

                    // Send out new requests
                    Self::send_requests(
                        &block_id_repair_socket,
                        &repair_socket,
                        &repair_info,
                        sharable_banks.root().slot(),
                        &mut state,
                    );
                }

                info!("BlockIdRepairService shutting down");
            })
            .unwrap()
    }

    /// Process any pending responses from the response receiver and generate any new requests
    fn process_responses(
        my_pubkey: Pubkey,
        response_receiver: &PacketBatchReceiver,
        state: &mut RepairState,
        throttle: &mut DynamicPacketToProcessThreshold,
        keypair: &solana_keypair::Keypair,
    ) {
        let Ok(packet_batch) = response_receiver.try_recv() else {
            return;
        };
        let mut packet_batches = vec![packet_batch];

        // Throttle
        let mut total_packets = packet_batches[0].len();
        let mut dropped_packets = 0;
        while let Ok(batch) = response_receiver.try_recv() {
            total_packets += batch.len();
            if throttle.should_drop(total_packets) {
                dropped_packets += batch.len();
            } else {
                packet_batches.push(batch);
            }
        }
        state.response_stats.dropped_packets += dropped_packets;
        state.response_stats.total_packets += total_packets;

        // Process all responses (including pings, which will generate pong requests)
        let compute_timer = Instant::now();
        packet_batches
            .iter()
            .flat_map(|packet_batch| packet_batch.iter())
            .for_each(|packet| {
                Self::process_block_id_repair_response(&my_pubkey, packet, keypair, state);
            });

        // adjust throttle based on actual compute time
        throttle.update(total_packets, compute_timer.elapsed());
    }

    /// Process a response:
    /// - Sanity checks on deserialization
    /// - Verify repair nonce
    /// - Queue more repair requests or events
    fn process_block_id_repair_response(
        my_pubkey: &Pubkey,
        packet: PacketRef<'_>,
        keypair: &solana_keypair::Keypair,
        state: &mut RepairState,
    ) {
        let Some(packet_data) = packet.data(..) else {
            state.response_stats.invalid_packets += 1;
            return;
        };
        let mut cursor = Cursor::new(packet_data);
        let Ok(response) = deserialize_from_with_limit::<_, BlockIdRepairResponse>(&mut cursor)
            .inspect_err(|e| {
                debug!("Failed to deserialize response: {e:?}");
            })
        else {
            state.response_stats.invalid_packets += 1;
            return;
        };

        // Ping -> send Pong
        if let BlockIdRepairResponse::Ping { ping } = response {
            let addr = packet.meta().socket_addr();
            let pong = Pong::new(&ping, keypair);
            let pong_protocol = RepairProtocol::Pong(pong);
            let pong_bytes =
                bincode::serialize(&pong_protocol).expect("Pong serialization cannot fail");

            debug!("{my_pubkey}: Received ping challenge from {addr}, queueing pong");

            state
                .pending_repair_requests
                .push(RepairRequest::Pong { pong_bytes, addr });

            state.response_stats.ping_responses += 1;
            return;
        }

        // For non-Ping responses, deserialize the nonce
        let nonce: u32 = match deserialize_from_with_limit(&mut cursor) {
            Ok(n) => n,
            Err(e) => {
                debug!("{my_pubkey}: Failed to deserialize nonce: {e:?}");
                state.response_stats.invalid_packets += 1;
                return;
            }
        };

        debug!("{my_pubkey}: Received response: {response:?}, nonce={nonce}");

        let Some(request) =
            // verify the response (and check merkle proof validity)
            state.outstanding_requests.register_response(
                nonce,
                &response,
                timestamp(),
                // If valid return the original request
                |block_id_request| *block_id_request,
            )
        else {
            debug!(
                "{my_pubkey}: Response with invalid nonce {nonce} or failed verification for {response:?}"
            );
            state.response_stats.invalid_packets += 1;
            return;
        };

        debug!("{my_pubkey}: Received valid response for request {request:?}");

        // Remove from sent_requests since we got a response
        state
            .sent_requests
            .remove(&RepairRequest::Metadata(request));

        let (slot, block_id) = request.block();

        match response {
            BlockIdRepairResponse::ParentFecSetCount {
                fec_set_count,
                parent_info: (p_slot, p_block_id),
                parent_proof: _,
            } => {
                // Queue a request to repair the parent (filtered out later if we already have the parent)
                state.pending_repair_events.push(RepairEvent::FetchBlock {
                    slot: p_slot,
                    block_id: p_block_id,
                });

                // Queue FecSetRoot requests
                state
                    .pending_repair_requests
                    .extend((0..fec_set_count as u32).map(|i| {
                        let fec_set_index = i * DATA_SHREDS_PER_FEC_BLOCK as u32;
                        RepairRequest::Metadata(BlockIdRepairType::FecSetRoot {
                            slot,
                            block_id,
                            fec_set_index,
                        })
                    }));

                state.response_stats.parent_fec_set_count_responses += 1;
            }

            BlockIdRepairResponse::FecSetRoot {
                fec_set_root: fec_set_merkle_root,
                ..
            } => {
                let BlockIdRepairType::FecSetRoot { fec_set_index, .. } = request else {
                    panic!(
                        "{my_pubkey}: Programmer error, *verified* response was FecSetRoot but \
                         request was not"
                    );
                };
                let start_index = fec_set_index;
                let end_index = fec_set_index + DATA_SHREDS_PER_FEC_BLOCK as u32;

                // Queue ShredForBlockId requests
                state
                    .pending_repair_requests
                    .extend((start_index..end_index).map(|index| {
                        RepairRequest::Shred(ShredRepairType::ShredForBlockId {
                            slot,
                            index,
                            fec_set_merkle_root,
                            block_id,
                        })
                    }));

                state.response_stats.fec_set_root_responses += 1;
            }

            BlockIdRepairResponse::Ping { .. } => {
                unreachable!("Ping handled above")
            }
        }

        state.response_stats.processed += 1;
    }

    /// Process a repair event and generate any requests
    fn process_repair_event(
        my_pubkey: Pubkey,
        event: RepairEvent,
        sharable_banks: &SharableBanks,
        blockstore: &Blockstore,
        state: &mut RepairState,
    ) {
        let root = sharable_banks.root().slot();

        if event.slot() <= root {
            return;
        }

        match event {
            RepairEvent::FetchBlock { slot, block_id } => {
                if state.requested_blocks.contains(&(slot, block_id)) {
                    return;
                }

                // Check if we already have the block, if so queue fetching the parent
                // Note: when a block becomes full in blockstore -> we atomically calculate the DMR and populate location
                if let Some(location) = blockstore.get_block_location(slot, block_id) {
                    Self::queue_fetch_parent_block(blockstore, slot, location, state);
                    return;
                }

                // We don't have the block. Check if turbine failed (dead)
                // Note: we require the invariant that Turbine + Eager repair will either:
                // - Eventually fill in all shreds for a slot (slot_meta.is_full()) resulting in the DMR calculation
                // - Mark the slot as dead
                if blockstore.is_dead(slot) {
                    info!(
                        "{my_pubkey}: FetchBlock: slot {slot} is dead, starting repair for \
                         block_id={block_id:?}"
                    );
                    state.pending_repair_requests.push(RepairRequest::Metadata(
                        BlockIdRepairType::ParentAndFecSetCount { slot, block_id },
                    ));
                    state.requested_blocks.insert((slot, block_id));
                    return;
                }

                // Turbine did not fail, check the progress
                match blockstore.get_double_merkle_root(slot, BlockLocation::Original) {
                    None => {
                        // Turbine has not completed, defer and check again later
                        debug!(
                            "{my_pubkey}: FetchBlock: Turbine not complete for slot {slot}, \
                             deferring"
                        );
                        state
                            .pending_repair_events
                            .push(RepairEvent::FetchBlock { slot, block_id });
                    }
                    Some(turbine_block_id) if turbine_block_id != block_id => {
                        // Turbine has a different block
                        warn!(
                            "{my_pubkey}: FetchBlock: Turbine has different block \
                             {turbine_block_id:?} vs requested {block_id:?} for slot {slot}, \
                             starting repair"
                        );
                        state.pending_repair_requests.push(RepairRequest::Metadata(
                            BlockIdRepairType::ParentAndFecSetCount { slot, block_id },
                        ));
                        state.requested_blocks.insert((slot, block_id));
                    }
                    Some(_) => {
                        // Turbine completed between when we checked for the block above and here
                        // Queue the parent
                        debug!(
                            "{my_pubkey}: FetchBlock: Turbine has correct block for slot {slot}, \
                             fetching parent"
                        );
                        Self::queue_fetch_parent_block(
                            blockstore,
                            slot,
                            BlockLocation::Original,
                            state,
                        );
                    }
                }
            }
        }
    }

    /// Helper to fetch the parent block for a slot we already have
    fn queue_fetch_parent_block(
        blockstore: &Blockstore,
        slot: Slot,
        location: BlockLocation,
        state: &mut RepairState,
    ) {
        debug_assert!(blockstore
            .meta_from_location(slot, location)
            .unwrap()
            .unwrap()
            .is_full());
        let parent = blockstore
            .get_parent_meta(slot, location)
            .unwrap()
            .expect("ParentMeta must be populated for full slots");

        state.pending_repair_events.push(RepairEvent::FetchBlock {
            slot: parent.parent_slot,
            block_id: parent.parent_block_id,
        });
    }

    /// Check for requests that have timed out and move them back to pending_repair_requests.
    /// For shred requests, we check if the shred has been received before retrying.
    fn retry_timed_out_requests(blockstore: &Blockstore, state: &mut RepairState, now: u64) {
        state.sent_requests.retain(|request, sent_time| {
            if now.saturating_sub(*sent_time) >= REPAIR_REQUEST_TIMEOUT_MS {
                match request {
                    RepairRequest::Pong { .. } => {}
                    RepairRequest::Metadata(_) => {
                        // Metadata requests: always retry on timeout
                        state.pending_repair_requests.push(request.clone());
                    }
                    // Since shred responses are sent to a different socket, we need to check
                    // blockstore to see if this expired request is actually expired, or if the
                    // shred has already been ingested
                    RepairRequest::Shred(shred_request) => {
                        if !Self::has_received_shred(blockstore, shred_request) {
                            state.pending_repair_requests.push(request.clone());
                        }
                    }
                }
                false
            } else {
                true
            }
        });
    }

    /// Check if we have received a shred for a ShredForBlockId request.
    /// Returns true if the shred exists in the blockstore's alternate index.
    fn has_received_shred(blockstore: &Blockstore, request: &ShredRepairType) -> bool {
        let ShredRepairType::ShredForBlockId {
            slot,
            index,
            block_id,
            ..
        } = request
        else {
            return false;
        };

        let location = BlockLocation::Alternate {
            block_id: *block_id,
        };
        blockstore
            .get_index_from_location(*slot, location)
            .ok()
            .flatten()
            .map(|idx| idx.data().contains(*index as u64))
            .unwrap_or(false)
    }

    /// Drain the pending requests and send them out to the cluster
    fn send_requests(
        block_id_repair_socket: &UdpSocket,
        repair_socket: &UdpSocket,
        repair_info: &RepairInfo,
        root: Slot,
        state: &mut RepairState,
    ) {
        let pending_count = state.pending_repair_requests.len();
        let max_batch_len = pending_count.min(MAX_REPAIR_REQUESTS_PER_ITERATION);
        let mut block_id_socket_batch: Vec<(Vec<u8>, SocketAddr)> =
            Vec::with_capacity(max_batch_len);
        let mut shred_socket_batch = Vec::with_capacity(max_batch_len);

        let root_bank = repair_info.bank_forks.read().unwrap().root_bank();
        let staked_nodes = root_bank.current_epoch_staked_nodes();
        let now = timestamp();

        while block_id_socket_batch
            .len()
            .saturating_add(shred_socket_batch.len())
            < MAX_REPAIR_REQUESTS_PER_ITERATION
        {
            let Some(request) = state.pending_repair_requests.pop() else {
                break;
            };

            // Skip slot check for Pong (always process) but check for other requests
            if !matches!(request, RepairRequest::Pong { .. }) && request.slot() <= root {
                continue;
            }

            match request {
                RepairRequest::Pong { pong_bytes, addr } => {
                    // Respond to ping challenge. The original request will be
                    // retried by retry_timed_out_requests if needed.
                    block_id_socket_batch.push((pong_bytes, addr));
                }
                RepairRequest::Metadata(block_id_repair_type) => {
                    let (bytes, addr) = state
                        .serve_repair
                        .block_id_repair_request(
                            &repair_info.repair_validators,
                            block_id_repair_type,
                            &mut state.peers_cache,
                            &mut state.outstanding_requests,
                            &repair_info.cluster_info.keypair(),
                            &staked_nodes,
                        )
                        .expect("Request serialization cannot fail");

                    block_id_socket_batch.push((bytes, addr));
                    state.sent_requests.insert(request, now);

                    // Update stats
                    state.request_stats.total_requests += 1;
                    match block_id_repair_type {
                        BlockIdRepairType::ParentAndFecSetCount { .. } => {
                            state.request_stats.parent_fec_set_count_requests += 1;
                        }
                        BlockIdRepairType::FecSetRoot { .. } => {
                            state.request_stats.fec_set_root_requests += 1;
                        }
                    }
                }
                RepairRequest::Shred(shred_request) => {
                    let (addr, bytes) = state
                        .serve_repair
                        .repair_request(
                            repair_info,
                            shred_request,
                            &mut state.peers_cache,
                            &mut RepairStats::default(),
                            &mut state.outstanding_shred_requests.write().unwrap(),
                            &repair_info.cluster_info.keypair(),
                            RepairRequestProtocol::UDP,
                        )
                        .expect("Request serialization cannot fail")
                        .expect("UDP requests return the payload");

                    shred_socket_batch.push((bytes, addr));
                    state.sent_requests.insert(request, now);

                    // Update stats
                    state.request_stats.total_requests += 1;
                    state.request_stats.shred_for_block_id_requests += 1;
                }
            }
        }

        if !block_id_socket_batch.is_empty() {
            let total = block_id_socket_batch.len();
            let _ = batch_send(
                block_id_repair_socket,
                block_id_socket_batch
                    .iter()
                    .map(|(bytes, addr)| (bytes, addr)),
            )
            .inspect_err(|SendPktsError::IoError(err, failed)| {
                error!(
                    "{}: failed to send block_id repair packets, packets failed {failed}/{total}: \
                     {err:?}",
                    repair_info.cluster_info.id(),
                )
            });
        }
        if !shred_socket_batch.is_empty() {
            let total = shred_socket_batch.len();
            let _ = batch_send(
                repair_socket,
                shred_socket_batch.iter().map(|(bytes, addr)| (bytes, addr)),
            )
            .inspect_err(|SendPktsError::IoError(err, failed)| {
                error!(
                    "{}: failed to send shred repair requests, packets failed {failed}/{total}: \
                     {err:?}",
                    repair_info.cluster_info.id(),
                )
            });
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bincode::Options,
        solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
        solana_hash::Hash,
        solana_keypair::{Keypair, Signer},
        solana_ledger::{
            blockstore::Blockstore,
            get_tmp_ledger_path_auto_delete,
            shred::merkle_tree::{MerkleTree, SIZE_OF_MERKLE_PROOF_ENTRY},
        },
        solana_perf::packet::Packet,
        solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config},
        solana_sha256_hasher::hashv,
        solana_streamer::socket::SocketAddrSpace,
        std::{io::Cursor, sync::RwLock},
    };

    /// Helper to build a merkle tree from leaf hashes and return the root and proofs
    fn build_merkle_tree(leaves: &[Hash]) -> (Hash, Vec<Vec<u8>>) {
        let tree = MerkleTree::try_new(leaves.iter().cloned().map(Ok)).unwrap();
        let root = *tree.root();
        let num_leaves = leaves.len();

        // Generate proofs for each leaf
        let proofs = (0..num_leaves)
            .map(|leaf_index| {
                tree.make_merkle_proof(leaf_index, num_leaves)
                    .flat_map(|entry| entry.unwrap().iter().copied())
                    .collect()
            })
            .collect();

        (root, proofs)
    }

    /// Serialize a response and nonce into packet format
    fn serialize_response(response: &BlockIdRepairResponse, nonce: u32) -> Vec<u8> {
        bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .serialize(&(response, nonce))
            .unwrap()
    }

    /// Create a packet from serialized data
    fn make_packet(data: &[u8]) -> Packet {
        let mut packet = Packet::default();
        packet.buffer_mut()[..data.len()].copy_from_slice(data);
        packet.meta_mut().size = data.len();
        packet
    }

    fn new_test_cluster_info() -> ClusterInfo {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified)
    }

    /// Create a RepairState for testing, along with bank_forks for tests that need it
    fn create_test_repair_state() -> (RepairState, Arc<RwLock<BankForks>>) {
        let genesis_config = create_genesis_config(100).genesis_config;
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank);

        let cluster_info = Arc::new(new_test_cluster_info());
        let serve_repair = ServeRepair::new_for_test(
            cluster_info,
            bank_forks.clone(),
            Arc::new(RwLock::new(HashSet::default())),
        );

        let state = RepairState {
            serve_repair,
            peers_cache: LruCache::new(REPAIR_PEERS_CACHE_CAPACITY),
            outstanding_requests: OutstandingBlockIdRepairs::default(),
            outstanding_shred_requests: Arc::new(RwLock::new(OutstandingShredRepairs::default())),
            pending_repair_requests: BinaryHeap::default(),
            sent_requests: HashMap::default(),
            requested_blocks: HashSet::default(),
            pending_repair_events: Vec::default(),
            response_stats: BlockIdRepairResponsesStats::default(),
            request_stats: BlockIdRepairRequestsStats::default(),
        };

        (state, bank_forks)
    }

    #[test]
    fn test_deserialize_parent_fec_set_count_response() {
        let fec_set_count = 3usize;
        let parent_slot = 99u64;
        let parent_block_id = Hash::new_unique();
        let parent_proof = vec![1u8; SIZE_OF_MERKLE_PROOF_ENTRY * 2];

        let response = BlockIdRepairResponse::ParentFecSetCount {
            fec_set_count,
            parent_info: (parent_slot, parent_block_id),
            parent_proof: parent_proof.clone(),
        };

        let data = bincode::serialize(&response).unwrap();
        let packet = make_packet(&data);
        let packet_data = packet.data(..).unwrap();

        let deser_response: BlockIdRepairResponse =
            deserialize_from_with_limit(&mut Cursor::new(packet_data)).unwrap();

        match deser_response {
            BlockIdRepairResponse::ParentFecSetCount {
                fec_set_count: fc,
                parent_info: (ps, pb),
                parent_proof: pp,
            } => {
                assert_eq!(fc, fec_set_count);
                assert_eq!(ps, parent_slot);
                assert_eq!(pb, parent_block_id);
                assert_eq!(pp, parent_proof);
            }
            _ => panic!("Expected ParentFecSetCount response"),
        }
    }

    #[test]
    fn test_deserialize_fec_set_root_response() {
        let fec_set_root = Hash::new_unique();
        let fec_set_proof = vec![2u8; SIZE_OF_MERKLE_PROOF_ENTRY * 3];

        let response = BlockIdRepairResponse::FecSetRoot {
            fec_set_root,
            fec_set_proof: fec_set_proof.clone(),
        };

        let data = bincode::serialize(&response).unwrap();
        let packet = make_packet(&data);
        let packet_data = packet.data(..).unwrap();

        let deser_response: BlockIdRepairResponse =
            deserialize_from_with_limit(&mut Cursor::new(packet_data)).unwrap();

        match deser_response {
            BlockIdRepairResponse::FecSetRoot {
                fec_set_root: fr,
                fec_set_proof: fp,
            } => {
                assert_eq!(fr, fec_set_root);
                assert_eq!(fp, fec_set_proof);
            }
            _ => panic!("Expected FecSetRoot response"),
        }
    }

    #[test]
    fn test_deserialize_invalid_response() {
        // Empty packet
        let packet = make_packet(&[]);
        let packet_data = packet.data(..).unwrap();
        assert!(
            deserialize_from_with_limit::<_, BlockIdRepairResponse>(&mut Cursor::new(packet_data))
                .is_err()
        );

        // Garbage data
        let packet = make_packet(&[0xff, 0xff, 0xff, 0xff]);
        let packet_data = packet.data(..).unwrap();
        assert!(
            deserialize_from_with_limit::<_, BlockIdRepairResponse>(&mut Cursor::new(packet_data))
                .is_err()
        );
    }

    #[test]
    fn test_retry_timed_out_requests() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (mut state, _bank_forks) = create_test_repair_state();

        let now = timestamp();
        let expired_time = now - REPAIR_REQUEST_TIMEOUT_MS - 100;

        // 1. Expired metadata request (ParentAndFecSetCount) - should retry
        let expired_metadata = RepairRequest::Metadata(BlockIdRepairType::ParentAndFecSetCount {
            slot: 100,
            block_id: Hash::new_unique(),
        });
        state
            .sent_requests
            .insert(expired_metadata.clone(), expired_time);

        // 2. Recent metadata request - should stay in sent_requests
        let recent_metadata = RepairRequest::Metadata(BlockIdRepairType::ParentAndFecSetCount {
            slot: 101,
            block_id: Hash::new_unique(),
        });
        state.sent_requests.insert(recent_metadata.clone(), now);

        // 3. Expired metadata request (FecSetRoot) - should retry
        let expired_fec_set_root = RepairRequest::Metadata(BlockIdRepairType::FecSetRoot {
            slot: 102,
            block_id: Hash::new_unique(),
            fec_set_index: 0,
        });
        state
            .sent_requests
            .insert(expired_fec_set_root.clone(), expired_time);

        // 4. Expired shred request, shred NOT in blockstore - should retry
        let expired_shred_not_received = RepairRequest::Shred(ShredRepairType::ShredForBlockId {
            slot: 103,
            index: 5,
            fec_set_merkle_root: Hash::new_unique(),
            block_id: Hash::new_unique(),
        });
        state
            .sent_requests
            .insert(expired_shred_not_received.clone(), expired_time);

        // 5. Expired shred request, shred IS in blockstore - should NOT retry
        let received_block_id = Hash::new_unique();
        let received_slot = 104u64;
        let received_shred_index = 10u32;
        blockstore
            .insert_shred_index_for_alternate_block(
                received_slot,
                received_block_id,
                received_shred_index,
            )
            .unwrap();
        let expired_shred_already_received =
            RepairRequest::Shred(ShredRepairType::ShredForBlockId {
                slot: received_slot,
                index: received_shred_index,
                fec_set_merkle_root: Hash::new_unique(),
                block_id: received_block_id,
            });
        state
            .sent_requests
            .insert(expired_shred_already_received.clone(), expired_time);

        // 6. Recent shred request - should stay in sent_requests
        let recent_shred = RepairRequest::Shred(ShredRepairType::ShredForBlockId {
            slot: 105,
            index: 15,
            fec_set_merkle_root: Hash::new_unique(),
            block_id: Hash::new_unique(),
        });
        state.sent_requests.insert(recent_shred.clone(), now);

        // Run the retry logic
        BlockIdRepairService::retry_timed_out_requests(&blockstore, &mut state, now);

        // Verify: only non-expired requests remain in sent_requests
        assert_eq!(state.sent_requests.len(), 2);
        assert!(state.sent_requests.contains_key(&recent_metadata));
        assert!(state.sent_requests.contains_key(&recent_shred));

        // Verify: 3 requests moved to pending (2 expired metadata + 1 expired shred not received)
        // The expired shred that was already received should NOT be in pending
        assert_eq!(state.pending_repair_requests.len(), 3);
        let pending: Vec<_> = std::iter::from_fn(|| state.pending_repair_requests.pop()).collect();
        assert!(pending.contains(&expired_metadata));
        assert!(pending.contains(&expired_fec_set_root));
        assert!(pending.contains(&expired_shred_not_received));
        assert!(!pending.contains(&expired_shred_already_received));
    }

    #[test]
    fn test_process_block_id_repair_response_parent_fec_set_count() {
        let (mut state, _bank_forks) = create_test_repair_state();
        let keypair = Keypair::new();

        let slot = 100u64;
        let parent_slot = 99u64;
        let parent_block_id = Hash::new_unique();
        let fec_set_count = 2usize;

        // Create valid merkle tree for the response
        let fec_set_roots: Vec<Hash> = (0..fec_set_count).map(|_| Hash::new_unique()).collect();
        let parent_info_leaf = hashv(&[&parent_slot.to_le_bytes(), parent_block_id.as_ref()]);
        let mut leaves = fec_set_roots.clone();
        leaves.push(parent_info_leaf);
        let (block_id, proofs) = build_merkle_tree(&leaves);
        let parent_proof = proofs[fec_set_count].clone();

        // Create the request that would have been sent
        let request = BlockIdRepairType::ParentAndFecSetCount { slot, block_id };

        // Register the request in outstanding_requests and get the nonce
        let nonce = state.outstanding_requests.add_request(request, timestamp());

        // Also track in sent_requests
        state
            .sent_requests
            .insert(RepairRequest::Metadata(request), timestamp());

        // Build the response
        let response = BlockIdRepairResponse::ParentFecSetCount {
            fec_set_count,
            parent_info: (parent_slot, parent_block_id),
            parent_proof,
        };

        // Serialize and create packet
        let data = serialize_response(&response, nonce);
        let packet = make_packet(&data);

        BlockIdRepairService::process_block_id_repair_response(
            &Pubkey::new_unique(),
            (&packet).into(),
            &keypair,
            &mut state,
        );

        // Verify: FetchBlock event for parent was added to pending_repair_events
        assert_eq!(state.pending_repair_events.len(), 1);
        let RepairEvent::FetchBlock {
            slot: s,
            block_id: b,
        } = &state.pending_repair_events[0];
        assert_eq!(*s, parent_slot);
        assert_eq!(*b, parent_block_id);

        // Verify: FecSetRoot requests were added to pending
        assert_eq!(state.pending_repair_requests.len(), fec_set_count);

        // Verify: request was removed from sent_requests
        assert!(!state
            .sent_requests
            .contains_key(&RepairRequest::Metadata(request)));

        // Verify: stats were updated
        assert_eq!(state.response_stats.parent_fec_set_count_responses, 1);
    }

    #[test]
    fn test_process_block_id_repair_response_fec_set_root() {
        let (mut state, _bank_forks) = create_test_repair_state();
        let keypair = Keypair::new();

        let slot = 100u64;
        let fec_set_index = 32u32; // Second FEC set
        let fec_set_count = 3usize;

        // Create valid merkle tree - FEC set roots form the leaves, parent info is last leaf
        let fec_set_roots: Vec<Hash> = (0..fec_set_count).map(|_| Hash::new_unique()).collect();
        let parent_info_leaf = Hash::new_unique(); // Placeholder for parent info
        let mut leaves = fec_set_roots.clone();
        leaves.push(parent_info_leaf);
        let (block_id, proofs) = build_merkle_tree(&leaves);

        // The FEC set root for fec_set_index=32 corresponds to leaf index 1 (32/32=1)
        let fec_set_leaf_index = fec_set_index as usize / DATA_SHREDS_PER_FEC_BLOCK;
        let fec_set_root = fec_set_roots[fec_set_leaf_index];
        let fec_set_proof = proofs[fec_set_leaf_index].clone();

        // Create the request that would have been sent
        let request = BlockIdRepairType::FecSetRoot {
            slot,
            block_id,
            fec_set_index,
        };

        // Register the request in outstanding_requests and get the nonce
        let nonce = state.outstanding_requests.add_request(request, timestamp());

        // Also track in sent_requests
        state
            .sent_requests
            .insert(RepairRequest::Metadata(request), timestamp());

        // Build the response
        let response = BlockIdRepairResponse::FecSetRoot {
            fec_set_root,
            fec_set_proof,
        };

        // Serialize and create packet
        let data = serialize_response(&response, nonce);
        let packet = make_packet(&data);

        BlockIdRepairService::process_block_id_repair_response(
            &Pubkey::new_unique(),
            (&packet).into(),
            &keypair,
            &mut state,
        );

        // Verify: No FetchBlock events (FecSetRoot doesn't generate those)
        assert!(state.pending_repair_events.is_empty());

        // Verify: ShredForBlockId requests were added to pending (one for each shred in FEC set)
        assert_eq!(
            state.pending_repair_requests.len(),
            DATA_SHREDS_PER_FEC_BLOCK
        );

        // Verify the shred requests have correct parameters
        while let Some(req) = state.pending_repair_requests.pop() {
            match req {
                RepairRequest::Shred(ShredRepairType::ShredForBlockId {
                    slot: s,
                    index,
                    fec_set_merkle_root,
                    block_id: b,
                }) => {
                    assert_eq!(s, slot);
                    assert!(
                        index >= fec_set_index
                            && index < fec_set_index + DATA_SHREDS_PER_FEC_BLOCK as u32
                    );
                    assert_eq!(fec_set_merkle_root, fec_set_root);
                    assert_eq!(b, block_id);
                }
                _ => panic!("Expected ShredForBlockId request"),
            }
        }

        // Verify: request was removed from sent_requests
        assert!(!state
            .sent_requests
            .contains_key(&RepairRequest::Metadata(request)));

        // Verify: stats were updated
        assert_eq!(state.response_stats.fec_set_root_responses, 1);
    }

    #[test]
    fn test_process_block_id_repair_response_invalid_nonce() {
        let (mut state, _bank_forks) = create_test_repair_state();
        let keypair = Keypair::new();

        // Create a response with a nonce that wasn't registered
        let response = BlockIdRepairResponse::ParentFecSetCount {
            fec_set_count: 2,
            parent_info: (99, Hash::new_unique()),
            parent_proof: vec![0u8; SIZE_OF_MERKLE_PROOF_ENTRY * 2],
        };

        let invalid_nonce = 99999u32;
        let data = serialize_response(&response, invalid_nonce);
        let packet = make_packet(&data);

        BlockIdRepairService::process_block_id_repair_response(
            &Pubkey::new_unique(),
            (&packet).into(),
            &keypair,
            &mut state,
        );

        // Verify: No events or requests generated
        assert!(state.pending_repair_events.is_empty());
        assert!(state.pending_repair_requests.is_empty());

        // Verify: invalid packet stat was incremented
        assert_eq!(state.response_stats.invalid_packets, 1);
    }

    #[test]
    fn test_process_repair_event_dead_slot_triggers_repair() {
        // When Turbine has failed (slot is dead), repair should kick off immediately
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (mut state, bank_forks) = create_test_repair_state();
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();

        let slot = 100u64;
        let block_id = Hash::new_unique();

        // Mark the slot as dead (Turbine failed)
        blockstore.set_dead_slot(slot).unwrap();

        let event = RepairEvent::FetchBlock { slot, block_id };

        BlockIdRepairService::process_repair_event(
            Pubkey::new_unique(),
            event,
            &sharable_banks,
            &blockstore,
            &mut state,
        );

        // Verify: ParentAndFecSetCount request was added
        assert_eq!(state.pending_repair_requests.len(), 1);
        match state.pending_repair_requests.pop().unwrap() {
            RepairRequest::Metadata(BlockIdRepairType::ParentAndFecSetCount {
                slot: s,
                block_id: b,
            }) => {
                assert_eq!(s, slot);
                assert_eq!(b, block_id);
            }
            _ => panic!("Expected ParentAndFecSetCount request"),
        }

        // Verify: block was added to requested_blocks
        assert!(state.requested_blocks.contains(&(slot, block_id)));

        // Verify: no deferred events
        assert!(state.pending_repair_events.is_empty());
    }

    #[test]
    fn test_process_repair_event_deferred_when_turbine_not_complete() {
        // When Turbine hasn't completed (slot not dead, no DMR), event should be deferred
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (mut state, bank_forks) = create_test_repair_state();
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();

        let slot = 100u64;
        let block_id = Hash::new_unique();
        let event = RepairEvent::FetchBlock { slot, block_id };

        BlockIdRepairService::process_repair_event(
            Pubkey::new_unique(),
            event,
            &sharable_banks,
            &blockstore,
            &mut state,
        );

        // Verify: No repair request was added (event was deferred)
        assert!(state.pending_repair_requests.is_empty());

        // Verify: Event was deferred
        assert_eq!(state.pending_repair_events.len(), 1);
        let RepairEvent::FetchBlock {
            slot: s,
            block_id: b,
        } = &state.pending_repair_events[0];
        assert_eq!(*s, slot);
        assert_eq!(*b, block_id);

        // Verify: block was NOT added to requested_blocks (so it can be re-added when reprocessed)
        assert!(!state.requested_blocks.contains(&(slot, block_id)));
    }

    #[test]
    fn test_process_repair_event_turbine_got_different_block() {
        // When Turbine completed with a different block_id, repair should kick off
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (mut state, bank_forks) = create_test_repair_state();
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();

        let slot = 100u64;
        let requested_block_id = Hash::new_unique();
        let turbine_block_id = Hash::new_unique(); // Different block_id from Turbine

        // Set up blockstore to have a different block_id at Original location
        blockstore
            .set_double_merkle_root(slot, BlockLocation::Original, turbine_block_id)
            .unwrap();

        let event = RepairEvent::FetchBlock {
            slot,
            block_id: requested_block_id,
        };

        BlockIdRepairService::process_repair_event(
            Pubkey::new_unique(),
            event,
            &sharable_banks,
            &blockstore,
            &mut state,
        );

        // Verify: ParentAndFecSetCount request was added for the requested block
        assert_eq!(state.pending_repair_requests.len(), 1);
        match state.pending_repair_requests.pop().unwrap() {
            RepairRequest::Metadata(BlockIdRepairType::ParentAndFecSetCount {
                slot: s,
                block_id: b,
            }) => {
                assert_eq!(s, slot);
                assert_eq!(b, requested_block_id);
            }
            _ => panic!("Expected ParentAndFecSetCount request"),
        }

        // Verify: block was added to requested_blocks
        assert!(state.requested_blocks.contains(&(slot, requested_block_id)));

        // Verify: no deferred events
        assert!(state.pending_repair_events.is_empty());
    }

    #[test]
    fn test_process_repair_event_already_requested() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (mut state, bank_forks) = create_test_repair_state();
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();

        let slot = 100u64;
        let block_id = Hash::new_unique();

        // Pre-add block to requested_blocks
        state.requested_blocks.insert((slot, block_id));

        let event = RepairEvent::FetchBlock { slot, block_id };

        BlockIdRepairService::process_repair_event(
            Pubkey::new_unique(),
            event,
            &sharable_banks,
            &blockstore,
            &mut state,
        );

        // Verify: No new request was added (block already requested)
        assert!(state.pending_repair_requests.is_empty());
    }

    #[test]
    fn test_process_repair_event_at_root_ignored() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (mut state, bank_forks) = create_test_repair_state();
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();

        // Use slot 0 which is at root
        let slot = 0u64;
        let block_id = Hash::new_unique();
        let event = RepairEvent::FetchBlock { slot, block_id };

        BlockIdRepairService::process_repair_event(
            Pubkey::new_unique(),
            event,
            &sharable_banks,
            &blockstore,
            &mut state,
        );

        // Verify: No request was added (slot at root is ignored)
        assert!(state.pending_repair_requests.is_empty());
        assert!(!state.requested_blocks.contains(&(slot, block_id)));
    }
}
