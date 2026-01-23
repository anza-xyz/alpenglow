//! Service responsible for fetching alternate versions of blocks through informed repair.
//! Receives [`RepairEvent`]s from votor / replay and engages in a repair process to fetch
//! the block to the alternate blockstore column.
//!
//! Finally if the block is canonical or otherwise necessary to make progress, it will move
//! the shreds over to the main blockstore column.
//!
//! Sends and processes [`BlockIdRepairType`] requests through a separate socket for block and
//! fec set metadata. Additionally sends [`ShredRepairType`] requests through the main socket
//! to fetch shreds.
use {
    super::{
        repair_service::{OutstandingShredRepairs, REPAIR_MS, REPAIR_REQUEST_TIMEOUT_MS},
        serve_repair::{RepairPeers, ServeRepair, ShredRepairType, REPAIR_PEERS_CACHE_CAPACITY},
        standard_repair_handler::StandardRepairHandler,
    },
    crate::repair::{
        outstanding_requests::OutstandingRequests,
        packet_threshold::DynamicPacketToProcessThreshold,
        repair_service::{RepairInfo, RepairStats},
        serve_repair::{
            BlockIdRepairResponse, BlockIdRepairType, RepairProtocol, RepairRequestProtocol,
            RepairResponse, REPAIR_RESPONSE_SERIALIZED_PING_BYTES,
        },
    },
    crossbeam_channel::unbounded,
    log::{debug, info, trace},
    lru::LruCache,
    solana_clock::Slot,
    solana_gossip::ping_pong::Pong,
    solana_keypair::signable::Signable,
    solana_ledger::{
        blockstore::Blockstore, blockstore_meta::BlockLocation, shred::DATA_SHREDS_PER_FEC_BLOCK,
    },
    solana_perf::{
        packet::{deserialize_from_with_limit, PacketRef},
        recycler::Recycler,
    },
    solana_runtime::bank_forks::SharableBanks,
    solana_streamer::{
        sendmmsg::{batch_send, SendPktsError},
        streamer::{self, PacketBatchReceiver, StreamerReceiveStats},
    },
    solana_time_utils::timestamp,
    solana_votor::event::{RepairEvent, RepairEventReceiver},
    solana_votor_messages::{consensus_message::Block, migration::MigrationStatus},
    std::{
        collections::{BinaryHeap, HashMap, HashSet},
        io::{Cursor, Read},
        net::UdpSocket,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RepairRequest {
    /// Metadata requests
    Metadata(BlockIdRepairType),

    /// Shred request
    Shred(ShredRepairType),
}

impl RepairRequest {
    fn slot(&self) -> Slot {
        match self {
            RepairRequest::Metadata(block_id_repair_type) => block_id_repair_type.block().0,
            RepairRequest::Shred(shred_repair_type) => shred_repair_type.slot(),
        }
    }
}

/// We prioritize requests with lower slot #s and then prefer metadata requests
/// before shred requests.
impl Ord for RepairRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use {
            std::cmp::Ordering, BlockIdRepairType::*, RepairRequest::*,
            ShredRepairType::ShredForBlockId,
        };
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

#[derive(Default)]
struct BlockIdRepairResponsesStats {
    total_packets: usize,
    processed: usize,
    dropped_packets: usize,
    invalid_packets: usize,
    parent_fec_set_count_responses: usize,
    fec_set_root_responses: usize,
}

impl BlockIdRepairResponsesStats {
    fn report(&mut self) {
        datapoint_info!(
            "block_id_repair_responses",
            ("total_packets", self.total_packets, i64),
            ("processed", self.processed, i64),
            ("dropped_packets", self.dropped_packets, i64),
            ("invalid_packets", self.invalid_packets, i64),
            (
                "parent_fec_set_count_responses",
                self.parent_fec_set_count_responses,
                i64
            ),
            ("fec_set_root_responses", self.fec_set_root_responses, i64),
        );
        *self = Self::default();
    }
}

#[derive(Default)]
struct BlockIdRepairRequestsStats {
    total_requests: usize,
    parent_fec_set_count_requests: usize,
    fec_set_root_requests: usize,
    shred_for_block_id_requests: usize,
}

impl BlockIdRepairRequestsStats {
    fn report(&mut self) {
        datapoint_info!(
            "block_id_repair_requests",
            ("total_requests", self.total_requests, i64),
            (
                "parent_fec_set_count_requests",
                self.parent_fec_set_count_requests,
                i64
            ),
            ("fec_set_root_requests", self.fec_set_root_requests, i64),
            (
                "shred_for_block_id_requests",
                self.shred_for_block_id_requests,
                i64
            ),
        );
        *self = Self::default();
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
            Some(Duration::from_millis(1)), // coalesce
            false,                          // use_pinned_memory
            None,                           // in_vote_only_mode
            false,                          // is_staked_service
        );

        let t_block_id_repair = Self::run(
            exit,
            response_receiver,
            repair_event_receiver,
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
    /// - Manages the blockstore columns and any potential copying that needs to occur
    #[allow(clippy::too_many_arguments)]
    fn run(
        exit: Arc<AtomicBool>,
        response_receiver: PacketBatchReceiver,
        repair_event_receiver: RepairEventReceiver,
        blockstore: Arc<Blockstore>,
        block_id_repair_socket: Arc<UdpSocket>,
        repair_socket: Arc<UdpSocket>,
        repair_info: RepairInfo,
        outstanding_shred_requests: Arc<RwLock<OutstandingShredRepairs>>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("solBlockIdRep".to_string())
            .spawn(move || {
                info!("Starting block ID repair service thread");
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

                    let root = sharable_banks.root().slot();

                    // Clean up old request tracking
                    state.requested_blocks.retain(|(slot, _)| *slot > root);

                    // Process responses (including pings), generate new requests / repair events
                    Self::process_responses(
                        &response_receiver,
                        &mut state,
                        &mut throttle,
                        &block_id_repair_socket,
                        &repair_info.cluster_info.keypair(),
                    );

                    // Receive new repair events from votor / replay
                    let new_events: Vec<_> = repair_event_receiver.try_iter().collect();
                    if !new_events.is_empty() {
                        info!(
                            "Received {} new repair events: {:?}",
                            new_events.len(),
                            new_events
                        );
                    }
                    state.pending_repair_events.extend(new_events);

                    // Generate repair requests for repair events
                    let events_to_process = std::mem::take(&mut state.pending_repair_events);
                    if !events_to_process.is_empty() {
                        trace!(
                            "Processing {} pending repair events",
                            events_to_process.len()
                        );
                    }
                    for event in events_to_process {
                        Self::process_repair_event(event, &sharable_banks, &blockstore, &mut state);
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

                    std::thread::sleep(Duration::from_millis(REPAIR_MS));
                }
            })
            .unwrap()
    }

    /// Process any pending responses from the response receiver and generate any new requests
    fn process_responses(
        response_receiver: &PacketBatchReceiver,
        state: &mut RepairState,
        throttle: &mut DynamicPacketToProcessThreshold,
        block_id_repair_socket: &UdpSocket,
        keypair: &solana_keypair::Keypair,
    ) {
        let Ok(packet_batch) = response_receiver.try_recv() else {
            return;
        };
        info!(
            "Received {} packets on response channel",
            packet_batch.len()
        );
        let mut packet_batches = vec![packet_batch];

        // throttle processing TODO(ashwin): this is copied from ancestor_hashes, see if we can do better
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

        // First handle any pings by sending pongs back
        let mut pending_pongs = Vec::new();
        for packet_batch in &packet_batches {
            for packet in packet_batch.iter() {
                let packet_size = packet.meta().size;
                debug!(
                    "Processing packet: size={}, ping_size={}, from={}",
                    packet_size,
                    REPAIR_RESPONSE_SERIALIZED_PING_BYTES,
                    packet.meta().socket_addr()
                );
                if packet_size == REPAIR_RESPONSE_SERIALIZED_PING_BYTES {
                    if let Ok(RepairResponse::Ping(ping)) = packet.deserialize_slice(..) {
                        if ping.verify() {
                            let pong = RepairProtocol::Pong(Pong::new(&ping, keypair));
                            if let Ok(pong) = bincode::serialize(&pong) {
                                let from_addr = packet.meta().socket_addr();
                                info!("Received ping from {}, sending pong", from_addr);
                                pending_pongs.push((pong, from_addr));
                            }
                        }
                    }
                }
            }
        }

        // Send pongs back
        if !pending_pongs.is_empty() {
            info!("Sending {} pongs", pending_pongs.len());
            let pending_pongs_refs = pending_pongs.iter().map(|(bytes, addr)| (bytes, addr));
            let _ = batch_send(block_id_repair_socket, pending_pongs_refs);
        }

        // process the regular responses
        let compute_timer = Instant::now();
        let mut response_count = 0;
        packet_batches
            .iter()
            .flat_map(|packet_batch| packet_batch.iter())
            .filter(|packet| {
                let is_ping = packet.meta().size == REPAIR_RESPONSE_SERIALIZED_PING_BYTES;
                if is_ping {
                    debug!("Filtering out ping packet (size={})", packet.meta().size);
                }
                !is_ping
            })
            .for_each(|packet| {
                response_count += 1;
                info!(
                    "Processing response packet {}, size={}",
                    response_count,
                    packet.meta().size
                );
                Self::process_block_id_repair_response(packet, state);
            });
        if response_count > 0 {
            info!("Processed {} response packets", response_count);
        }

        // adjust throttle based on actual compute time
        throttle.update(total_packets, compute_timer.elapsed());
    }

    /// Process a response:
    /// - Sanity checks on deserialization
    /// - Verify repair nonce
    /// - Queue more repair requests or events
    fn process_block_id_repair_response(packet: PacketRef<'_>, state: &mut RepairState) {
        let Some((response, nonce)) = Self::deserialize_response_and_nonce(packet) else {
            trace!("Failed to deserialize response packet");
            state.response_stats.invalid_packets += 1;
            return;
        };

        debug!("Received response: {:?}, nonce={}", response, nonce);

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
            info!(
                "Response with invalid nonce {} or failed verification for {:?}",
                nonce, response
            );
            state.response_stats.invalid_packets += 1;
            return;
        };

        info!("Valid response for request {:?}", request);

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
                        "Programmer error, *verified* response was FecSetRoot but request was not"
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
        }

        state.response_stats.processed += 1;
    }

    /// Deserialize a packet into a [`BlockIdRepairResponse`] along with the nonce
    /// Returns `None` deserialization failed or there are trailing bits
    fn deserialize_response_and_nonce(packet: PacketRef) -> Option<(BlockIdRepairResponse, u32)> {
        let packet_data = packet.data(..)?;

        let mut cursor = Cursor::new(packet_data);
        let response: BlockIdRepairResponse = match deserialize_from_with_limit(&mut cursor) {
            Ok(r) => r,
            Err(e) => {
                debug!(
                    "Failed to deserialize response: {:?}, packet_size={}",
                    e,
                    packet_data.len()
                );
                return None;
            }
        };
        let nonce: u32 = match deserialize_from_with_limit(&mut cursor) {
            Ok(n) => n,
            Err(e) => {
                debug!("Failed to deserialize nonce: {:?}", e);
                return None;
            }
        };

        if cursor.bytes().next().is_some() {
            debug!("Response has trailing bytes, discarding");
            return None;
        }
        Some((response, nonce))
    }

    /// Process a repair event and generate any requests or do any blockstore column management
    fn process_repair_event(
        event: RepairEvent,
        sharable_banks: &SharableBanks,
        blockstore: &Blockstore,
        state: &mut RepairState,
    ) {
        let root = sharable_banks.root().slot();
        debug!("process_repair_event: {:?}, root={}", event, root);

        if event.slot() <= root {
            debug!("Ignoring event for slot {} <= root {}", event.slot(), root);
            return;
        }

        match event {
            RepairEvent::FetchBlock { slot, block_id } => {
                debug!("FetchBlock event: slot={}, block_id={:?}", slot, block_id);
                if state.requested_blocks.contains(&(slot, block_id)) {
                    debug!(
                        "FetchBlock: already requested slot={}, block_id={:?}",
                        slot, block_id
                    );
                    return;
                }

                // Check if we already have the block
                if let Some(location) = blockstore.get_block_location(slot, block_id) {
                    debug!(
                        "FetchBlock: already have block at {:?}, fetching parent",
                        location
                    );
                    Self::queue_fetch_parent_block(blockstore, slot, location, state);
                    return;
                }

                // We don't have the block. Check Turbine status.
                if blockstore.is_dead(slot) {
                    info!(
                        "FetchBlock: slot {} is dead, starting repair for block_id={:?}",
                        slot, block_id
                    );
                    state.pending_repair_requests.push(RepairRequest::Metadata(
                        BlockIdRepairType::ParentAndFecSetCount { slot, block_id },
                    ));
                    state.requested_blocks.insert((slot, block_id));
                    return;
                }

                // Check if Turbine has completed for this slot
                let double_merkle_root =
                    blockstore.get_double_merkle_root(slot, BlockLocation::Original);
                let slot_meta = blockstore.meta(slot).unwrap();
                debug!(
                    "FetchBlock: slot {} double_merkle_root={:?}, has_meta={:?}",
                    slot,
                    double_merkle_root,
                    slot_meta.is_some()
                );
                match double_merkle_root {
                    None => {
                        // Turbine hasn't completed. Check if there's any turbine activity for this slot.
                        if slot_meta.is_none() {
                            // No turbine data at all - turbine is likely disabled or not sending us shreds.
                            // Start repair immediately.
                            info!(
                                "FetchBlock: No turbine data for slot {}, starting repair for \
                                 block_id={:?}",
                                slot, block_id
                            );
                            state.pending_repair_requests.push(RepairRequest::Metadata(
                                BlockIdRepairType::ParentAndFecSetCount { slot, block_id },
                            ));
                            state.requested_blocks.insert((slot, block_id));
                        } else {
                            // Turbine has started but not completed, defer and check again later
                            debug!(
                                "FetchBlock: Turbine not complete for slot {}, deferring",
                                slot
                            );
                            debug_assert!(slot_meta.is_none_or(|s| !s.is_full()));
                            state
                                .pending_repair_events
                                .push(RepairEvent::FetchBlock { slot, block_id });
                        }
                    }
                    Some(turbine_block_id) if turbine_block_id != block_id => {
                        info!(
                            "FetchBlock: Turbine has different block {:?} vs requested {:?} for \
                             slot {}, starting repair",
                            turbine_block_id, block_id, slot
                        );
                        state.pending_repair_requests.push(RepairRequest::Metadata(
                            BlockIdRepairType::ParentAndFecSetCount { slot, block_id },
                        ));
                        state.requested_blocks.insert((slot, block_id));
                    }
                    Some(_) => {
                        debug!(
                            "FetchBlock: Turbine has correct block for slot {}, fetching parent",
                            slot
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

    /// Check for requests that have timed out and move them back to pending_repair_requests.
    /// For shred requests, we check if the shred has been received before retrying.
    fn retry_timed_out_requests(blockstore: &Blockstore, state: &mut RepairState, now: u64) {
        // TODO(ashwin): use extract_if when we upstream (rust 1.88+)
        let mut timed_out = Vec::new();
        state.sent_requests.retain(|request, sent_time| {
            if now.saturating_sub(*sent_time) >= REPAIR_REQUEST_TIMEOUT_MS {
                match request {
                    RepairRequest::Metadata(_) => {
                        // Metadata requests: always retry on timeout
                        timed_out.push(*request);
                    }
                    // Since shred responses are sent to a different socket, we need to check
                    // blockstore to see if this expired request is actually expired, or if the
                    // shred has already been ingested
                    RepairRequest::Shred(shred_request) => {
                        if !Self::has_received_shred(blockstore, shred_request) {
                            timed_out.push(*request);
                        }
                    }
                }
                false
            } else {
                true
            }
        });
        state.pending_repair_requests.extend(timed_out);
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
        if pending_count > 0 {
            debug!(
                "send_requests: {} pending requests, root={}",
                pending_count, root
            );
        }

        let max_batch_len = pending_count.min(MAX_REPAIR_REQUESTS_PER_ITERATION);
        let mut metadata_batch = Vec::with_capacity(max_batch_len);
        let mut shred_batch = Vec::with_capacity(max_batch_len);

        let root_bank = repair_info.bank_forks.read().unwrap().root_bank();
        let staked_nodes = root_bank.current_epoch_staked_nodes();
        let now = timestamp();

        while metadata_batch.len().saturating_add(shred_batch.len())
            < MAX_REPAIR_REQUESTS_PER_ITERATION
        {
            let Some(request) = state.pending_repair_requests.pop() else {
                break;
            };

            if request.slot() <= root {
                continue;
            }

            match request {
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

                    metadata_batch.push((bytes, addr));
                    state.sent_requests.insert(request, now);
                }
                RepairRequest::Shred(shred_request) => {
                    let (addr, bytes) = state
                        .serve_repair
                        .repair_request(
                            repair_info,
                            shred_request,
                            &mut state.peers_cache,
                            &mut RepairStats::default(), // TODO(ashwin): split out stats
                            &mut state.outstanding_shred_requests.write().unwrap(),
                            &repair_info.cluster_info.keypair(),
                            RepairRequestProtocol::UDP,
                        )
                        .expect("Request serialization cannot fail")
                        .expect("UDP requests return the payload");

                    shred_batch.push((bytes, addr));
                    state.sent_requests.insert(request, now);
                }
            }
        }

        if !metadata_batch.is_empty() {
            let total = metadata_batch.len();
            let addrs: Vec<_> = metadata_batch.iter().map(|(_, addr)| addr).collect();
            info!("Sending {} metadata repair requests to {:?}", total, addrs);
            let _ = batch_send(
                block_id_repair_socket,
                metadata_batch.iter().map(|(bytes, addr)| (bytes, addr)),
            )
            .inspect_err(|SendPktsError::IoError(err, failed)| {
                error!(
                    "{}: failed to send metadata repair requests, packets failed \
                     {failed}/{total}: {err:?}",
                    repair_info.cluster_info.id(),
                )
            });
        }
        if !shred_batch.is_empty() {
            let total = shred_batch.len();
            let addrs: Vec<_> = shred_batch.iter().map(|(_, addr)| addr).collect();
            info!("Sending {} shred repair requests to {:?}", total, addrs);
            let _ = batch_send(
                repair_socket,
                shred_batch.iter().map(|(bytes, addr)| (bytes, addr)),
            )
            .inspect_err(|SendPktsError::IoError(err, failed)| {
                error!(
                    "{}: failed to send shared repair requests, packets failed {failed}/{total}: \
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
            blockstore_meta::ParentMeta,
            get_tmp_ledger_path_auto_delete,
            shred::merkle_tree::{MerkleTree, SIZE_OF_MERKLE_PROOF_ENTRY},
        },
        solana_perf::packet::Packet,
        solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config},
        solana_sha256_hasher::hashv,
        solana_streamer::socket::SocketAddrSpace,
        std::sync::RwLock,
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
        let nonce = 12345u32;

        let response = BlockIdRepairResponse::ParentFecSetCount {
            fec_set_count,
            parent_info: (parent_slot, parent_block_id),
            parent_proof: parent_proof.clone(),
        };

        let data = serialize_response(&response, nonce);
        let packet = make_packet(&data);

        let result = BlockIdRepairService::deserialize_response_and_nonce((&packet).into());
        assert!(result.is_some());

        let (deser_response, deser_nonce) = result.unwrap();
        assert_eq!(deser_nonce, nonce);

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
        let nonce = 67890u32;

        let response = BlockIdRepairResponse::FecSetRoot {
            fec_set_root,
            fec_set_proof: fec_set_proof.clone(),
        };

        let data = serialize_response(&response, nonce);
        let packet = make_packet(&data);

        let result = BlockIdRepairService::deserialize_response_and_nonce((&packet).into());
        assert!(result.is_some());

        let (deser_response, deser_nonce) = result.unwrap();
        assert_eq!(deser_nonce, nonce);

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
    fn test_deserialize_invalid_packet() {
        // Empty packet
        let packet = make_packet(&[]);
        assert!(BlockIdRepairService::deserialize_response_and_nonce((&packet).into()).is_none());

        // Garbage data
        let packet = make_packet(&[0xff, 0xff, 0xff, 0xff]);
        assert!(BlockIdRepairService::deserialize_response_and_nonce((&packet).into()).is_none());

        // Truncated response (missing nonce)
        let response = BlockIdRepairResponse::FecSetRoot {
            fec_set_root: Hash::new_unique(),
            fec_set_proof: vec![],
        };
        let mut data = bincode::options()
            .with_fixint_encoding()
            .serialize(&response)
            .unwrap();
        // Don't add nonce
        let packet = make_packet(&data);
        assert!(BlockIdRepairService::deserialize_response_and_nonce((&packet).into()).is_none());

        // Extra trailing bytes should cause failure
        data = serialize_response(&response, 123);
        data.extend_from_slice(&[0xff, 0xff]); // Add trailing garbage
        let packet = make_packet(&data);
        assert!(BlockIdRepairService::deserialize_response_and_nonce((&packet).into()).is_none());
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
        state.sent_requests.insert(expired_metadata, expired_time);

        // 2. Recent metadata request - should stay in sent_requests
        let recent_metadata = RepairRequest::Metadata(BlockIdRepairType::ParentAndFecSetCount {
            slot: 101,
            block_id: Hash::new_unique(),
        });
        state.sent_requests.insert(recent_metadata, now);

        // 3. Expired metadata request (FecSetRoot) - should retry
        let expired_fec_set_root = RepairRequest::Metadata(BlockIdRepairType::FecSetRoot {
            slot: 102,
            block_id: Hash::new_unique(),
            fec_set_index: 0,
        });
        state
            .sent_requests
            .insert(expired_fec_set_root, expired_time);

        // 4. Expired shred request, shred NOT in blockstore - should retry
        let expired_shred_not_received = RepairRequest::Shred(ShredRepairType::ShredForBlockId {
            slot: 103,
            index: 5,
            fec_set_merkle_root: Hash::new_unique(),
            block_id: Hash::new_unique(),
        });
        state
            .sent_requests
            .insert(expired_shred_not_received, expired_time);

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
            .insert(expired_shred_already_received, expired_time);

        // 6. Recent shred request - should stay in sent_requests
        let recent_shred = RepairRequest::Shred(ShredRepairType::ShredForBlockId {
            slot: 105,
            index: 15,
            fec_set_merkle_root: Hash::new_unique(),
            block_id: Hash::new_unique(),
        });
        state.sent_requests.insert(recent_shred, now);

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

        BlockIdRepairService::process_block_id_repair_response((&packet).into(), &mut state);

        // Verify: FetchBlock event for parent was added to pending_repair_events
        assert_eq!(state.pending_repair_events.len(), 1);
        match &state.pending_repair_events[0] {
            RepairEvent::FetchBlock {
                slot: s,
                block_id: b,
            } => {
                assert_eq!(*s, parent_slot);
                assert_eq!(*b, parent_block_id);
            }
            _ => panic!("Expected FetchBlock event"),
        }

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

        BlockIdRepairService::process_block_id_repair_response((&packet).into(), &mut state);

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

        // Create a response with a nonce that wasn't registered
        let response = BlockIdRepairResponse::ParentFecSetCount {
            fec_set_count: 2,
            parent_info: (99, Hash::new_unique()),
            parent_proof: vec![0u8; SIZE_OF_MERKLE_PROOF_ENTRY * 2],
        };

        let invalid_nonce = 99999u32;
        let data = serialize_response(&response, invalid_nonce);
        let packet = make_packet(&data);

        BlockIdRepairService::process_block_id_repair_response((&packet).into(), &mut state);

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

        BlockIdRepairService::process_repair_event(event, &sharable_banks, &blockstore, &mut state);

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

        BlockIdRepairService::process_repair_event(event, &sharable_banks, &blockstore, &mut state);

        // Verify: No repair request was added (event was deferred)
        assert!(state.pending_repair_requests.is_empty());

        // Verify: Event was deferred
        assert_eq!(state.pending_repair_events.len(), 1);
        match &state.pending_repair_events[0] {
            RepairEvent::FetchBlock {
                slot: s,
                block_id: b,
            } => {
                assert_eq!(*s, slot);
                assert_eq!(*b, block_id);
            }
            _ => panic!("Expected FetchBlock event"),
        }

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

        BlockIdRepairService::process_repair_event(event, &sharable_banks, &blockstore, &mut state);

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

        BlockIdRepairService::process_repair_event(event, &sharable_banks, &blockstore, &mut state);

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

        BlockIdRepairService::process_repair_event(event, &sharable_banks, &blockstore, &mut state);

        // Verify: No request was added (slot at root is ignored)
        assert!(state.pending_repair_requests.is_empty());
        assert!(!state.requested_blocks.contains(&(slot, block_id)));
    }
}
