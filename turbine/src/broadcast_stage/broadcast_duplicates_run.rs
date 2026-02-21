use {
    super::*,
    crate::cluster_nodes::ClusterNodesCache,
    agave_votor::event::VotorEventSender,
    crossbeam_channel::Sender,
    itertools::Itertools,
    solana_entry::{block_component::BlockComponent, entry::Entry},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shredder},
    solana_signature::Signature,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    std::collections::HashSet,
};

pub const MINIMUM_DUPLICATE_SLOT: Slot = 20;
pub const DUPLICATE_RATE: usize = 10;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum ClusterPartition {
    Stake(u64),
    Pubkey(Vec<Pubkey>),
}

#[derive(Clone, Debug)]
pub struct BroadcastDuplicatesConfig {
    /// Amount of stake (excluding the leader) or a set of validator pubkeys
    /// to send a duplicate version of some slots to.
    /// Note this is sampled from a list of stakes sorted least to greatest.
    pub partition: ClusterPartition,
    /// If passed `Some(receiver)`, will signal all the duplicate slots via the given
    /// `receiver`
    pub duplicate_slot_sender: Option<Sender<Slot>>,
}

#[derive(Clone)]
pub(super) struct BroadcastDuplicatesRun {
    config: BroadcastDuplicatesConfig,
    current_slot: Slot,
    chained_merkle_root: Hash,
    carryover_entry: Option<WorkingBankEntryMarker>,
    next_shred_index: u32,
    next_code_index: u32,
    shred_version: u16,
    recent_blockhash: Option<Hash>,
    prev_entry_hash: Option<Hash>,
    num_slots_broadcasted: usize,
    cluster_nodes_cache: Arc<ClusterNodesCache<BroadcastStage>>,
    original_last_data_shreds: Arc<Mutex<HashSet<Signature>>>,
    partition_last_data_shreds: Arc<Mutex<HashSet<Signature>>>,
    reed_solomon_cache: Arc<ReedSolomonCache>,
    migration_status: Arc<MigrationStatus>,
}

impl BroadcastDuplicatesRun {
    pub(super) fn new(
        shred_version: u16,
        config: BroadcastDuplicatesConfig,
        migration_status: Arc<MigrationStatus>,
    ) -> Self {
        let cluster_nodes_cache = Arc::new(ClusterNodesCache::<BroadcastStage>::new(
            CLUSTER_NODES_CACHE_NUM_EPOCH_CAP,
            CLUSTER_NODES_CACHE_TTL,
        ));
        Self {
            config,
            chained_merkle_root: Hash::default(),
            carryover_entry: None,
            next_shred_index: u32::MAX,
            next_code_index: 0,
            shred_version,
            current_slot: 0,
            recent_blockhash: None,
            prev_entry_hash: None,
            num_slots_broadcasted: 0,
            cluster_nodes_cache,
            original_last_data_shreds: Arc::<Mutex<HashSet<Signature>>>::default(),
            partition_last_data_shreds: Arc::<Mutex<HashSet<Signature>>>::default(),
            reed_solomon_cache: Arc::<ReedSolomonCache>::default(),
            migration_status,
        }
    }
}

impl BroadcastRun for BroadcastDuplicatesRun {
    fn run(
        &mut self,
        keypair: &Keypair,
        blockstore: &Blockstore,
        receiver: &Receiver<WorkingBankEntryMarker>,
        socket_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        _votor_event_sender: &VotorEventSender,
    ) -> Result<()> {
        // 1) Pull entries from banking stage
        let mut stats = ProcessShredsStats::default();
        let receive_results =
            broadcast_utils::recv_slot_entries(receiver, &mut self.carryover_entry, &mut stats)?;
        let bank = receive_results.bank.clone();
        let last_tick_height = receive_results.last_tick_height;

        let send_header = if bank.slot() != self.current_slot {
            self.chained_merkle_root = broadcast_utils::get_chained_merkle_root_from_parent(
                bank.slot(),
                bank.parent_slot(),
                blockstore,
            )
            .unwrap();
            self.next_shred_index = 0;
            self.next_code_index = 0;
            self.current_slot = bank.slot();
            self.prev_entry_hash = None;
            self.num_slots_broadcasted += 1;

            self.migration_status.is_alpenglow_enabled()
        } else {
            false
        };

        // Check if we have a marker - if so, nothing to duplicate
        if matches!(receive_results.component, BlockComponent::BlockMarker(_)) {
            return Ok(());
        }

        // Update the recent blockhash based on transactions in the entries
        for entry in receive_results.entries() {
            if !entry.transactions.is_empty() {
                self.recent_blockhash = Some(*entry.transactions[0].message.recent_blockhash());
                break;
            }
        }

        // 2) Convert entries to shreds + generate coding shreds. Set a garbage PoH on the last entry
        // in the slot to make verification fail on validators
        let (component, last_entries) = {
            if last_tick_height == bank.max_tick_height()
                && bank.slot() > MINIMUM_DUPLICATE_SLOT
                && self.num_slots_broadcasted % DUPLICATE_RATE == 0
                && self.recent_blockhash.is_some()
            {
                // Extract the last entry from the component
                let (original_last_entry, prev_entry_hash, component) =
                    if let BlockComponent::EntryBatch(mut entries) = receive_results.component {
                        // Get the last entry from the batch (should be the final tick)
                        let last_entry = entries.pop().expect("EntryBatch should not be empty");

                        // Try to get the second-to-last entry from this batch
                        let prev_hash = entries.last().map(|e| e.hash).or(self.prev_entry_hash);

                        // Create component with remaining entries
                        let component = BlockComponent::EntryBatch(entries);

                        (last_entry, prev_hash, component)
                    } else {
                        panic!("Expected EntryBatch, found BlockMarker");
                    };

                // Last entry has to be a tick
                assert!(original_last_entry.is_tick());

                if let Some(prev_entry_hash) = prev_entry_hash {
                    // Inject an extra entry before the last tick
                    let extra_tx = system_transaction::transfer(
                        keypair,
                        &Pubkey::new_unique(),
                        1,
                        self.recent_blockhash.unwrap(),
                    );
                    let new_extra_entry = Entry::new(&prev_entry_hash, 1, vec![extra_tx]);

                    // This will only work with sleepy tick producer where the hashing
                    // checks in replay are turned off, because we're introducing an extra
                    // hash for the last tick in the `new_extra_entry`.
                    let new_last_entry = Entry::new(
                        &new_extra_entry.hash,
                        original_last_entry.num_hashes,
                        vec![],
                    );

                    (
                        component,
                        Some((original_last_entry, vec![new_extra_entry, new_last_entry])),
                    )
                } else {
                    (component, None)
                }
            } else {
                (receive_results.component, None)
            }
        };

        self.prev_entry_hash = last_entries
            .as_ref()
            .map(|(original_last_entry, _)| original_last_entry.hash)
            .or_else(|| match &component {
                BlockComponent::EntryBatch(entries) => entries.last().map(|e| e.hash),
                BlockComponent::BlockMarker(_) => None,
            });

        let shredder = Shredder::new(
            bank.slot(),
            bank.parent().unwrap().slot(),
            (bank.tick_height() % bank.ticks_per_slot()) as u8,
            self.shred_version,
        )
        .expect("Expected to create a new shredder");

        let (header_data_shreds, header_coding_shreds) = if send_header {
            let header = produce_block_header(bank.parent_slot(), self.chained_merkle_root);

            shredder.component_to_merkle_shreds_for_tests(
                keypair,
                &BlockComponent::BlockMarker(header),
                false,
                Some(self.chained_merkle_root),
                self.next_shred_index,
                self.next_code_index,
                &self.reed_solomon_cache,
                &mut stats,
            )
        } else {
            (vec![], vec![])
        };
        if let Some(shred) = header_data_shreds.iter().max_by_key(|shred| shred.index()) {
            self.chained_merkle_root = shred.merkle_root().unwrap();
        }
        self.next_shred_index += header_data_shreds.len() as u32;
        if let Some(index) = header_coding_shreds.iter().map(Shred::index).max() {
            self.next_code_index = index + 1;
        }

        let (component_data_shreds, component_coding_shreds) = shredder
            .component_to_merkle_shreds_for_tests(
                keypair,
                &component,
                last_tick_height == bank.max_tick_height() && last_entries.is_none(),
                Some(self.chained_merkle_root),
                self.next_shred_index,
                self.next_code_index,
                &self.reed_solomon_cache,
                &mut stats,
            );
        if let Some(shred) = component_data_shreds
            .iter()
            .max_by_key(|shred| shred.index())
        {
            self.chained_merkle_root = shred.merkle_root().unwrap();
        }
        self.next_shred_index += component_data_shreds.len() as u32;
        if let Some(index) = component_coding_shreds.iter().map(Shred::index).max() {
            self.next_code_index = index + 1;
        }

        let data_shreds = header_data_shreds
            .into_iter()
            .chain(component_data_shreds)
            .collect_vec();

        let last_shreds =
            last_entries.map(|(original_last_entry, duplicate_extra_last_entries)| {
                let (original_last_data_shred, _) = shredder.component_to_merkle_shreds_for_tests(
                    keypair,
                    &BlockComponent::EntryBatch(vec![original_last_entry]),
                    true,
                    Some(self.chained_merkle_root),
                    self.next_shred_index,
                    self.next_code_index,
                    &self.reed_solomon_cache,
                    &mut stats,
                );
                // Don't mark the last shred as last so that validators won't
                // know that they've gotten all the shreds, and will continue
                // trying to repair.
                let (partition_last_data_shred, _) = shredder.component_to_merkle_shreds_for_tests(
                    keypair,
                    &BlockComponent::EntryBatch(duplicate_extra_last_entries),
                    true,
                    Some(self.chained_merkle_root),
                    self.next_shred_index,
                    self.next_code_index,
                    &self.reed_solomon_cache,
                    &mut stats,
                );
                let sigs: Vec<_> = partition_last_data_shred
                    .iter()
                    .map(|s| (s.signature(), s.index()))
                    .collect();
                info!(
                    "duplicate signatures for slot {}, sigs: {:?}",
                    bank.slot(),
                    sigs,
                );

                assert_eq!(
                    original_last_data_shred.len(),
                    partition_last_data_shred.len()
                );
                self.next_shred_index += u32::try_from(original_last_data_shred.len()).unwrap();
                (original_last_data_shred, partition_last_data_shred)
            });

        let data_shreds = Arc::new(data_shreds);
        blockstore_sender.send((data_shreds.clone(), None))?;

        // 3) Start broadcast step
        info!(
            "{} Sending good shreds for slot {} to network",
            keypair.pubkey(),
            data_shreds.first().unwrap().slot()
        );
        assert!(data_shreds.iter().all(|shred| shred.slot() == bank.slot()));
        socket_sender.send((data_shreds, None))?;

        // Special handling of last shred to cause partition
        if let Some((original_last_data_shred, partition_last_data_shred)) = last_shreds {
            let pubkey = keypair.pubkey();
            self.original_last_data_shreds.lock().unwrap().extend(
                original_last_data_shred.iter().map(|shred| {
                    assert!(shred.verify(&pubkey));
                    shred.signature()
                }),
            );
            self.partition_last_data_shreds.lock().unwrap().extend(
                partition_last_data_shred.iter().map(|shred| {
                    info!("adding {} to partition set", shred.signature());
                    assert!(shred.verify(&pubkey));
                    shred.signature()
                }),
            );
            let original_last_data_shred = Arc::new(original_last_data_shred);
            let partition_last_data_shred = Arc::new(partition_last_data_shred);

            // Store the original shreds that this node replayed
            blockstore_sender.send((original_last_data_shred.clone(), None))?;

            assert!(original_last_data_shred
                .iter()
                .all(|shred| shred.slot() == bank.slot()));
            assert!(partition_last_data_shred
                .iter()
                .all(|shred| shred.slot() == bank.slot()));

            if let Some(duplicate_slot_sender) = &self.config.duplicate_slot_sender {
                let _ = duplicate_slot_sender.send(bank.slot());
            }
            socket_sender.send((original_last_data_shred, None))?;
            socket_sender.send((partition_last_data_shred, None))?;
        }
        Ok(())
    }

    fn transmit(
        &mut self,
        receiver: &TransmitReceiver,
        cluster_info: &ClusterInfo,
        sock: BroadcastSocket,
        bank_forks: &RwLock<BankForks>,
        _quic_endpoint_sender: &AsyncSender<(SocketAddr, Bytes)>,
    ) -> Result<()> {
        let (shreds, _) = receiver.recv()?;
        if shreds.is_empty() {
            return Ok(());
        }
        let slot = shreds.first().unwrap().slot();
        assert!(shreds.iter().all(|shred| shred.slot() == slot));
        let (root_bank, working_bank) = {
            let bank_forks = bank_forks.read().unwrap();
            (bank_forks.root_bank(), bank_forks.working_bank())
        };
        let self_pubkey = cluster_info.id();
        // Create cluster partition.
        let cluster_partition: HashSet<Pubkey> = {
            match &self.config.partition {
                ClusterPartition::Stake(partition_total_stake) => {
                    let mut cumulative_stake = 0;
                    let epoch = root_bank.get_leader_schedule_epoch(slot);
                    root_bank
                        .epoch_staked_nodes(epoch)
                        .unwrap()
                        .iter()
                        .filter(|(pubkey, _)| **pubkey != self_pubkey)
                        .sorted_by_key(|(pubkey, stake)| (**stake, **pubkey))
                        .take_while(|(_, stake)| {
                            cumulative_stake += *stake;
                            cumulative_stake <= *partition_total_stake
                        })
                        .map(|(pubkey, _)| *pubkey)
                        .collect()
                }
                ClusterPartition::Pubkey(pubkeys) => pubkeys.iter().cloned().collect(),
            }
        };

        // Broadcast data
        let cluster_nodes =
            self.cluster_nodes_cache
                .get(slot, &root_bank, &working_bank, cluster_info);
        let socket_addr_space = cluster_info.socket_addr_space();
        let packets: Vec<_> = shreds
            .iter()
            .filter_map(|shred| {
                let node = cluster_nodes.get_broadcast_peer(&shred.id())?;
                if !socket_addr_space.check(&node.tvu(Protocol::UDP)?) {
                    return None;
                }
                if self
                    .original_last_data_shreds
                    .lock()
                    .unwrap()
                    .remove(shred.signature())
                {
                    if cluster_partition.contains(node.pubkey()) {
                        info!(
                            "Not broadcasting original shred index {}, slot {} to partition node \
                             {}",
                            shred.index(),
                            shred.slot(),
                            node.pubkey(),
                        );
                        return None;
                    }
                } else if self
                    .partition_last_data_shreds
                    .lock()
                    .unwrap()
                    .remove(shred.signature())
                {
                    // If the shred is part of the partition, broadcast it directly to the
                    // partition node. This is to account for cases when the partition stake
                    // is small such as in `test_duplicate_shreds_broadcast_leader()`, then
                    // the partition node is never selected by get_broadcast_peer()
                    return Some(
                        cluster_partition
                            .iter()
                            .filter_map(|pubkey| {
                                info!(
                                    "Broadcasting partition shred index {}, slot {} to partition \
                                     node {}",
                                    shred.index(),
                                    shred.slot(),
                                    pubkey,
                                );
                                let tvu = cluster_info.lookup_contact_info(pubkey, |node| {
                                    node.tvu(Protocol::UDP)
                                })??;
                                Some((shred.payload(), tvu))
                            })
                            .collect(),
                    );
                }

                Some(vec![(shred.payload(), node.tvu(Protocol::UDP)?)])
            })
            .flatten()
            .collect();

        let sock = match sock {
            BroadcastSocket::Udp(sock) => sock,
            BroadcastSocket::Xdp(_) => {
                panic!("Xdp not supported for duplicate shreds run");
            }
        };
        batch_send(sock, packets).map_err(|SendPktsError::IoError(err, _)| Error::Io(err))
    }

    fn record(&mut self, receiver: &RecordReceiver, blockstore: &Blockstore) -> Result<()> {
        let (all_shreds, _) = receiver.recv()?;
        blockstore
            .insert_shreds(all_shreds.to_vec(), None, true)
            .expect("Failed to insert shreds in blockstore");
        Ok(())
    }
}
