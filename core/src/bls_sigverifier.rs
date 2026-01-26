use {
    crate::cluster_info_vote_listener::VerifiedVoteSender,
    agave_bls_cert_verify::cert_verify::verify_cert_get_total_stake,
    crossbeam_channel::{Receiver, Sender, TrySendError},
    log::warn,
    rayon::iter::{IntoParallelIterator, ParallelIterator},
    solana_bls_signatures::{
        pubkey::{Pubkey as BlsPubkey, PubkeyProjective, VerifiablePubkey},
        signature::SignatureProjective,
    },
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_measure::measure::Measure,
    solana_perf::{
        deduper::{dedup_packets_and_count_discards, Deduper},
        packet::PacketBatch,
    },
    solana_pubkey::Pubkey,
    solana_rpc::alpenglow_last_voted::AlpenglowLastVoted,
    solana_runtime::{bank::Bank, bank_forks::SharableBanks},
    solana_streamer::streamer,
    solana_votor::{
        common::certificate_limits_and_vote_types,
        consensus_metrics::{ConsensusMetricsEvent, ConsensusMetricsEventSender},
        consensus_rewards,
    },
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateType, ConsensusMessage, VoteMessage},
        fraction::Fraction,
        reward_certificate::AddVoteMessage,
        vote::Vote,
    },
    std::{
        collections::{HashMap, HashSet},
        num::NonZeroU64,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder},
        time::{Duration, Instant},
    },
};

pub fn spawn_service(
    packet_receiver: Receiver<PacketBatch>,
    banks: SharableBanks,
    vote_sender: VerifiedVoteSender,
    reward_votes_sender: Sender<AddVoteMessage>,
    message_sender: Sender<ConsensusMessage>,
    metrics_sender: ConsensusMetricsEventSender,
    last_voted: Arc<AlpenglowLastVoted>,
    cluster_info: Arc<ClusterInfo>,
    leader_schedule: Arc<LeaderScheduleCache>,
) -> thread::JoinHandle<()> {
    let verifier = BLSSigVerifier::new(
        banks,
        vote_sender,
        reward_votes_sender,
        message_sender,
        metrics_sender,
        last_voted,
        cluster_info,
        leader_schedule,
    );

    Builder::new()
        .name("solSigVerBLS".to_string())
        .spawn(move || verifier.run(packet_receiver))
        .unwrap()
}

pub struct BLSSigVerifier {
    banks: SharableBanks,
    vote_sender: VerifiedVoteSender,
    reward_votes_sender: Sender<AddVoteMessage>,
    message_sender: Sender<ConsensusMessage>,
    metrics_sender: ConsensusMetricsEventSender,
    last_voted: Arc<AlpenglowLastVoted>,
    cluster_info: Arc<ClusterInfo>,
    leader_schedule: Arc<LeaderScheduleCache>,

    verified_certs: RwLock<HashSet<CertificateType>>,
    vote_payloads: RwLock<HashMap<Vote, Arc<Vec<u8>>>>,

    stats: Stats,
}

#[derive(Debug, Default)]
struct Stats {
    // Packet processing
    received: AtomicU64,
    received_discarded: AtomicU64,
    received_malformed: AtomicU64,
    received_no_epoch_stakes: AtomicU64,
    received_bad_rank: AtomicU64,
    received_old: AtomicU64,
    received_verified: AtomicU64,
    received_votes: AtomicU64,

    // Signature verification
    received_bad_signature_votes: AtomicU64,
    received_bad_signature_certs: AtomicU64,
    received_not_enough_stake: AtomicU64,
    total_valid_packets: AtomicU64,

    // Sending
    sent: AtomicU64,
    sent_failed: AtomicU64,
    verified_votes_sent: AtomicU64,
    verified_votes_sent_failed: AtomicU64,
    consensus_reward_send_failed: AtomicU64,

    // Performance metrics
    preprocess_count: AtomicU64,
    preprocess_elapsed_us: AtomicU64,
    votes_batch_count: AtomicU64,
    votes_batch_distinct_messages_count: AtomicU64,
    votes_batch_optimistic_elapsed_us: AtomicU64,
    votes_batch_parallel_verify_count: AtomicU64,
    votes_batch_parallel_verify_elapsed_us: AtomicU64,
    certs_batch_count: AtomicU64,
    certs_batch_elapsed_us: AtomicU64,
}

impl BLSSigVerifier {
    pub fn new(
        banks: SharableBanks,
        vote_sender: VerifiedVoteSender,
        reward_votes_sender: Sender<AddVoteMessage>,
        message_sender: Sender<ConsensusMessage>,
        metrics_sender: ConsensusMetricsEventSender,
        last_voted: Arc<AlpenglowLastVoted>,
        cluster_info: Arc<ClusterInfo>,
        leader_schedule: Arc<LeaderScheduleCache>,
    ) -> Self {
        Self {
            banks,
            vote_sender,
            reward_votes_sender,
            message_sender,
            metrics_sender,
            last_voted,
            cluster_info,
            leader_schedule,
            verified_certs: RwLock::new(HashSet::new()),
            vote_payloads: RwLock::new(HashMap::new()),
            stats: Stats::default(),
        }
    }

    fn run(mut self, receiver: Receiver<PacketBatch>) {
        const DEDUPER_NUM_BITS: u64 = 63_999_979;
        const DEDUPER_RESET_INTERVAL: Duration = Duration::from_secs(2);

        let mut rng = rand::thread_rng();
        let mut deduper = Deduper::<2, [u8]>::new(&mut rng, DEDUPER_NUM_BITS);
        let mut last_stats = Instant::now();

        loop {
            deduper.maybe_reset(&mut rng, 0.001, DEDUPER_RESET_INTERVAL);

            match streamer::recv_packet_batches(&receiver) {
                Ok((mut batches, _, _)) => {
                    dedup_packets_and_count_discards(&deduper, &mut batches);
                    if let Err(e) = self.process_batches(batches) {
                        if matches!(e.as_ref(), TrySendError::Disconnected(_)) {
                            break;
                        }
                    }
                }
                Err(streamer::StreamerError::RecvTimeout(e)) => {
                    if e.is_disconnected() {
                        break;
                    }
                    continue;
                }
                Err(_) => continue,
            }

            if last_stats.elapsed() > Duration::from_secs(1) {
                self.report_stats();
                last_stats = Instant::now();
            }
        }
    }

    pub fn verify_and_send_batches(&mut self, batches: Vec<PacketBatch>) {
        let _ = self.process_batches(batches);
    }

    /// Extract votes and certs from the packet batch, verify and send the results
    fn process_batches(
        &mut self,
        batches: Vec<PacketBatch>,
    ) -> Result<(), Box<TrySendError<ConsensusMessage>>> {
        let mut preprocess_timer = Measure::start("preprocess");
        let root = self.banks.root();
        let root_slot = root.slot();

        self.clean_cache(root_slot);

        let (votes, certs, metrics_events) = self.extract_messages(batches, &root);
        preprocess_timer.stop();
        self.stats.preprocess_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .preprocess_elapsed_us
            .fetch_add(preprocess_timer.as_us(), Ordering::Relaxed);

        let (verified_votes, verified_certs) = rayon::join(
            || self.verify_votes(votes, &root),
            || self.verify_certificates(certs, &root),
        );

        self.stats.total_valid_packets.fetch_add(
            (verified_votes.len() + verified_certs.len()) as u64,
            Ordering::Relaxed,
        );

        self.send_results(verified_votes, verified_certs, metrics_events)?;

        Ok(())
    }

    fn extract_messages(
        &self,
        batches: Vec<PacketBatch>,
        root: &Bank,
    ) -> (
        Vec<VoteMessage>,
        Vec<Certificate>,
        Vec<ConsensusMetricsEvent>,
    ) {
        let root_slot = root.slot();
        let mut votes = Vec::new();
        let mut certs = Vec::new();
        let mut metrics = Vec::new();

        for batch in batches {
            for packet in batch.into_iter() {
                self.extract_packet(
                    packet,
                    root_slot,
                    root,
                    &mut votes,
                    &mut certs,
                    &mut metrics,
                );
            }
        }

        (votes, certs, metrics)
    }

    /// Retreivie the bls message from the packet, performing any filtering checks as needed
    fn extract_packet(
        &self,
        packet: solana_perf::packet::PacketRef,
        root_slot: Slot,
        root: &Bank,
        votes: &mut Vec<VoteMessage>,
        certs: &mut Vec<Certificate>,
        metrics: &mut Vec<ConsensusMetricsEvent>,
    ) {
        self.stats.received.fetch_add(1, Ordering::Relaxed);

        if packet.meta().discard() {
            self.stats
                .received_discarded
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        let Ok(msg) = packet.deserialize_slice::<ConsensusMessage, _>(..) else {
            self.stats
                .received_malformed
                .fetch_add(1, Ordering::Relaxed);
            return;
        };

        match msg {
            ConsensusMessage::Vote(vote) if self.valid_vote(&vote, root_slot, root, metrics) => {
                votes.push(vote)
            }
            ConsensusMessage::Certificate(cert) if self.valid_certificate(&cert, root_slot) => {
                certs.push(cert)
            }
            _ => (),
        }
    }

    /// Check whether the vote is worth verifying
    fn valid_vote(
        &self,
        vote: &VoteMessage,
        root_slot: Slot,
        root: &Bank,
        metrics: &mut Vec<ConsensusMetricsEvent>,
    ) -> bool {
        // Consensus pool does not need votes for slots older than root slot,
        // however the rewards container may still need them.
        if vote.vote.slot() <= root_slot
            && !consensus_rewards::wants_vote(
                &self.cluster_info,
                &self.leader_schedule,
                root_slot,
                vote,
            )
        {
            self.stats.received_old.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        match self.get_vote_pubkey(vote, root) {
            Some(pubkey) => {
                metrics.push(ConsensusMetricsEvent::Vote {
                    id: pubkey,
                    vote: vote.vote,
                });
                self.stats.received_votes.fetch_add(1, Ordering::Relaxed);
                true
            }
            None => {
                let epoch = root.epoch_schedule().get_epoch(vote.vote.slot());
                let has_epoch_stakes = root.epoch_stakes_map().get(&epoch).is_some();

                if has_epoch_stakes {
                    self.stats.received_bad_rank.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.stats
                        .received_no_epoch_stakes
                        .fetch_add(1, Ordering::Relaxed);
                }
                false
            }
        }
    }

    /// Check whether the certificate is worth verifying
    fn valid_certificate(&self, cert: &Certificate, root_slot: Slot) -> bool {
        if cert.cert_type.slot() <= root_slot {
            self.stats.received_old.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let already_seen = self
            .verified_certs
            .read()
            .unwrap()
            .contains(&cert.cert_type);

        if already_seen {
            self.stats.received_verified.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        true
    }

    fn verify_votes(&self, votes: Vec<VoteMessage>, bank: &Bank) -> Vec<(VoteMessage, Pubkey)> {
        if votes.is_empty() {
            return Vec::new();
        }

        self.stats.votes_batch_count.fetch_add(1, Ordering::Relaxed);
        let mut votes_batch_timer = Measure::start("votes_batch_optimistic");

        // Group by payload
        let mut groups: HashMap<Vote, Vec<VoteMessage>> = HashMap::new();
        for vote in votes {
            groups.entry(vote.vote).or_default().push(vote);
        }

        let distinct_messages = groups.len();
        self.stats
            .votes_batch_distinct_messages_count
            .fetch_add(distinct_messages as u64, Ordering::Relaxed);

        let mut verified = Vec::new();

        for (vote, messages) in groups {
            let payload = self.get_vote_payload(&vote);

            // Get pubkeys
            let vote_data: Vec<_> = messages
                .into_iter()
                .filter_map(|msg| {
                    self.get_bls_pubkey(&msg, bank)
                        .zip(self.get_validator_pubkey(&msg, bank))
                        .map(|(bls_pubkey, pubkey)| (msg, bls_pubkey, pubkey))
                })
                .collect();

            if vote_data.is_empty() {
                continue;
            }

            // Try batch verification
            let pubkeys: Vec<BlsPubkey> = vote_data.iter().map(|(_, k, _)| *k).collect();
            let sigs: Vec<_> = vote_data.iter().map(|(v, _, _)| v.signature).collect();

            match self.batch_verify(&pubkeys, &sigs, &payload) {
                Ok(true) => {
                    // Optimistic batch verification succeeded
                    votes_batch_timer.stop();
                    self.stats
                        .votes_batch_optimistic_elapsed_us
                        .fetch_add(votes_batch_timer.as_us(), Ordering::Relaxed);

                    for (msg, _, pubkey) in vote_data {
                        verified.push((msg, pubkey));
                    }
                }
                _ => {
                    // Fallback to individual verification. TODO(ashwin): benchmark and parallelize
                    votes_batch_timer.stop();
                    let mut parallel_verify_timer = Measure::start("votes_batch_parallel_verify");
                    self.stats
                        .votes_batch_parallel_verify_count
                        .fetch_add(1, Ordering::Relaxed);

                    for (msg, bls_key, pubkey) in vote_data {
                        if bls_key
                            .verify_signature(&msg.signature, &payload)
                            .unwrap_or(false)
                        {
                            verified.push((msg, pubkey));
                        } else {
                            // TODO: blacklist
                            self.stats
                                .received_bad_signature_votes
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    parallel_verify_timer.stop();
                    self.stats
                        .votes_batch_parallel_verify_elapsed_us
                        .fetch_add(parallel_verify_timer.as_us(), Ordering::Relaxed);
                }
            }
        }

        verified
    }

    fn verify_certificates(&self, certs: Vec<Certificate>, bank: &Bank) -> Vec<Certificate> {
        if certs.is_empty() {
            return Vec::new();
        }

        self.stats.certs_batch_count.fetch_add(1, Ordering::Relaxed);
        let mut certs_batch_timer = Measure::start("certs_batch");

        let verified: Vec<_> = certs
            .into_par_iter()
            .filter(|cert| match self.verify_certificate(cert, bank) {
                Ok(()) => {
                    self.verified_certs.write().unwrap().insert(cert.cert_type);
                    true
                }
                Err(insufficient_stake) => {
                    if insufficient_stake {
                        self.stats
                            .received_not_enough_stake
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.stats
                            .received_bad_signature_certs
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    false
                }
            })
            .collect();

        certs_batch_timer.stop();
        self.stats
            .certs_batch_elapsed_us
            .fetch_add(certs_batch_timer.as_us(), Ordering::Relaxed);

        verified
    }

    fn verify_certificate(&self, cert: &Certificate, bank: &Bank) -> Result<(), bool> {
        let slot = cert.cert_type.slot();
        let epoch = bank.epoch_schedule().get_epoch(slot);

        let Some(stakes) = bank.epoch_stakes_map().get(&epoch) else {
            return Err(false);
        };

        let key_map = stakes.bls_pubkey_to_rank_map();
        let total_stake = stakes.total_stake();

        let Ok(aggregate_stake) = verify_cert_get_total_stake(cert, key_map.len(), |rank| {
            key_map
                .get_pubkey_and_stake(rank)
                .map(|(_, bls, stake)| (*stake, *bls))
        }) else {
            return Err(false);
        };

        let (required_fraction, _) = certificate_limits_and_vote_types(&cert.cert_type);
        let fraction = Fraction::new(aggregate_stake, NonZeroU64::new(total_stake).unwrap());

        if fraction >= required_fraction {
            Ok(())
        } else {
            Err(true) // insufficient stake
        }
    }

    fn send_results(
        &self,
        votes: Vec<(VoteMessage, Pubkey)>,
        certs: Vec<Certificate>,
        metrics: Vec<ConsensusMetricsEvent>,
    ) -> Result<(), Box<TrySendError<ConsensusMessage>>> {
        let root_slot = self.banks.root().slot();

        // Process votes
        let mut votes_by_pubkey: HashMap<Pubkey, Vec<Slot>> = HashMap::new();
        let mut last_voted: HashMap<Pubkey, Slot> = HashMap::new();
        let mut reward_votes = Vec::new();

        for (vote, pubkey) in votes {
            let slot = vote.vote.slot();

            if vote.vote.is_notarization_or_finalization() || vote.vote.is_notarize_fallback() {
                votes_by_pubkey.entry(pubkey).or_default().push(slot);
                last_voted
                    .entry(pubkey)
                    .and_modify(|s| *s = (*s).max(slot))
                    .or_insert(slot);
            }

            // Check if rewards container wants this vote
            if consensus_rewards::wants_vote(
                &self.cluster_info,
                &self.leader_schedule,
                root_slot,
                &vote,
            ) {
                reward_votes.push(vote);
            }

            self.send(ConsensusMessage::Vote(vote)).map_err(|e| *e)?;
        }

        // Send certificates
        for cert in certs {
            self.send(ConsensusMessage::Certificate(cert))
                .map_err(|e| *e)?;
        }

        // Send vote notifications
        for (pubkey, slots) in votes_by_pubkey {
            match self.vote_sender.try_send((pubkey, slots)) {
                Ok(()) => {
                    self.stats
                        .verified_votes_sent
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    self.stats
                        .verified_votes_sent_failed
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Send votes to rewards container
        let add_vote_msg = AddVoteMessage {
            votes: reward_votes,
        };
        match self.reward_votes_sender.try_send(add_vote_msg) {
            Ok(()) => (),
            Err(TrySendError::Full(_)) => {
                self.stats
                    .consensus_reward_send_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                warn!(
                    "could not send votes to reward container, receive side of channel is closed"
                );
            }
        }

        // Update tracking and metrics
        self.last_voted.update_last_voted(&last_voted);
        let _ = self.metrics_sender.send((Instant::now(), metrics));

        Ok(())
    }

    fn send(&self, msg: ConsensusMessage) -> Result<(), Box<TrySendError<ConsensusMessage>>> {
        match self.message_sender.try_send(msg) {
            Ok(()) => {
                self.stats.sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                self.stats.sent_failed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    fn batch_verify(
        &self,
        pubkeys: &[BlsPubkey],
        sigs: &[solana_bls_signatures::Signature],
        payload: &[u8],
    ) -> Result<bool, ()> {
        let agg_pubkey =
            PubkeyProjective::par_aggregate(pubkeys.into_par_iter()).map_err(|_| ())?;
        let agg_sig = SignatureProjective::par_aggregate(sigs.into_par_iter()).map_err(|_| ())?;
        Ok(agg_pubkey
            .verify_signature(&agg_sig, payload)
            .unwrap_or(false))
    }

    fn get_vote_payload(&self, vote: &Vote) -> Arc<Vec<u8>> {
        let vote_payloads = self.vote_payloads.read().unwrap();
        if let Some(payload) = vote_payloads.get(vote) {
            return Arc::clone(payload);
        }
        drop(vote_payloads);

        let mut vote_payloads = self.vote_payloads.write().unwrap();
        vote_payloads
            .entry(*vote)
            .or_insert_with(|| Arc::new(bincode::serialize(vote).unwrap()))
            .clone()
    }

    fn get_bls_pubkey(&self, vote: &VoteMessage, bank: &Bank) -> Option<BlsPubkey> {
        let epoch = bank.epoch_schedule().get_epoch(vote.vote.slot());
        bank.epoch_stakes_map()
            .get(&epoch)?
            .bls_pubkey_to_rank_map()
            .get_pubkey_and_stake(vote.rank.into())
            .map(|(_, bls, _)| *bls)
    }

    fn get_validator_pubkey(&self, vote: &VoteMessage, bank: &Bank) -> Option<Pubkey> {
        let epoch = bank.epoch_schedule().get_epoch(vote.vote.slot());
        bank.epoch_stakes_map()
            .get(&epoch)?
            .bls_pubkey_to_rank_map()
            .get_pubkey_and_stake(vote.rank.into())
            .map(|(pubkey, _, _)| *pubkey)
    }

    fn get_vote_pubkey(&self, vote: &VoteMessage, bank: &Bank) -> Option<Pubkey> {
        let epoch = bank.epoch_schedule().get_epoch(vote.vote.slot());
        bank.epoch_stakes_map()
            .get(&epoch)?
            .bls_pubkey_to_rank_map()
            .get_pubkey_and_stake(vote.rank.into())
            .map(|(pubkey, _, _)| (*pubkey))
    }

    fn clean_cache(&self, root_slot: Slot) {
        self.verified_certs
            .write()
            .unwrap()
            .retain(|cert| cert.slot() > root_slot);
        self.vote_payloads
            .write()
            .unwrap()
            .retain(|vote, _| vote.slot() > root_slot);
    }

    fn report_stats(&self) {
        let received = self.stats.received.swap(0, Ordering::Relaxed);
        if received == 0 {
            return;
        }

        datapoint_info!(
            "bls_sig_verifier_stats",
            (
                "preprocess_count",
                self.stats.preprocess_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "preprocess_elapsed_us",
                self.stats.preprocess_elapsed_us.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_count",
                self.stats.votes_batch_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_distinct_messages_count",
                self.stats
                    .votes_batch_distinct_messages_count
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_optimistic_elapsed_us",
                self.stats
                    .votes_batch_optimistic_elapsed_us
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_parallel_verify_count",
                self.stats
                    .votes_batch_parallel_verify_count
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "votes_batch_parallel_verify_elapsed_us",
                self.stats
                    .votes_batch_parallel_verify_elapsed_us
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "certs_batch_count",
                self.stats.certs_batch_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "certs_batch_elapsed_us",
                self.stats.certs_batch_elapsed_us.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "sent",
                self.stats.sent.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "sent_failed",
                self.stats.sent_failed.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verified_votes_sent",
                self.stats.verified_votes_sent.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "verified_votes_sent_failed",
                self.stats
                    .verified_votes_sent_failed
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            ("received", received as i64, i64),
            (
                "received_bad_rank",
                self.stats.received_bad_rank.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_bad_signature_certs",
                self.stats
                    .received_bad_signature_certs
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_bad_signature_votes",
                self.stats
                    .received_bad_signature_votes
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_not_enough_stake",
                self.stats
                    .received_not_enough_stake
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_discarded",
                self.stats.received_discarded.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_old",
                self.stats.received_old.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_verified",
                self.stats.received_verified.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_votes",
                self.stats.received_votes.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_no_epoch_stakes",
                self.stats
                    .received_no_epoch_stakes
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "received_malformed",
                self.stats.received_malformed.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "consensus_rewards_send_failed",
                self.stats
                    .consensus_reward_send_failed
                    .swap(0, Ordering::Relaxed) as i64,
                i64
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::cluster_info_vote_listener::VerifiedVoteReceiver,
        bitvec::prelude::{BitVec, Lsb0},
        crossbeam_channel::unbounded,
        solana_bls_signatures::Signature as BlsSignature,
        solana_gossip::contact_info::ContactInfo,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_perf::packet::{Packet, PacketBatch, PinnedPacketBatch},
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts, ValidatorVoteKeypairs,
            },
        },
        solana_signer::Signer,
        solana_signer_store::encode_base2,
        solana_streamer::socket::SocketAddrSpace,
        solana_votor::consensus_pool::certificate_builder::CertificateBuilder,
        solana_votor_messages::{
            consensus_message::{Certificate, CertificateType, ConsensusMessage, VoteMessage},
            vote::Vote,
        },
    };

    fn create_test_environment() -> (
        Vec<ValidatorVoteKeypairs>,
        Arc<RwLock<BankForks>>,
        BLSSigVerifier,
        VerifiedVoteReceiver,
        Receiver<ConsensusMessage>,
    ) {
        let validator_keypairs: Vec<_> =
            (0..10).map(|_| ValidatorVoteKeypairs::new_rand()).collect();
        let stakes: Vec<_> = (0..validator_keypairs.len())
            .map(|i| 1000 - i as u64)
            .collect();

        let genesis = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            stakes,
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();

        let (vote_sender, vote_receiver) = unbounded();
        let (reward_votes_sender, _reward_votes_receiver) = unbounded();
        let (message_sender, message_receiver) = unbounded();
        let (metrics_sender, _metrics_receiver) = unbounded();
        let last_voted = Arc::new(AlpenglowLastVoted::default());

        let keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = Arc::new(ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        ));
        let leader_schedule = Arc::new(LeaderScheduleCache::new_from_bank(&sharable_banks.root()));

        let verifier = BLSSigVerifier::new(
            sharable_banks,
            vote_sender,
            reward_votes_sender,
            message_sender,
            metrics_sender,
            last_voted,
            cluster_info,
            leader_schedule,
        );

        (
            validator_keypairs,
            bank_forks,
            verifier,
            vote_receiver,
            message_receiver,
        )
    }

    fn create_vote_message(
        keypairs: &[ValidatorVoteKeypairs],
        vote: Vote,
        rank: u16,
    ) -> VoteMessage {
        let bls_keypair = &keypairs[rank as usize].bls_keypair;
        let payload = bincode::serialize(&vote).unwrap();
        let signature = bls_keypair.sign(&payload).into();
        VoteMessage {
            vote,
            signature,
            rank,
        }
    }

    fn create_certificate(
        keypairs: &[ValidatorVoteKeypairs],
        cert_type: CertificateType,
        signer_ranks: &[u16],
    ) -> Certificate {
        let mut builder = CertificateBuilder::new(cert_type);
        let vote = cert_type.to_source_vote();

        let vote_messages: Vec<_> = signer_ranks
            .iter()
            .map(|&rank| create_vote_message(keypairs, vote, rank))
            .collect();

        builder.aggregate(&vote_messages).unwrap();
        builder.build().unwrap()
    }

    fn messages_to_batches(messages: &[ConsensusMessage]) -> Vec<PacketBatch> {
        let packets: Vec<_> = messages
            .iter()
            .map(|msg| {
                let mut packet = Packet::default();
                packet.populate_packet(None, msg).unwrap();
                packet
            })
            .collect();
        vec![PinnedPacketBatch::new(packets).into()]
    }

    #[test]
    fn test_verify_valid_vote() {
        let (keypairs, _, mut verifier, vote_receiver, message_receiver) =
            create_test_environment();

        let vote = Vote::new_finalization_vote(10);
        let vote_msg = create_vote_message(&keypairs, vote, 0);
        let msg = ConsensusMessage::Vote(vote_msg);

        verifier
            .process_batches(messages_to_batches(&[msg]))
            .unwrap();

        // Check vote was sent
        assert_eq!(message_receiver.try_iter().count(), 1);
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 1);
        assert_eq!(
            verifier.stats.total_valid_packets.load(Ordering::Relaxed),
            1
        );

        // Check verified vote notification
        let (pubkey, slots) = vote_receiver.try_recv().unwrap();
        assert_eq!(pubkey, keypairs[0].vote_keypair.pubkey());
        assert_eq!(slots, vec![10]);
    }

    #[test]
    fn test_verify_invalid_signature() {
        let (keypairs, _, mut verifier, vote_receiver, message_receiver) =
            create_test_environment();

        let vote = Vote::new_finalization_vote(10);
        let mut vote_msg = create_vote_message(&keypairs, vote, 0);
        // Corrupt signature
        vote_msg.signature = BlsSignature::default();
        let msg = ConsensusMessage::Vote(vote_msg);

        verifier
            .process_batches(messages_to_batches(&[msg]))
            .unwrap();

        // Check vote was rejected
        assert!(message_receiver.is_empty());
        assert!(vote_receiver.is_empty());
        assert_eq!(
            verifier
                .stats
                .received_bad_signature_votes
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            verifier.stats.total_valid_packets.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_verify_certificate_valid() {
        let (keypairs, _, mut verifier, _, message_receiver) = create_test_environment();

        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        // 7 out of 10 validators = 70% > 60% required
        let cert = create_certificate(&keypairs, cert_type, &[0, 1, 2, 3, 4, 5, 6]);
        let msg = ConsensusMessage::Certificate(cert);

        verifier
            .process_batches(messages_to_batches(&[msg]))
            .unwrap();

        // Check certificate was sent
        assert_eq!(message_receiver.try_iter().count(), 1);
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 1);
        assert_eq!(
            verifier.stats.total_valid_packets.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_verify_certificate_insufficient_stake() {
        let (keypairs, _, mut verifier, _, message_receiver) = create_test_environment();

        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        // Only 5 validators = 50% < 60% required
        let cert = create_certificate(&keypairs, cert_type, &[5, 6, 7, 8, 9]);
        let msg = ConsensusMessage::Certificate(cert);

        verifier
            .process_batches(messages_to_batches(&[msg]))
            .unwrap();

        // Check certificate was rejected
        assert!(message_receiver.is_empty());
        assert_eq!(
            verifier
                .stats
                .received_not_enough_stake
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            verifier.stats.total_valid_packets.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_old_messages_filtered() {
        let (keypairs, bank_forks, mut verifier, vote_receiver, message_receiver) =
            create_test_environment();

        // Advance root
        let bank5 = Bank::new_from_parent(
            bank_forks.read().unwrap().root_bank(),
            &Pubkey::default(),
            5,
        );
        bank_forks.write().unwrap().insert(bank5);
        bank_forks.write().unwrap().set_root(5, None, None).unwrap();

        // Create old vote (slot 3 < root 5)
        let old_vote = Vote::new_finalization_vote(3);
        let vote_msg = create_vote_message(&keypairs, old_vote, 0);

        // Create old certificate
        let cert_type = CertificateType::Notarize(4, Hash::new_unique());
        let cert = create_certificate(&keypairs, cert_type, &[0, 1, 2, 3, 4, 5, 6]);

        let messages = vec![
            ConsensusMessage::Vote(vote_msg),
            ConsensusMessage::Certificate(cert),
        ];

        verifier
            .process_batches(messages_to_batches(&messages))
            .unwrap();

        // Check both were filtered
        assert!(message_receiver.is_empty());
        assert!(vote_receiver.is_empty());
        assert_eq!(verifier.stats.received_old.load(Ordering::Relaxed), 2);
        assert_eq!(
            verifier.stats.total_valid_packets.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_malformed_packet() {
        let (_, _, mut verifier, _, message_receiver) = create_test_environment();

        let mut packet = Packet::default();
        packet.buffer_mut()[..4].copy_from_slice(&[0xff, 0xff, 0xff, 0xff]);
        packet.meta_mut().size = 4;

        let batch = PinnedPacketBatch::new(vec![packet]).into();
        verifier.process_batches(vec![batch]).unwrap();

        assert!(message_receiver.is_empty());
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_discarded_packet() {
        let (_, _, mut verifier, _, message_receiver) = create_test_environment();

        let mut packet = Packet::default();
        packet.meta_mut().set_discard(true);

        let batch = PinnedPacketBatch::new(vec![packet]).into();
        verifier.process_batches(vec![batch]).unwrap();

        assert!(message_receiver.is_empty());
        assert_eq!(verifier.stats.received_discarded.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_batch_verification_optimistic() {
        let (keypairs, _, mut verifier, vote_receiver, message_receiver) =
            create_test_environment();

        // Create multiple votes with same payload
        let vote = Vote::new_finalization_vote(10);
        let messages: Vec<_> = (0..5)
            .map(|i| ConsensusMessage::Vote(create_vote_message(&keypairs, vote, i)))
            .collect();

        verifier
            .process_batches(messages_to_batches(&messages))
            .unwrap();

        // All should be verified
        assert_eq!(message_receiver.try_iter().count(), 5);
        assert_eq!(vote_receiver.try_iter().count(), 5);
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 5);
        assert_eq!(
            verifier
                .stats
                .votes_batch_distinct_messages_count
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            verifier
                .stats
                .votes_batch_parallel_verify_count
                .load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_batch_verification_fallback() {
        let (keypairs, _, mut verifier, _, message_receiver) = create_test_environment();

        // Create votes where one has invalid signature
        let vote = Vote::new_finalization_vote(10);
        let mut messages = vec![];

        for i in 0..3 {
            messages.push(ConsensusMessage::Vote(create_vote_message(
                &keypairs, vote, i,
            )));
        }

        // Add one with bad signature
        let mut bad_vote = create_vote_message(&keypairs, vote, 3);
        bad_vote.signature = BlsSignature::default();
        messages.push(ConsensusMessage::Vote(bad_vote));

        verifier
            .process_batches(messages_to_batches(&messages))
            .unwrap();

        // Only 3 should be verified (fallback to individual verification)
        assert_eq!(message_receiver.try_iter().count(), 3);
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 3);
        assert_eq!(
            verifier
                .stats
                .received_bad_signature_votes
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            verifier
                .stats
                .votes_batch_parallel_verify_count
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_certificate_caching() {
        let (keypairs, _, mut verifier, _, message_receiver) = create_test_environment();

        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        let cert = create_certificate(&keypairs, cert_type, &[0, 1, 2, 3, 4, 5, 6]);
        let msg = ConsensusMessage::Certificate(cert.clone());

        // Send same certificate twice
        verifier
            .process_batches(messages_to_batches(&[msg.clone()]))
            .unwrap();
        verifier
            .process_batches(messages_to_batches(&[msg]))
            .unwrap();

        // Should only be sent once due to caching
        assert_eq!(message_receiver.try_iter().count(), 1);
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received_verified.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cache_cleanup() {
        let (_, _, verifier, _, _) = create_test_environment();

        // Add items to cache directly
        {
            // Add vote payloads
            let mut vote_payloads = verifier.vote_payloads.write().unwrap();
            let vote = Vote::new_finalization_vote(10);
            let payload = Arc::new(bincode::serialize(&vote).unwrap());
            vote_payloads.insert(vote, payload);

            let vote2 = Vote::new_finalization_vote(25);
            let payload2 = Arc::new(bincode::serialize(&vote2).unwrap());
            vote_payloads.insert(vote2, payload2);
            drop(vote_payloads);

            // Add verified certificates
            let mut verified_certs = verifier.verified_certs.write().unwrap();
            let cert_type = CertificateType::Notarize(10, Hash::new_unique());
            verified_certs.insert(cert_type);

            let cert_type2 = CertificateType::Notarize(25, Hash::new_unique());
            verified_certs.insert(cert_type2);
        }

        // Verify cache has items
        {
            let vote_payloads = verifier.vote_payloads.read().unwrap();
            assert_eq!(vote_payloads.len(), 2);
            drop(vote_payloads);

            let verified_certs = verifier.verified_certs.read().unwrap();
            assert_eq!(verified_certs.len(), 2);
        }

        // Clean cache with root slot 20 (should remove slot 10 items but keep slot 25)
        verifier.clean_cache(20);

        // Verify cache was cleaned properly
        {
            let vote_payloads = verifier.vote_payloads.read().unwrap();
            assert_eq!(vote_payloads.len(), 1); // Only slot 25 vote remains
            assert!(vote_payloads.contains_key(&Vote::new_finalization_vote(25)));
            drop(vote_payloads);

            let verified_certs = verifier.verified_certs.read().unwrap();
            assert_eq!(verified_certs.len(), 1); // Only slot 25 cert remains
            assert!(verified_certs.iter().any(|c| c.slot() == 25));
        }
    }

    #[test]
    fn test_invalid_rank() {
        let (_, _, mut verifier, _, message_receiver) = create_test_environment();

        let vote_msg = VoteMessage {
            vote: Vote::new_finalization_vote(10),
            signature: BlsSignature::default(),
            rank: 999, // Invalid rank
        };

        verifier
            .process_batches(messages_to_batches(&[ConsensusMessage::Vote(vote_msg)]))
            .unwrap();

        assert!(message_receiver.is_empty());
        assert_eq!(verifier.stats.received_bad_rank.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_multiple_distinct_vote_messages() {
        let (keypairs, _, mut verifier, _, message_receiver) = create_test_environment();

        // Create votes with different payloads
        let messages: Vec<_> = (0..3)
            .map(|i| {
                let vote = Vote::new_finalization_vote(10 + i);
                ConsensusMessage::Vote(create_vote_message(&keypairs, vote, i as u16))
            })
            .collect();

        verifier
            .process_batches(messages_to_batches(&messages))
            .unwrap();

        assert_eq!(message_receiver.try_iter().count(), 3);
        assert_eq!(
            verifier
                .stats
                .votes_batch_distinct_messages_count
                .load(Ordering::Relaxed),
            3
        );
    }

    #[test]
    fn test_certificate_with_invalid_signature() {
        let (_, _, mut verifier, _, message_receiver) = create_test_environment();

        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        let mut bitmap = BitVec::<u8, Lsb0>::new();
        bitmap.resize(7, true);

        let cert = Certificate {
            cert_type,
            signature: BlsSignature::default(), // Invalid signature
            bitmap: encode_base2(&bitmap).unwrap(),
        };

        verifier
            .process_batches(messages_to_batches(&[ConsensusMessage::Certificate(cert)]))
            .unwrap();

        assert!(message_receiver.is_empty());
        assert_eq!(
            verifier
                .stats
                .received_bad_signature_certs
                .load(Ordering::Relaxed),
            1
        );
    }
}
