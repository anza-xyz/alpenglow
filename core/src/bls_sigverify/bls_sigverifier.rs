//! The BLS signature verifier.

use {
    crate::{
        bls_sigverify::{
            bls_cert_sigverify::verify_and_send_certificates,
            bls_vote_sigverify::{verify_and_send_votes, VoteToVerify},
            error::BLSSigVerifyError,
            stats::BLSSigVerifierStats,
        },
        cluster_info_vote_listener::VerifiedVoteSender,
    },
    crossbeam_channel::Sender,
    solana_bls_signatures::pubkey::Pubkey as BlsPubkey,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_measure::measure::Measure,
    solana_pubkey::Pubkey,
    solana_rpc::alpenglow_last_voted::AlpenglowLastVoted,
    solana_runtime::{bank::Bank, bank_forks::SharableBanks},
    solana_streamer::packet::PacketBatch,
    solana_votor::{
        consensus_metrics::{ConsensusMetricsEvent, ConsensusMetricsEventSender},
        consensus_rewards::{self},
    },
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateType, ConsensusMessage, VoteMessage},
        reward_certificate::AddVoteMessage,
    },
    std::{
        collections::{HashMap, HashSet},
        sync::{atomic::Ordering, Arc, RwLock},
        time::Instant,
    },
};

pub struct BLSSigVerifier {
    votes_for_repair_sender: VerifiedVoteSender,
    reward_votes_sender: Sender<AddVoteMessage>,
    message_sender: Sender<ConsensusMessage>,
    sharable_banks: SharableBanks,
    stats: BLSSigVerifierStats,
    verified_certs: RwLock<HashSet<CertificateType>>,
    consensus_metrics_sender: ConsensusMetricsEventSender,
    last_checked_root_slot: Slot,
    alpenglow_last_voted: Arc<AlpenglowLastVoted>,
    cluster_info: Arc<ClusterInfo>,
    leader_schedule: Arc<LeaderScheduleCache>,

    // Recycled buffers implementing the Buffer Recycling pattern.
    //
    // The `extract_and_filter_messages` function demultiplexes mixed consensus messages into
    // distinct vectors for votes, certificates, and metrics. Since the quantity of each message
    // type is dynamic, we use these persistent buffers (`vote_buffer`, `cert_buffer`, `metric_buffer`)
    // to amortize the cost of heap allocations over time.
    vote_buffer: Vec<VoteToVerify>,
    cert_buffer: Vec<Certificate>,
    metric_buffer: Vec<ConsensusMetricsEvent>,
}

impl BLSSigVerifier {
    pub fn new(
        sharable_banks: SharableBanks,
        votes_for_repair_sender: VerifiedVoteSender,
        reward_votes_sender: Sender<AddVoteMessage>,
        message_sender: Sender<ConsensusMessage>,
        consensus_metrics_sender: ConsensusMetricsEventSender,
        alpenglow_last_voted: Arc<AlpenglowLastVoted>,
        cluster_info: Arc<ClusterInfo>,
        leader_schedule: Arc<LeaderScheduleCache>,
    ) -> Self {
        Self {
            sharable_banks,
            votes_for_repair_sender,
            reward_votes_sender,
            message_sender,
            stats: BLSSigVerifierStats::default(),
            verified_certs: RwLock::new(HashSet::new()),
            consensus_metrics_sender,
            last_checked_root_slot: 0,
            alpenglow_last_voted,
            cluster_info,
            leader_schedule,
            vote_buffer: Vec::new(),
            cert_buffer: Vec::new(),
            metric_buffer: Vec::new(),
        }
    }

    pub(super) fn verify_and_send_batches(
        &mut self,
        batches: Vec<PacketBatch>,
    ) -> Result<(), BLSSigVerifyError> {
        let mut preprocess_time = Measure::start("preprocess");
        let mut last_voted_slots: HashMap<Pubkey, Slot> = HashMap::new();
        let root_bank = self.sharable_banks.root();

        // Prune entries <= the new root slot
        if self.last_checked_root_slot < root_bank.slot() {
            self.prune_caches(&root_bank);
        }

        // Recycle buffers.
        // Note: `cert_buffer` and `metric_buffer` are usually empty from draining,
        // but we clear them explicitly to guarantee a clean state.
        self.vote_buffer.clear();
        self.cert_buffer.clear();
        self.metric_buffer.clear();

        self.extract_and_filter_messages(batches, &root_bank);

        preprocess_time.stop();
        self.stats.preprocess_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .preprocess_elapsed_us
            .fetch_add(preprocess_time.as_us(), Ordering::Relaxed);

        // Destructure self to borrow fields independently for the closure
        let vote_buffer = &self.vote_buffer;
        let stats = &self.stats;
        let cluster_info = &self.cluster_info;
        let leader_schedule = &self.leader_schedule;
        let message_sender = &self.message_sender;
        let votes_for_repair_sender = &self.votes_for_repair_sender;
        let metric_buffer = &mut self.metric_buffer;
        let cert_buffer = &mut self.cert_buffer;
        let verified_certs = &self.verified_certs;
        let reward_votes_sender = &self.reward_votes_sender;

        // Perform signature verification
        let (votes_result, certs_result) = rayon::join(
            || {
                verify_and_send_votes(
                    vote_buffer,
                    &root_bank,
                    stats,
                    cluster_info,
                    leader_schedule,
                    message_sender,
                    votes_for_repair_sender,
                    reward_votes_sender,
                    &mut last_voted_slots,
                    metric_buffer,
                )
            },
            || {
                verify_and_send_certificates(
                    cert_buffer,
                    &root_bank,
                    verified_certs,
                    stats,
                    message_sender,
                )
            },
        );

        votes_result?;
        certs_result?;

        // Send to RPC service for last voted tracking
        self.alpenglow_last_voted
            .update_last_voted(&last_voted_slots);

        // Send to metrics service for metrics aggregation
        Self::send_consensus_metrics(&mut self.metric_buffer, &self.consensus_metrics_sender);

        self.stats.maybe_report_stats();
        Ok(())
    }

    fn prune_caches(&mut self, root_bank: &Bank) {
        self.last_checked_root_slot = root_bank.slot();
        self.verified_certs
            .write()
            .unwrap()
            .retain(|cert| cert.slot() > root_bank.slot());
    }

    fn extract_and_filter_messages(&mut self, batches: Vec<PacketBatch>, root_bank: &Bank) {
        for packet in batches.iter().flatten() {
            self.stats.received.fetch_add(1, Ordering::Relaxed);

            if packet.meta().discard() {
                self.stats
                    .received_discarded
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let Ok(message) = packet.deserialize_slice::<ConsensusMessage, _>(..) else {
                self.stats
                    .received_malformed
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };

            match message {
                ConsensusMessage::Vote(vote_message) => {
                    let Some((pubkey, bls_pubkey)) = self.resolve_voter(&vote_message, root_bank)
                    else {
                        continue;
                    };

                    if self.check_stale_vote(&vote_message, root_bank) {
                        continue;
                    }

                    // Push the vote to the recycled buffer.
                    //
                    // Note: We must store owned copies of `VoteMessage` here because standard
                    // `serde` deserialization creates new instances on the stack from the packet
                    // bytes. We cannot hold references to these stack-allocated objects beyond
                    // the current loop iteration.
                    //
                    // If we switch to a zero-copy deserialization framework (e.g. `bytemuck::Pod`)
                    // that allows viewing fields directly from the raw packet bytes, `VoteToVerify`
                    // could be refactored to hold references/slices, avoiding the memory copy
                    // entirely.
                    self.vote_buffer.push(VoteToVerify {
                        vote_message,
                        bls_pubkey,
                        pubkey,
                    });
                }

                ConsensusMessage::Certificate(cert) => {
                    // Only need certs newer than root slot
                    if cert.cert_type.slot() <= root_bank.slot() {
                        self.stats.received_old.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    self.cert_buffer.push(cert);
                }
            }

            // Filter already verified certifies
            if !self.cert_buffer.is_empty() {
                let verified_certs = self.verified_certs.read().unwrap();
                let len_before = self.cert_buffer.len();

                // apply in-place filtering
                self.cert_buffer
                    .retain(|cert| !verified_certs.contains(&cert.cert_type));

                let len_after = self.cert_buffer.len();
                let duplicates = len_before - len_after;
                if duplicates > 0 {
                    self.stats
                        .received_verified
                        .fetch_add(duplicates as u64, Ordering::Relaxed);
                }
            }
        }
    }

    fn send_consensus_metrics(
        metrics: &mut Vec<ConsensusMetricsEvent>,
        sender: &ConsensusMetricsEventSender,
    ) {
        if metrics.is_empty() {
            return;
        }

        // Drain the buffer to create the Vec expected by the channel,
        // leaving `metrics` empty but with capacity preserved.
        //
        // clippy suggests `mem::take` which would steal the buffer's capacity,
        // defeating the buffer recycling optimization.
        #[allow(clippy::drain_collect)]
        let metrics_vec: Vec<_> = metrics.drain(..).collect();

        if sender.send((Instant::now(), metrics_vec)).is_err() {
            warn!("could not send consensus metrics, receive side of channel is closed");
        }
    }

    fn resolve_voter(
        &mut self,
        vote_message: &VoteMessage,
        root_bank: &Bank,
    ) -> Option<(Pubkey, BlsPubkey)> {
        let Some(rank_map) = root_bank.get_rank_map(vote_message.vote.slot()) else {
            self.stats
                .received_no_epoch_stakes
                .fetch_add(1, Ordering::Relaxed);
            return None;
        };

        // Invalid rank
        let (pubkey, bls_pubkey, _) = rank_map
            .get_pubkey_and_stake(vote_message.rank.into())
            .or_else(|| {
                self.stats.received_bad_rank.fetch_add(1, Ordering::Relaxed);
                None
            })?;

        Some((*pubkey, *bls_pubkey))
    }

    fn check_stale_vote(&self, vote_message: &VoteMessage, root_bank: &Bank) -> bool {
        // if newer than root, we should keep
        if vote_message.vote.slot() > root_bank.slot() {
            return false;
        }

        // even if old, we might still want it for the rewards container
        if consensus_rewards::wants_vote(
            &self.cluster_info,
            &self.leader_schedule,
            root_bank.slot(),
            vote_message,
        ) {
            return false;
        }

        // otherwise, record stats and discard
        self.stats.received_old.fetch_add(1, Ordering::Relaxed);
        true
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            bls_sigverify::stats::STATS_INTERVAL_DURATION,
            cluster_info_vote_listener::VerifiedVoteReceiver,
        },
        bitvec::prelude::{BitVec, Lsb0},
        crossbeam_channel::Receiver,
        solana_bls_signatures::{Signature, Signature as BLSSignature},
        solana_gossip::contact_info::ContactInfo,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_perf::packet::{Packet, PinnedPacketBatch},
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
        std::time::Instant,
    };

    fn create_keypairs_and_bls_sig_verifier_with_channels(
        votes_for_repair_sender: VerifiedVoteSender,
        message_sender: Sender<ConsensusMessage>,
        consensus_metrics_sender: ConsensusMetricsEventSender,
        reward_votes_sender: Sender<AddVoteMessage>,
    ) -> (Vec<ValidatorVoteKeypairs>, BLSSigVerifier) {
        // Create 10 node validatorvotekeypairs vec
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let stakes_vec = (0..validator_keypairs.len())
            .map(|i| 1_000 - i as u64)
            .collect::<Vec<_>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            stakes_vec,
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();
        let alpenglow_last_voted = Arc::new(AlpenglowLastVoted::default());
        let keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = Arc::new(ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        ));
        let leader_schedule = Arc::new(LeaderScheduleCache::new_from_bank(&sharable_banks.root()));
        (
            validator_keypairs,
            BLSSigVerifier::new(
                sharable_banks,
                votes_for_repair_sender,
                reward_votes_sender,
                message_sender,
                consensus_metrics_sender,
                alpenglow_last_voted,
                cluster_info,
                leader_schedule,
            ),
        )
    }

    fn create_keypairs_and_bls_sig_verifier() -> (
        Vec<ValidatorVoteKeypairs>,
        BLSSigVerifier,
        VerifiedVoteReceiver,
        Receiver<ConsensusMessage>,
    ) {
        let (votes_for_repair_sender, votes_for_repair_receiver) = crossbeam_channel::unbounded();
        let (message_sender, message_receiver) = crossbeam_channel::unbounded();
        let (consensus_metrics_sender, consensus_metrics_receiver) = crossbeam_channel::unbounded();
        let (reward_votes_sender, reward_votes_receiver) = crossbeam_channel::unbounded();
        // the sigverifier sends msgs on some channels which the tests do not inspect.
        // use a thread to keep the receive side of these channels alive so that the sending of msgs doesn't fail.
        // the thread does not need to be joined and will exit when the sigverifier is dropped.
        std::thread::spawn(move || {
            while consensus_metrics_receiver.recv().is_ok() {}
            while reward_votes_receiver.recv().is_ok() {}
        });
        let (keypairs, verifier) = create_keypairs_and_bls_sig_verifier_with_channels(
            votes_for_repair_sender,
            message_sender,
            consensus_metrics_sender,
            reward_votes_sender,
        );
        (
            keypairs,
            verifier,
            votes_for_repair_receiver,
            message_receiver,
        )
    }

    fn create_signed_vote_message(
        validator_keypairs: &[ValidatorVoteKeypairs],
        vote: Vote,
        rank: usize,
    ) -> VoteMessage {
        let bls_keypair = &validator_keypairs[rank].bls_keypair;
        let payload = bincode::serialize(&vote).expect("Failed to serialize vote");
        let signature: BLSSignature = bls_keypair.sign(&payload).into();
        VoteMessage {
            vote,
            signature,
            rank: rank as u16,
        }
    }

    fn create_signed_certificate_message(
        validator_keypairs: &[ValidatorVoteKeypairs],
        cert_type: CertificateType,
        ranks: &[usize],
    ) -> Certificate {
        let mut builder = CertificateBuilder::new(cert_type);
        // Assumes Base2 encoding (single vote type) for simplicity in this helper.
        let vote = cert_type.to_source_vote();
        let vote_messages: Vec<VoteMessage> = ranks
            .iter()
            .map(|&rank| create_signed_vote_message(validator_keypairs, vote, rank))
            .collect();

        builder
            .aggregate(&vote_messages)
            .expect("Failed to aggregate votes");
        builder.build().expect("Failed to build certificate")
    }

    #[test]
    fn test_blssigverifier_send_packets() {
        let (validator_keypairs, mut verifier, votes_for_repair_receiver, receiver) =
            create_keypairs_and_bls_sig_verifier();

        let vote_rank1 = 2;
        let cert_ranks = [0, 2, 3, 4, 5, 7, 8, 9];
        let cert_type = CertificateType::Finalize(4);
        let vote_message1 = create_signed_vote_message(
            &validator_keypairs,
            Vote::new_finalization_vote(5),
            vote_rank1,
        );
        let cert = create_signed_certificate_message(&validator_keypairs, cert_type, &cert_ranks);
        let messages1 = vec![
            ConsensusMessage::Vote(vote_message1),
            ConsensusMessage::Certificate(cert),
        ];

        assert!(verifier
            .verify_and_send_batches(messages_to_batches(&messages1))
            .is_ok());
        assert_eq!(receiver.try_iter().count(), 2);
        assert_eq!(
            verifier
                .stats
                .verify_votes_consensus_sent
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            verifier
                .stats
                .verify_certs_consensus_sent
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 2);
        let received_verified_votes1 = votes_for_repair_receiver.try_recv().unwrap();
        assert_eq!(
            received_verified_votes1,
            (
                validator_keypairs[vote_rank1].vote_keypair.pubkey(),
                vec![5]
            )
        );

        let vote_rank2 = 3;
        let vote_message2 = create_signed_vote_message(
            &validator_keypairs,
            Vote::new_notarization_vote(6, Hash::new_unique()),
            vote_rank2,
        );
        let messages2 = vec![ConsensusMessage::Vote(vote_message2)];
        assert!(verifier
            .verify_and_send_batches(messages_to_batches(&messages2))
            .is_ok());

        assert_eq!(receiver.try_iter().count(), 1);
        assert_eq!(
            verifier
                .stats
                .verify_votes_consensus_sent
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            verifier
                .stats
                .verify_certs_consensus_sent
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 3); // 2 + 1 = 3
        let received_verified_votes2 = votes_for_repair_receiver.try_recv().unwrap();
        assert_eq!(
            received_verified_votes2,
            (
                validator_keypairs[vote_rank2].vote_keypair.pubkey(),
                vec![6]
            )
        );

        verifier.stats.last_stats_logged = Instant::now() - STATS_INTERVAL_DURATION;
        let vote_rank3 = 9;
        let vote_message3 = create_signed_vote_message(
            &validator_keypairs,
            Vote::new_notarization_fallback_vote(7, Hash::new_unique()),
            vote_rank3,
        );
        let messages3 = vec![ConsensusMessage::Vote(vote_message3)];
        assert!(verifier
            .verify_and_send_batches(messages_to_batches(&messages3))
            .is_ok());
        assert_eq!(receiver.try_iter().count(), 1);
        assert_eq!(
            verifier
                .stats
                .verify_votes_consensus_sent
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 0);
        let received_verified_votes3 = votes_for_repair_receiver.try_recv().unwrap();
        assert_eq!(
            received_verified_votes3,
            (
                validator_keypairs[vote_rank3].vote_keypair.pubkey(),
                vec![7]
            )
        );
    }

    #[test]
    fn test_blssigverifier_verify_malformed() {
        let (validator_keypairs, mut verifier, _, receiver) =
            create_keypairs_and_bls_sig_verifier();

        let packets = vec![Packet::default()];
        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());

        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 1);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty(), "Malformed packet should not be sent");

        // Send a packet with no epoch stakes
        let vote_message_no_stakes = create_signed_vote_message(
            &validator_keypairs,
            Vote::new_finalization_vote(5_000_000_000), // very high slot
            0,
        );
        let messages_no_stakes = vec![ConsensusMessage::Vote(vote_message_no_stakes)];

        assert!(verifier
            .verify_and_send_batches(messages_to_batches(&messages_no_stakes))
            .is_ok());

        assert_eq!(
            verifier
                .stats
                .received_no_epoch_stakes
                .load(Ordering::Relaxed),
            1
        );

        // Expect no messages since the packet was malformed
        assert!(
            receiver.is_empty(),
            "Packet with no epoch stakes should not be sent"
        );

        // Send a packet with invalid rank
        let messages_invalid_rank = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 1000, // Invalid rank
        })];
        assert!(verifier
            .verify_and_send_batches(messages_to_batches(&messages_invalid_rank))
            .is_ok());
        assert_eq!(verifier.stats.received_bad_rank.load(Ordering::Relaxed), 1);

        // Expect no messages since the packet was malformed
        assert!(
            receiver.is_empty(),
            "Packet with invalid rank should not be sent"
        );
    }

    #[test]
    fn test_blssigverifier_send_packets_channel_full() {
        agave_logger::setup();
        let (votes_for_repair_sender, _votes_for_repair_receiver) = crossbeam_channel::unbounded();
        let (message_sender, message_receiver) = crossbeam_channel::bounded(1);
        let (consensus_metrics_sender, _consensus_metrics_receiver) =
            crossbeam_channel::unbounded();
        let (reward_votes_sender, _reward_votes_receiver) = crossbeam_channel::unbounded();
        let (validator_keypairs, mut verifier) = create_keypairs_and_bls_sig_verifier_with_channels(
            votes_for_repair_sender,
            message_sender,
            consensus_metrics_sender,
            reward_votes_sender,
        );

        let msg1 = ConsensusMessage::Vote(create_signed_vote_message(
            &validator_keypairs,
            Vote::new_finalization_vote(5),
            0,
        ));
        let msg2 = ConsensusMessage::Vote(create_signed_vote_message(
            &validator_keypairs,
            Vote::new_notarization_fallback_vote(6, Hash::new_unique()),
            2,
        ));
        let messages = vec![msg1.clone(), msg2];
        assert!(verifier
            .verify_and_send_batches(messages_to_batches(&messages))
            .is_ok());

        // We failed to send the second message because the channel is full.
        assert_eq!(message_receiver.len(), 1);
        assert_eq!(message_receiver.recv().unwrap(), msg1);
        assert_eq!(
            verifier
                .stats
                .verify_votes_consensus_sent
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            verifier
                .stats
                .verify_votes_consensus_channel_full
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_blssigverifier_send_packets_receiver_closed() {
        let (validator_keypairs, mut verifier, _, receiver) =
            create_keypairs_and_bls_sig_verifier();

        // Close the receiver to simulate a disconnected channel.
        drop(receiver);

        let msg = ConsensusMessage::Vote(create_signed_vote_message(
            &validator_keypairs,
            Vote::new_finalization_vote(5),
            0,
        ));
        let messages = vec![msg];
        let result = verifier.verify_and_send_batches(messages_to_batches(&messages));
        assert!(result.is_err());
    }

    #[test]
    fn test_blssigverifier_send_discarded_packets() {
        let (validator_keypairs, mut verifier, _, receiver) =
            create_keypairs_and_bls_sig_verifier();

        let message = ConsensusMessage::Vote(create_signed_vote_message(
            &validator_keypairs,
            Vote::new_finalization_vote(5),
            0,
        ));
        let mut packet = Packet::default();
        packet
            .populate_packet(None, &message)
            .expect("Failed to populate packet");
        packet.meta_mut().set_discard(true); // Manually discard

        let packets = vec![packet];
        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];

        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert!(receiver.is_empty(), "Discarded packet should not be sent");
        assert_eq!(
            verifier
                .stats
                .verify_votes_consensus_sent
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received_discarded.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received_votes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_blssigverifier_verify_votes_all_valid() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_votes = 5;
        let mut packets = Vec::with_capacity(num_votes);
        let vote = Vote::new_skip_vote(42);
        let vote_payload = bincode::serialize(&vote).expect("Failed to serialize vote");

        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(num_votes) {
            let rank = i as u16;
            let bls_keypair = &validator_keypair.bls_keypair;
            let signature: BLSSignature = bls_keypair.sign(&vote_payload).into();
            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });
            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packets.push(packet);
        }

        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            num_votes,
            "Did not send all valid packets"
        );
    }

    #[test]
    fn test_blssigverifier_verify_votes_two_distinct_messages() {
        let (validator_keypairs, mut verifier, _repair_channel, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_votes_group1 = 3;
        let num_votes_group2 = 4;
        let num_votes = num_votes_group1 + num_votes_group2;
        let mut packets = Vec::with_capacity(num_votes);

        let vote1 = Vote::new_skip_vote(42);
        let _vote1_payload = bincode::serialize(&vote1).expect("Failed to serialize vote");
        let vote2 = Vote::new_notarization_vote(43, Hash::new_unique());
        let _vote2_payload = bincode::serialize(&vote2).expect("Failed to serialize vote");

        // Group 1 votes
        for (i, _) in validator_keypairs.iter().enumerate().take(num_votes_group1) {
            let msg =
                ConsensusMessage::Vote(create_signed_vote_message(&validator_keypairs, vote1, i));
            let mut p = Packet::default();
            p.populate_packet(None, &msg).unwrap();
            packets.push(p);
        }

        // Group 2 votes
        for (i, _) in validator_keypairs
            .iter()
            .enumerate()
            .skip(num_votes_group1)
            .take(num_votes_group2)
        {
            let msg =
                ConsensusMessage::Vote(create_signed_vote_message(&validator_keypairs, vote2, i));
            let mut p = Packet::default();
            p.populate_packet(None, &msg).unwrap();
            packets.push(p);
        }

        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        verifier.verify_and_send_batches(packet_batches).unwrap();
        assert_eq!(
            message_receiver.try_iter().count(),
            num_votes,
            "Did not send all valid packets"
        );
        assert_eq!(
            verifier
                .stats
                .votes_batch_distinct_messages_count
                .load(Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn test_blssigverifier_verify_votes_invalid_in_two_distinct_messages() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_votes = 5;
        let invalid_rank = 3; // This voter will sign vote 2 with an invalid signature.
        let mut packets = Vec::with_capacity(num_votes);

        let vote1 = Vote::new_skip_vote(42);
        let vote1_payload = bincode::serialize(&vote1).expect("Failed to serialize vote");
        let vote2 = Vote::new_skip_vote(43);
        let vote2_payload = bincode::serialize(&vote2).expect("Failed to serialize vote");
        let invalid_payload =
            bincode::serialize(&Vote::new_skip_vote(99)).expect("Failed to serialize vote");

        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(num_votes) {
            let rank = i as u16;
            let bls_keypair = &validator_keypair.bls_keypair;

            // Split the votes: Ranks 0, 1 sign vote 1. Ranks 2, 3, 4 sign vote 2.
            let (vote, payload) = if i < 2 {
                (vote1, &vote1_payload)
            } else {
                (vote2, &vote2_payload)
            };

            let signature = if rank == invalid_rank {
                bls_keypair.sign(&invalid_payload).into() // Invalid signature
            } else {
                bls_keypair.sign(payload).into()
            };

            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });
            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packets.push(packet);
        }

        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        let sent_messages: Vec<_> = message_receiver.try_iter().collect();
        assert_eq!(
            sent_messages.len(),
            num_votes - 1,
            "Only valid votes should be sent"
        );
        assert!(!sent_messages.iter().any(|msg| {
            if let ConsensusMessage::Vote(vm) = msg {
                vm.vote == vote2 && vm.rank == invalid_rank
            } else {
                false
            }
        }));
        assert_eq!(
            verifier
                .stats
                .received_bad_signature_votes
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_blssigverifier_verify_votes_one_invalid_signature() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_votes = 5;
        let invalid_rank = 2;
        let mut packets = Vec::with_capacity(num_votes);
        let mut consensus_messages = Vec::with_capacity(num_votes); // ADDED: To hold messages for later comparison.

        let vote = Vote::new_skip_vote(42);
        let valid_vote_payload = bincode::serialize(&vote).expect("Failed to serialize vote");
        let invalid_vote_payload =
            bincode::serialize(&Vote::new_skip_vote(99)).expect("Failed to serialize vote");

        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(num_votes) {
            let rank = i as u16;
            let bls_keypair = &validator_keypair.bls_keypair;

            let signature = if rank == invalid_rank {
                bls_keypair.sign(&invalid_vote_payload).into() // Invalid signature
            } else {
                bls_keypair.sign(&valid_vote_payload).into() // Valid signature
            };

            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });

            consensus_messages.push(consensus_message.clone());

            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packets.push(packet);
        }

        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        let sent_messages: Vec<_> = message_receiver.try_iter().collect();
        assert_eq!(
            sent_messages.len(),
            num_votes - 1,
            "Only valid votes should be sent"
        );

        // Ensure the message with the invalid rank is not in the sent messages.
        assert!(!sent_messages.iter().any(|msg| {
            if let ConsensusMessage::Vote(vm) = msg {
                vm.rank == invalid_rank
            } else {
                false
            }
        }));

        assert_eq!(
            verifier
                .stats
                .received_bad_signature_votes
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_blssigverifier_verify_votes_empty_batch() {
        let (_, mut verifier, _, _) = create_keypairs_and_bls_sig_verifier();

        let packet_batches = vec![];
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_verify_certificate_base2_valid() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_signers = 7; // > 2/3 of 10 validators
        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        let cert = create_signed_certificate_message(
            &validator_keypairs,
            cert_type,
            &(0..num_signers).collect::<Vec<_>>(),
        );
        let consensus_message = ConsensusMessage::Certificate(cert);
        let packet_batches = messages_to_batches(&[consensus_message]);

        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            1,
            "Valid Base2 certificate should be sent"
        );
    }

    #[test]
    fn test_verify_certificate_base2_just_enough_stake() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_signers = 6; // = 60% of 10 validators
        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        let cert = create_signed_certificate_message(
            &validator_keypairs,
            cert_type,
            &(0..num_signers).collect::<Vec<_>>(),
        );
        let consensus_message = ConsensusMessage::Certificate(cert);
        let packet_batches = messages_to_batches(&[consensus_message]);

        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            1,
            "Valid Base2 certificate should be sent"
        );
    }

    #[test]
    fn test_verify_certificate_base2_not_enough_stake() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_signers = 5; // < 60% of 10 validators
        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        let cert = create_signed_certificate_message(
            &validator_keypairs,
            cert_type,
            &(0..num_signers).collect::<Vec<_>>(),
        );
        let consensus_message = ConsensusMessage::Certificate(cert);
        let packet_batches = messages_to_batches(&[consensus_message]);

        // The call still succeeds, but the packet is marked for discard.
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            0,
            "This certificate should be invalid"
        );
        assert_eq!(
            verifier
                .stats
                .received_not_enough_stake
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_verify_certificate_base3_valid() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let slot = 20;
        let block_hash = Hash::new_unique();
        let notarize_vote = Vote::new_notarization_vote(slot, block_hash);
        let notarize_fallback_vote = Vote::new_notarization_fallback_vote(slot, block_hash);
        let mut all_vote_messages = Vec::new();
        (0..4).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(
                &validator_keypairs,
                notarize_vote,
                i,
            ))
        });
        (4..7).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(
                &validator_keypairs,
                notarize_fallback_vote,
                i,
            ))
        });
        let cert_type = CertificateType::NotarizeFallback(slot, block_hash);
        let mut builder = CertificateBuilder::new(cert_type);
        builder
            .aggregate(&all_vote_messages)
            .expect("Failed to aggregate votes");
        let cert = builder.build().expect("Failed to build certificate");
        let consensus_message = ConsensusMessage::Certificate(cert);
        let packet_batches = messages_to_batches(&[consensus_message]);

        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            1,
            "Valid Base3 certificate should be sent"
        );
    }

    #[test]
    fn test_verify_certificate_base3_just_enough_stake() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let slot = 20;
        let block_hash = Hash::new_unique();
        let notarize_vote = Vote::new_notarization_vote(slot, block_hash);
        let notarize_fallback_vote = Vote::new_notarization_fallback_vote(slot, block_hash);
        let mut all_vote_messages = Vec::new();
        (0..4).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(
                &validator_keypairs,
                notarize_vote,
                i,
            ))
        });
        (4..6).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(
                &validator_keypairs,
                notarize_fallback_vote,
                i,
            ))
        });
        let cert_type = CertificateType::NotarizeFallback(slot, block_hash);
        let mut builder = CertificateBuilder::new(cert_type);
        builder
            .aggregate(&all_vote_messages)
            .expect("Failed to aggregate votes");
        let cert = builder.build().expect("Failed to build certificate");
        let consensus_message = ConsensusMessage::Certificate(cert);
        let packet_batches = messages_to_batches(&[consensus_message]);

        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            1,
            "Valid Base3 certificate should be sent"
        );
    }

    #[test]
    fn test_verify_certificate_base3_not_enough_stake() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let slot = 20;
        let block_hash = Hash::new_unique();
        let notarize_vote = Vote::new_notarization_vote(slot, block_hash);
        let notarize_fallback_vote = Vote::new_notarization_fallback_vote(slot, block_hash);
        let mut all_vote_messages = Vec::new();
        (0..4).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(
                &validator_keypairs,
                notarize_vote,
                i,
            ))
        });
        (4..5).for_each(|i| {
            all_vote_messages.push(create_signed_vote_message(
                &validator_keypairs,
                notarize_fallback_vote,
                i,
            ))
        });
        let cert_type = CertificateType::NotarizeFallback(slot, block_hash);
        let mut builder = CertificateBuilder::new(cert_type);
        builder
            .aggregate(&all_vote_messages)
            .expect("Failed to aggregate votes");
        let cert = builder.build().expect("Failed to build certificate");
        let consensus_message = ConsensusMessage::Certificate(cert);
        let packet_batches = messages_to_batches(&[consensus_message]);

        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            0,
            "This certificate should be invalid"
        );
        assert_eq!(
            verifier
                .stats
                .received_not_enough_stake
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_verify_certificate_invalid_signature() {
        let (_validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_signers = 7;
        let slot = 10;
        let block_hash = Hash::new_unique();
        let cert_type = CertificateType::Notarize(slot, block_hash);
        let mut bitmap = BitVec::<u8, Lsb0>::new();
        bitmap.resize(num_signers, false);
        for i in 0..num_signers {
            bitmap.set(i, true);
        }
        let encoded_bitmap = encode_base2(&bitmap).unwrap();

        let cert = Certificate {
            cert_type,
            signature: BLSSignature::default(), // Use a default/wrong signature
            bitmap: encoded_bitmap,
        };
        let consensus_message = ConsensusMessage::Certificate(cert);
        let packet_batches = messages_to_batches(&[consensus_message]);

        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert!(
            message_receiver.is_empty(),
            "Certificate with invalid signature should be discarded"
        );
        assert_eq!(
            verifier
                .stats
                .received_bad_signature_certs
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_verify_mixed_valid_batch() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let mut packets = Vec::new();
        let num_votes = 2;

        let vote = Vote::new_skip_vote(42);
        let vote_payload = bincode::serialize(&vote).unwrap();
        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(num_votes) {
            let rank = i as u16;
            let bls_keypair = &validator_keypair.bls_keypair;
            let signature: BLSSignature = bls_keypair.sign(&vote_payload).into();
            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });
            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packets.push(packet);
        }

        let num_cert_signers = 7;
        let cert_type = CertificateType::Notarize(10, Hash::new_unique());
        let cert_original_vote = Vote::new_notarization_vote(10, cert_type.to_block().unwrap().1);
        let cert_payload = bincode::serialize(&cert_original_vote).unwrap();

        let cert_vote_messages: Vec<VoteMessage> = (0..num_cert_signers)
            .map(|i| {
                let signature = validator_keypairs[i].bls_keypair.sign(&cert_payload);
                VoteMessage {
                    vote: cert_original_vote,
                    signature: signature.into(),
                    rank: i as u16,
                }
            })
            .collect();
        let mut builder = CertificateBuilder::new(cert_type);
        builder
            .aggregate(&cert_vote_messages)
            .expect("Failed to aggregate votes for certificate");
        let cert = builder.build().expect("Failed to build certificate");
        let consensus_message_cert = ConsensusMessage::Certificate(cert);
        let mut cert_packet = Packet::default();
        cert_packet
            .populate_packet(None, &consensus_message_cert)
            .unwrap();
        packets.push(cert_packet);

        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert_eq!(
            message_receiver.try_iter().count(),
            num_votes + 1,
            "All valid messages in a mixed batch should be sent"
        );
        assert_eq!(
            verifier
                .stats
                .verify_votes_consensus_sent
                .load(Ordering::Relaxed),
            num_votes as u64
        );
        assert_eq!(
            verifier
                .stats
                .verify_certs_consensus_sent
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_verify_vote_with_invalid_rank() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let invalid_rank = 999;
        let vote = Vote::new_skip_vote(42);
        let vote_payload = bincode::serialize(&vote).unwrap();
        let bls_keypair = &validator_keypairs[0].bls_keypair;
        let signature: BLSSignature = bls_keypair.sign(&vote_payload).into();

        let consensus_message = ConsensusMessage::Vote(VoteMessage {
            vote,
            signature,
            rank: invalid_rank,
        });

        let packet_batches = messages_to_batches(&[consensus_message]);
        assert!(verifier.verify_and_send_batches(packet_batches).is_ok());
        assert!(
            message_receiver.is_empty(),
            "Packet with invalid rank should be discarded"
        );
        assert_eq!(verifier.stats.received_bad_rank.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_verify_old_vote_and_cert() {
        let (message_sender, message_receiver) = crossbeam_channel::unbounded();
        let (votes_for_repair_sender, _) = crossbeam_channel::unbounded();
        let (consensus_metrics_sender, _) = crossbeam_channel::unbounded();
        let (reward_votes_sender, _reward_votes_receiver) = crossbeam_channel::unbounded();
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let stakes_vec = (0..validator_keypairs.len())
            .map(|i| 1_000 - i as u64)
            .collect::<Vec<_>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            stakes_vec,
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        let bank5 = Bank::new_from_parent(Arc::new(bank0), &Pubkey::default(), 5);
        let bank_forks = BankForks::new_rw_arc(bank5);

        bank_forks.write().unwrap().set_root(5, None, None).unwrap();

        let sharable_banks = bank_forks.read().unwrap().sharable_banks();
        let keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = Arc::new(ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        ));
        let leader_schedule = Arc::new(LeaderScheduleCache::new_from_bank(&sharable_banks.root()));
        let mut sig_verifier = BLSSigVerifier::new(
            sharable_banks,
            votes_for_repair_sender,
            reward_votes_sender,
            message_sender,
            consensus_metrics_sender,
            Arc::new(AlpenglowLastVoted::default()),
            cluster_info,
            leader_schedule,
        );

        let vote = Vote::new_skip_vote(2);
        let vote_payload = bincode::serialize(&vote).unwrap();
        let bls_keypair = &validator_keypairs[0].bls_keypair;
        let signature: BLSSignature = bls_keypair.sign(&vote_payload).into();
        let consensus_message_vote = ConsensusMessage::Vote(VoteMessage {
            vote,
            signature,
            rank: 0,
        });
        let packet_batches_vote = messages_to_batches(&[consensus_message_vote]);

        assert!(sig_verifier
            .verify_and_send_batches(packet_batches_vote)
            .is_ok());
        assert!(
            message_receiver.is_empty(),
            "Old vote should not have been sent"
        );
        assert_eq!(sig_verifier.stats.received_old.load(Ordering::Relaxed), 1);

        let cert = create_signed_certificate_message(
            &validator_keypairs,
            CertificateType::Finalize(3),
            &[0], // Signer rank 0
        );
        let consensus_message_cert = ConsensusMessage::Certificate(cert);
        let packet_batches_cert = messages_to_batches(&[consensus_message_cert]);

        assert!(sig_verifier
            .verify_and_send_batches(packet_batches_cert)
            .is_ok());
        assert!(
            message_receiver.is_empty(),
            "Old certificate should not have been sent"
        );
        assert_eq!(sig_verifier.stats.received_old.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_verified_certs_are_skipped() {
        let (validator_keypairs, mut verifier, _, message_receiver) =
            create_keypairs_and_bls_sig_verifier();

        let num_signers = 8;
        let slot = 10;
        let block_hash = Hash::new_unique();
        let cert_type = CertificateType::Notarize(slot, block_hash);
        let original_vote = Vote::new_notarization_vote(slot, block_hash);
        let signed_payload = bincode::serialize(&original_vote).unwrap();
        let mut vote_messages: Vec<VoteMessage> = (0..num_signers)
            .map(|i| {
                let signature = validator_keypairs[i].bls_keypair.sign(&signed_payload);
                VoteMessage {
                    vote: original_vote,
                    signature: signature.into(),
                    rank: i as u16,
                }
            })
            .collect();

        let mut builder1 = CertificateBuilder::new(cert_type);
        builder1
            .aggregate(&vote_messages)
            .expect("Failed to aggregate votes");
        let cert1 = builder1.build().expect("Failed to build certificate");
        let consensus_message1 = ConsensusMessage::Certificate(cert1);
        let packet_batches1 = messages_to_batches(&[consensus_message1]);

        assert!(verifier.verify_and_send_batches(packet_batches1).is_ok());

        assert_eq!(
            message_receiver.try_iter().count(),
            1,
            "First certificate should be sent"
        );
        assert_eq!(verifier.stats.received_verified.load(Ordering::Relaxed), 0);

        vote_messages.pop(); // Remove one signature
        let mut builder2 = CertificateBuilder::new(cert_type);
        builder2
            .aggregate(&vote_messages)
            .expect("Failed to aggregate votes");
        let cert2 = builder2.build().expect("Failed to build certificate");
        let consensus_message2 = ConsensusMessage::Certificate(cert2);
        let packet_batches2 = messages_to_batches(&[consensus_message2]);

        assert!(verifier.verify_and_send_batches(packet_batches2).is_ok());
        assert!(
            message_receiver.is_empty(),
            "Second, weaker certificate should not be sent"
        );
        assert_eq!(
            verifier.stats.received.load(Ordering::Relaxed),
            2,
            "Should have received two packets in total"
        );
        assert_eq!(
            verifier.stats.received_verified.load(Ordering::Relaxed),
            1,
            "Should have detected one already-verified cert"
        );
    }

    fn messages_to_batches(messages: &[ConsensusMessage]) -> Vec<PacketBatch> {
        let packets: Vec<_> = messages
            .iter()
            .map(|msg| {
                let mut p = Packet::default();
                p.populate_packet(None, msg).unwrap();
                p
            })
            .collect();
        vec![PinnedPacketBatch::new(packets).into()]
    }
}
