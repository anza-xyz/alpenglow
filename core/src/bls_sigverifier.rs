use {
    crate::cluster_info_vote_listener::VerifiedVoterSlotsSender,
    agave_votor::consensus_rewards,
    agave_votor_messages::{
        consensus_message::{Certificate, ConsensusMessage, VoteMessage},
        migration::MigrationStatus,
        reward_certificate::AddVoteMessage,
    },
    crossbeam_channel::{Receiver, Sender, TrySendError},
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_perf::packet::PacketBatch,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::SharableBanks},
    solana_streamer::streamer,
    std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder},
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)]
enum BLSSigVerifierError {
    #[error("channel to consensus pool disconnected")]
    ConsensusPoolChannelDisconnected,
    #[error("channel to rewards container disconnected")]
    RewardsChannelDisconnected,
    #[error("channel to repair disconnected")]
    RepairChannelDisconnected,
}

pub fn spawn_service(
    exit: Arc<AtomicBool>,
    packet_receiver: Receiver<PacketBatch>,
    banks: SharableBanks,
    vote_sender: VerifiedVoterSlotsSender,
    reward_votes_sender: Sender<AddVoteMessage>,
    message_sender: Sender<Vec<ConsensusMessage>>,
    cluster_info: Arc<ClusterInfo>,
    leader_schedule: Arc<LeaderScheduleCache>,
    migration_status: Arc<MigrationStatus>,
) -> thread::JoinHandle<()> {
    let verifier = BLSSigVerifier::new(
        banks,
        vote_sender,
        reward_votes_sender,
        message_sender,
        cluster_info,
        leader_schedule,
        migration_status,
    );

    Builder::new()
        .name("solSigVerBLS".to_string())
        .spawn(move || verifier.run(exit, packet_receiver))
        .unwrap()
}

pub struct BLSSigVerifier {
    banks: SharableBanks,
    /// Sender to repair service
    vote_sender: VerifiedVoterSlotsSender,
    /// Sender to consensus rewards service
    reward_votes_sender: Sender<AddVoteMessage>,
    /// Sender to votor
    message_sender: Sender<Vec<ConsensusMessage>>,
    cluster_info: Arc<ClusterInfo>,
    leader_schedule: Arc<LeaderScheduleCache>,
    migration_status: Arc<MigrationStatus>,
    // Reused buffers to avoid reallocating per packet batch.
    vote_buffer: Vec<VoteMessage>,
    cert_buffer: Vec<Certificate>,
}

impl BLSSigVerifier {
    pub fn new(
        banks: SharableBanks,
        vote_sender: VerifiedVoterSlotsSender,
        reward_votes_sender: Sender<AddVoteMessage>,
        message_sender: Sender<Vec<ConsensusMessage>>,
        cluster_info: Arc<ClusterInfo>,
        leader_schedule: Arc<LeaderScheduleCache>,
        migration_status: Arc<MigrationStatus>,
    ) -> Self {
        Self {
            banks,
            vote_sender,
            reward_votes_sender,
            message_sender,
            cluster_info,
            leader_schedule,
            migration_status,
            vote_buffer: Vec::new(),
            cert_buffer: Vec::new(),
        }
    }

    fn run(mut self, exit: Arc<AtomicBool>, receiver: Receiver<PacketBatch>) {
        info!("BLSSigverifier starting");
        while !exit.load(Ordering::Relaxed) {
            const SOFT_RECEIVE_CAP: usize = 5_000;
            match streamer::recv_packet_batches(&receiver, SOFT_RECEIVE_CAP) {
                Ok((batches, _, _)) => {
                    if let Err(err) = self.process_batches(batches) {
                        warn!("BLSSigVerifier exiting after channel disconnect: {err}");
                        break;
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
        }
        info!("BLSSigverifier shutting down");
    }

    /// Extract votes and certs from the packet batch, verify and send the results
    fn process_batches(&mut self, batches: Vec<PacketBatch>) -> Result<(), BLSSigVerifierError> {
        if self.migration_status.is_pre_feature_activation() {
            // We only need to start processing messages once the feature flag is active
            return Ok(());
        }

        let root = self.banks.root();
        self.vote_buffer.clear();
        self.cert_buffer.clear();
        Self::extract_messages(
            batches,
            &root,
            &mut self.vote_buffer,
            &mut self.cert_buffer,
        );

        let verified_votes = Self::verify_votes(self.vote_buffer.drain(..), &root);
        let verified_certs = Self::verify_certificates(self.cert_buffer.drain(..), &root);
        let reward_votes = self.collect_reward_votes(&verified_votes, root.slot());

        self.send_results(verified_votes, verified_certs)?;
        self.send_reward_votes(reward_votes)?;

        Ok(())
    }

    fn extract_messages(
        batches: Vec<PacketBatch>,
        _root: &Bank,
        votes: &mut Vec<VoteMessage>,
        certs: &mut Vec<Certificate>,
    ) {
        for batch in batches {
            for packet in batch.into_iter() {
                Self::extract_packet(packet, votes, certs);
            }
        }
    }

    /// Retrieve the bls message from the packet, performing any filtering checks as needed
    fn extract_packet(
        packet: solana_perf::packet::PacketRef,
        votes: &mut Vec<VoteMessage>,
        certs: &mut Vec<Certificate>,
    ) {
        if packet.meta().discard() {
            return;
        }

        let Ok(msg) = packet.deserialize_slice::<ConsensusMessage, _>(..) else {
            return;
        };

        match msg {
            ConsensusMessage::Vote(vote) => votes.push(vote),
            ConsensusMessage::Certificate(cert) => certs.push(cert),
        }
    }

    fn verify_votes(votes: impl IntoIterator<Item = VoteMessage>, bank: &Bank) -> Vec<(VoteMessage, Pubkey)> {
        // Actual BLS signature verification to follow
        votes
            .into_iter()
            .filter_map(|vote| {
                let entry = bank
                    .current_epoch_stakes()
                    .bls_pubkey_to_rank_map()
                    .get_pubkey_stake_entry(vote.rank as usize)?;
                Some((vote, entry.pubkey))
            })
            .collect()
    }

    fn verify_certificates(certs: impl IntoIterator<Item = Certificate>, _bank: &Bank) -> Vec<Certificate> {
        // Actual certificate verification to follow
        certs.into_iter().collect()
    }

    fn send_results(
        &self,
        votes: Vec<(VoteMessage, Pubkey)>,
        certs: Vec<Certificate>,
    ) -> Result<(), BLSSigVerifierError> {
        let mut votes_by_pubkey: HashMap<Pubkey, Vec<Slot>> = HashMap::new();

        // Send votes
        for (vote, pubkey) in &votes {
            votes_by_pubkey
                .entry(*pubkey)
                .or_default()
                .push(vote.vote.slot());
        }
        let votes = votes
            .into_iter()
            .map(|(v, _)| ConsensusMessage::Vote(v))
            .collect();
        self.send(votes)?;

        // Send certificates
        let certs = certs
            .into_iter()
            .map(ConsensusMessage::Certificate)
            .collect();
        self.send(certs)?;

        // Send votes to repair service
        for (pubkey, slots) in votes_by_pubkey {
            match self.vote_sender.try_send((pubkey, slots)) {
                Ok(()) | Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Disconnected(_)) => {
                    return Err(BLSSigVerifierError::RepairChannelDisconnected);
                }
            }
        }

        Ok(())
    }

    fn send(&self, msgs: Vec<ConsensusMessage>) -> Result<(), BLSSigVerifierError> {
        match self.message_sender.try_send(msgs) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Ok(()),
            Err(TrySendError::Disconnected(_)) => {
                Err(BLSSigVerifierError::ConsensusPoolChannelDisconnected)
            }
        }
    }

    fn collect_reward_votes(
        &self,
        votes: &[(VoteMessage, Pubkey)],
        root_slot: Slot,
    ) -> AddVoteMessage {
        let votes = votes
            .iter()
            .filter_map(|(vote, _)| {
                consensus_rewards::wants_vote(
                    &self.cluster_info,
                    &self.leader_schedule,
                    root_slot,
                    vote,
                )
                .then_some(*vote)
            })
            .collect();
        AddVoteMessage { votes }
    }

    fn send_reward_votes(&self, votes: AddVoteMessage) -> Result<(), BLSSigVerifierError> {
        match self.reward_votes_sender.try_send(votes) {
            Ok(()) | Err(TrySendError::Full(_)) => Ok(()),
            Err(TrySendError::Disconnected(_)) => {
                Err(BLSSigVerifierError::RewardsChannelDisconnected)
            }
        }
    }
}
