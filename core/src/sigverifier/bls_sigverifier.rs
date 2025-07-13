//! The BLS signature verifier.
//! This is just a placeholder for now, until we have a real implementation.

use {
    crate::{
        cluster_info_vote_listener::VerifiedVoteSender,
        sigverify_stage::{SigVerifier, SigVerifyServiceError},
    },
    alpenglow_vote::bls_message::BLSMessage,
    crossbeam_channel::{Sender, TrySendError},
    solana_pubkey::Pubkey,
    solana_runtime::{bank_forks::BankForks, epoch_stakes::EpochStakes},
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
    },
    solana_streamer::packet::PacketBatch,
    std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

const STATS_INTERVAL_DURATION: Duration = Duration::from_secs(1); // Log stats every second

// We are adding our own stats because we do BLS decoding in batch verification,
// and we send one BLS message at a time. So it makes sense to have finer-grained stats
#[derive(Debug)]
pub(crate) struct BLSSigVerifierStats {
    pub sent: u64,
    pub sent_failed: u64,
    pub verified_votes_sent: u64,
    pub verified_votes_sent_failed: u64,
    pub received: u64,
    pub received_malformed: u64,
    pub received_no_epoch_stakes: u64,
    pub received_votes: u64,
    pub received_votes_epoch_aggregated: i64,
    pub last_stats_logged: Instant,
}

impl BLSSigVerifierStats {
    pub fn new() -> Self {
        Self {
            sent: 0,
            sent_failed: 0,
            verified_votes_sent: 0,
            verified_votes_sent_failed: 0,
            received: 0,
            received_malformed: 0,
            received_no_epoch_stakes: 0,
            received_votes: 0,
            received_votes_epoch_aggregated: 0,
            last_stats_logged: Instant::now(),
        }
    }

    pub fn accumulate(&mut self, other: BLSSigVerifierStats) {
        self.sent += other.sent;
        self.sent_failed += other.sent_failed;
        self.verified_votes_sent += other.verified_votes_sent;
        self.verified_votes_sent_failed += other.verified_votes_sent_failed;
        self.received += other.received;
        self.received_malformed += other.received_malformed;
        self.received_no_epoch_stakes += other.received_no_epoch_stakes;
        self.received_votes += other.received_votes;
        self.received_votes_epoch_aggregated += other.received_votes_epoch_aggregated;
    }
}

pub struct BLSSigVerifier {
    bank_forks: Arc<RwLock<BankForks>>,
    verified_votes_sender: VerifiedVoteSender,
    message_sender: Sender<BLSMessage>,
    stats: BLSSigVerifierStats,
    root_epoch: Epoch,
    epoch_schedule: EpochSchedule,
    epoch_stakes_map: Arc<HashMap<Epoch, EpochStakes>>,
}

impl SigVerifier for BLSSigVerifier {
    type SendType = BLSMessage;
    // TODO(wen): just a placeholder without any verification.
    fn verify_batches(&self, batches: Vec<PacketBatch>, _valid_packets: usize) -> Vec<PacketBatch> {
        batches
    }

    fn send_packets(
        &mut self,
        packet_batches: Vec<PacketBatch>,
    ) -> Result<(), SigVerifyServiceError<Self::SendType>> {
        // TODO(wen): just a placeholder without any batching.
        let mut stats = BLSSigVerifierStats::new();
        let mut verified_votes: HashMap<Pubkey, Vec<Slot>> = HashMap::new();

        for packet in packet_batches.iter().flatten() {
            stats.received += 1;

            let message = match packet.deserialize_slice::<BLSMessage, _>(..) {
                Ok(msg) => msg,
                Err(e) => {
                    trace!("Failed to deserialize BLS message: {}", e);
                    stats.received_malformed += 1;
                    continue;
                }
            };

            let slot = match &message {
                BLSMessage::Vote(vote_message) => vote_message.vote.slot(),
                BLSMessage::Certificate(certificate_message) => {
                    certificate_message.certificate.slot
                }
            };
            let epoch = self.epoch_schedule.get_epoch(slot);
            let rank_to_pubkey_map = if let Some(epoch_stakes) = self.epoch_stakes_map.get(&epoch) {
                epoch_stakes.bls_pubkey_to_rank_map()
            } else {
                stats.received_no_epoch_stakes += 1;
                continue;
            };

            if let BLSMessage::Vote(vote_message) = &message {
                let vote = &vote_message.vote;
                stats.received_votes += 1;
                stats.received_votes_epoch_aggregated += (epoch - self.root_epoch) as i64;
                if vote.is_notarization_or_finalization() || vote.is_notarize_fallback() {
                    let Some((pubkey, _)) = rank_to_pubkey_map.get_pubkey(vote_message.rank.into())
                    else {
                        self.stats.received_malformed += 1;
                        continue;
                    };
                    let cur_slots: &mut Vec<Slot> = verified_votes.entry(*pubkey).or_default();
                    if !cur_slots.contains(&slot) {
                        cur_slots.push(slot);
                    }
                }
            }

            // Now send the BLS message to certificate pool.
            match self.message_sender.try_send(message) {
                Ok(()) => stats.sent += 1,
                Err(TrySendError::Full(_)) => {
                    stats.sent_failed += 1;
                }
                Err(e @ TrySendError::Disconnected(_)) => {
                    return Err(e.into());
                }
            }
        }
        self.send_verified_votes(verified_votes);
        self.stats.accumulate(stats);
        // We don't need lock on stats for now because stats are read and written in a single thread.
        self.maybe_report_stats();
        Ok(())
    }
}

impl BLSSigVerifier {
    fn maybe_report_stats(&mut self) {
        let now = Instant::now();
        let time_since_last_log = now.duration_since(self.stats.last_stats_logged);
        if time_since_last_log < STATS_INTERVAL_DURATION {
            return;
        }
        if self.stats.received_votes > 0
            && (self.stats.received_votes_epoch_aggregated as f64
                / self.stats.received_votes as f64)
                > 1.0
        {
            self.update_epoch_stakes_map();
        }
        datapoint_info!(
            "bls_sig_verifier_stats",
            ("sent", self.stats.sent as i64, i64),
            ("sent_failed", self.stats.sent_failed as i64, i64),
            (
                "verified_votes_sent",
                self.stats.verified_votes_sent as i64,
                i64
            ),
            (
                "verified_votes_sent_failed",
                self.stats.verified_votes_sent_failed,
                u64
            ),
            ("received", self.stats.received, u64),
            ("received_malformed", self.stats.received_malformed, u64),
        );
        self.stats = BLSSigVerifierStats::new();
    }

    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        verified_votes_sender: VerifiedVoteSender,
        message_sender: Sender<BLSMessage>,
    ) -> Self {
        let mut verifier = Self {
            bank_forks,
            verified_votes_sender,
            message_sender,
            stats: BLSSigVerifierStats::new(),
            epoch_schedule: EpochSchedule::default(),
            epoch_stakes_map: Arc::new(HashMap::new()),
            root_epoch: Epoch::default(),
        };
        verifier.update_epoch_stakes_map();
        verifier
    }

    fn update_epoch_stakes_map(&mut self) {
        let root_bank = self.bank_forks.read().unwrap().root_bank();
        let epoch = root_bank.epoch();
        if self.epoch_stakes_map.is_empty() || epoch > self.root_epoch {
            self.epoch_stakes_map = Arc::new(root_bank.epoch_stakes_map().clone());
            self.root_epoch = epoch;
            self.epoch_schedule = root_bank.epoch_schedule().clone();
        }
    }

    fn send_verified_votes(&mut self, verified_votes: HashMap<Pubkey, Vec<Slot>>) {
        for (pubkey, slots) in verified_votes {
            match self.verified_votes_sender.try_send((pubkey, slots)) {
                Ok(()) => {
                    self.stats.verified_votes_sent += 1;
                }
                Err(e) => {
                    trace!("Failed to send verified vote: {}", e);
                    self.stats.verified_votes_sent_failed += 1;
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> &BLSSigVerifierStats {
        &self.stats
    }

    #[cfg(test)]
    pub(crate) fn set_last_stats_logged(&mut self, last_stats_logged: Instant) {
        self.stats.last_stats_logged = last_stats_logged;
    }
}

// Add tests for the BLS signature verifier
#[cfg(test)]
mod tests {
    use {
        super::*,
        alpenglow_vote::{
            bls_message::{BLSMessage, CertificateMessage, VoteMessage},
            certificate::{Certificate, CertificateType},
            vote::Vote,
        },
        bitvec::prelude::*,
        crossbeam_channel::Receiver,
        solana_bls_signatures::Signature,
        solana_perf::packet::Packet,
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts_no_program,
                ValidatorVoteKeypairs,
            },
        },
        solana_sdk::{hash::Hash, signer::Signer},
        std::time::Duration,
    };

    fn create_keypairs_and_bls_sig_verifier(
        verified_vote_sender: VerifiedVoteSender,
        message_sender: Sender<BLSMessage>,
    ) -> (Vec<ValidatorVoteKeypairs>, BLSSigVerifier) {
        // Create 10 node validatorvotekeypairs vec
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let stakes_vec = (0..validator_keypairs.len())
            .map(|i| 1_000 - i as u64)
            .collect::<Vec<_>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts_no_program(
            1_000_000_000,
            &validator_keypairs,
            stakes_vec,
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);
        (validator_keypairs, BLSSigVerifier::new(bank_forks, verified_vote_sender, message_sender))
    }

    fn test_bls_message_transmission(
        verifier: &mut BLSSigVerifier,
        receiver: Option<&Receiver<BLSMessage>>,
        messages: &[BLSMessage],
        expect_send_packets_ok: bool,
    ) {
        let packets = messages
            .iter()
            .map(|msg| {
                let mut packet = Packet::default();
                packet
                    .populate_packet(None, msg)
                    .expect("Failed to populate packet");
                packet
            })
            .collect::<Vec<Packet>>();
        let packet_batches = vec![PacketBatch::new(packets)];
        if expect_send_packets_ok {
            assert!(verifier.send_packets(packet_batches).is_ok());
            if let Some(receiver) = receiver {
                for msg in messages {
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(received_msg) => assert_eq!(received_msg, *msg),
                        Err(e) => warn!("Failed to receive BLS message: {}", e),
                    }
                }
            }
        } else {
            assert!(verifier.send_packets(packet_batches).is_err());
        }
    }

    #[test]
    fn test_blssigverifier_send_packets() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let (verified_vote_sender, verfied_vote_receiver) = crossbeam_channel::unbounded();
        // Create bank forks and epoch stakes

        let (validator_keypairs, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);

        let mut bitmap = BitVec::<u8, Lsb0>::repeat(false, 8);
        bitmap.set(3, true);
        bitmap.set(5, true);
        let vote_rank: usize = 2;
        let messages = vec![
            BLSMessage::Vote(VoteMessage {
                vote: Vote::new_finalization_vote(5),
                signature: Signature::default(),
                rank: vote_rank as u16,
            }),
            BLSMessage::Certificate(CertificateMessage {
                certificate: Certificate {
                    slot: 4,
                    certificate_type: CertificateType::Finalize,
                    block_id: None,
                    replayed_bank_hash: None,
                },
                signature: Signature::default(),
                bitmap,
            }),
        ];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        let stats = verifier.stats();
        assert_eq!(stats.sent, 2);
        assert_eq!(stats.received, 2);
        assert_eq!(stats.received_malformed, 0);
        let received_verified_votes = verfied_vote_receiver.try_recv().unwrap();
        assert_eq!(received_verified_votes, (validator_keypairs[vote_rank].vote_keypair.pubkey(), vec![5]));

        let vote_rank: usize = 3;
        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_notarization_vote(6, Hash::new_unique(), Hash::new_unique()),
            signature: Signature::default(),
            rank: vote_rank as u16,
        })];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        let stats = verifier.stats();
        assert_eq!(stats.sent, 3);
        assert_eq!(stats.received, 3);
        assert_eq!(stats.received_malformed, 0);
        let received_verified_votes = verfied_vote_receiver.try_recv().unwrap();
        assert_eq!(received_verified_votes, (validator_keypairs[vote_rank].vote_keypair.pubkey(), vec![6]));

        // Pretend 10 seconds have passed, make sure stats are reset
        verifier.set_last_stats_logged(Instant::now() - STATS_INTERVAL_DURATION);
        let vote_rank: usize = 9;
        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_notarization_fallback_vote(7, Hash::new_unique(), Hash::new_unique()),
            signature: Signature::default(),
            rank: vote_rank as u16,
        })];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        // Since we just logged all stats (including the packet just sent), stats should be reset
        let stats = verifier.stats();
        assert_eq!(stats.sent, 0);
        assert_eq!(stats.received, 0);
        assert_eq!(stats.received_malformed, 0);
        let received_verified_votes = verfied_vote_receiver.try_recv().unwrap();
        assert_eq!(received_verified_votes, (validator_keypairs[vote_rank].vote_keypair.pubkey(), vec![7]));
    }

    #[test]
    fn test_blssigverifier_send_packets_malformed() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);

        let packets = vec![Packet::default()];
        let packet_batches = vec![PacketBatch::new(packets)];
        assert!(verifier.send_packets(packet_batches).is_ok());
        let stats = verifier.stats();
        assert_eq!(stats.sent, 0);
        assert_eq!(stats.received, 1);
        assert_eq!(stats.received_malformed, 1);
        assert_eq!(stats.received_no_epoch_stakes, 0);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());

        // Send a packet with no epoch stakes
        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5_000_000_000),
            signature: Signature::default(),
            rank: 0,
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, true);
        let stats = verifier.stats();
        assert_eq!(stats.sent, 0);
        assert_eq!(stats.received, 2);
        assert_eq!(stats.received_malformed, 1);
        assert_eq!(stats.received_no_epoch_stakes, 1);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());

        // Send a packet with invalid rank
        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 1000, // Invalid rank
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, true);
        let stats = verifier.stats();
        assert_eq!(stats.sent, 0);
        assert_eq!(stats.received, 3);
        assert_eq!(stats.received_malformed, 2);
        assert_eq!(stats.received_no_epoch_stakes, 1);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());
    }

    #[test]
    fn test_blssigverifier_send_packets_channel_full() {
        solana_logger::setup();
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);
        let messages = vec![
            BLSMessage::Vote(VoteMessage {
                vote: Vote::new_finalization_vote(5),
                signature: Signature::default(),
                rank: 0,
            }),
            BLSMessage::Vote(VoteMessage {
                vote: Vote::new_notarization_fallback_vote(
                    6,
                    Hash::new_unique(),
                    Hash::new_unique(),
                ),
                signature: Signature::default(),
                rank: 2,
            }),
        ];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);

        // We failed to send the second message because the channel is full.
        let stats = verifier.stats();
        assert_eq!(stats.sent, 1);
        assert_eq!(stats.received, 2);
        assert_eq!(stats.received_malformed, 0);
    }

    #[test]
    fn test_blssigverifier_send_packets_receiver_closed() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);
        // Close the receiver, should get panic on next send
        drop(receiver);
        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 0,
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, false);
    }
}
