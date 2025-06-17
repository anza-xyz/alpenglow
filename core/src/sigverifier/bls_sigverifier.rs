//! The BLS signature verifier.
//! This is just a placeholder for now, until we have a real implementation.

use {
    crate::sigverify_stage::{SigVerifier, SigVerifyServiceError},
    alpenglow_vote::bls_message::BLSMessage,
    crossbeam_channel::{Sender, TrySendError},
    solana_streamer::packet::PacketBatch,
    std::time::{Duration, Instant},
};

const STATS_INTERVAL_DURATION: Duration = Duration::from_secs(1); // Log stats every second

// We are adding our own stats because we do BLS decoding in batch verification,
// and we send one BLS message at a time. So it makes sense to have finer-grained stats
#[derive(Debug)]
pub(crate) struct BLSSigVerifierStats {
    pub sent: u64,
    pub sent_failed: u64,
    pub received: u64,
    pub received_malformed: u64,
    pub last_stats_logged: Instant,
}

impl BLSSigVerifierStats {
    pub fn new() -> Self {
        Self {
            sent: 0,
            sent_failed: 0,
            received: 0,
            received_malformed: 0,
            last_stats_logged: Instant::now(),
        }
    }

    pub fn accumulate(&mut self, other: BLSSigVerifierStats) {
        self.sent += other.sent;
        self.sent_failed += other.sent_failed;
        self.received += other.received;
        self.received_malformed += other.received_malformed;
    }
}

pub struct BLSSigVerifier {
    sender: Sender<BLSMessage>,
    stats: BLSSigVerifierStats,
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

            match self.sender.try_send(message) {
                Ok(()) => stats.sent += 1,
                Err(TrySendError::Full(_)) => {
                    stats.sent_failed += 1;
                }
                Err(e @ TrySendError::Disconnected(_)) => {
                    return Err(e.into());
                }
            }
        }
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
        datapoint_info!(
            "bls_sig_verifier_stats",
            ("sent", self.stats.sent, u64),
            ("sent_failed", self.stats.sent_failed, u64),
            ("received", self.stats.received, u64),
            ("received_malformed", self.stats.received_malformed, u64),
        );
        self.stats = BLSSigVerifierStats::new();
    }

    pub fn new(sender: Sender<BLSMessage>) -> Self {
        Self {
            sender,
            stats: BLSSigVerifierStats::new(),
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
        solana_bls::Signature,
        solana_perf::packet::Packet,
        solana_sdk::hash::Hash,
        std::time::Duration,
    };

    fn test_bls_message_transmission(
        verifier: &mut BLSSigVerifier,
        receiver: Option<&Receiver<BLSMessage>>,
        messages: &[BLSMessage],
        expect_is_ok: bool,
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
        if expect_is_ok {
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
        let mut verifier = BLSSigVerifier::new(sender);

        let mut bitmap = BitVec::<u8, Lsb0>::repeat(false, 8);
        bitmap.set(3, true);
        bitmap.set(5, true);
        let messages = vec![
            BLSMessage::Vote(VoteMessage {
                vote: Vote::new_finalization_vote(5),
                signature: Signature::default(),
                rank: 0,
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

        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_notarization_vote(6, Hash::new_unique(), Hash::new_unique()),
            signature: Signature::default(),
            rank: 1,
        })];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        let stats = verifier.stats();
        assert_eq!(stats.sent, 3);
        assert_eq!(stats.received, 3);
        assert_eq!(stats.received_malformed, 0);

        // Pretend 10 seconds have passed, make sure stats are reset
        verifier.set_last_stats_logged(Instant::now() - STATS_INTERVAL_DURATION);
        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(7),
            signature: Signature::default(),
            rank: 2,
        })];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        // Since we just logged all stats (including the packet just sent), stats should be reset
        let stats = verifier.stats();
        assert_eq!(stats.sent, 0);
        assert_eq!(stats.received, 0);
        assert_eq!(stats.received_malformed, 0);
    }

    #[test]
    fn test_blssigverifier_send_packets_malformed() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut verifier = BLSSigVerifier::new(sender);

        let packets = vec![Packet::default()];
        let packet_batches = vec![PacketBatch::new(packets)];
        assert!(verifier.send_packets(packet_batches).is_ok());
        let stats = verifier.stats();
        assert_eq!(stats.sent, 0);
        assert_eq!(stats.received, 1);
        assert_eq!(stats.received_malformed, 1);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());
    }

    #[test]
    fn test_blssigverifier_send_packets_channel_full() {
        solana_logger::setup();
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let mut verifier = BLSSigVerifier::new(sender);
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
        let mut verifier = BLSSigVerifier::new(sender);
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
