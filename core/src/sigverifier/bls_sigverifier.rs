//! The BLS signature verifier.
//! This is just a placeholder for now, until we have a real implementation.

use {
    crate::sigverify_stage::{SigVerifier, SigVerifyServiceError},
    alpenglow_vote::bls_message::BLSMessage,
    crossbeam_channel::{Sender, TrySendError},
    solana_streamer::packet::PacketBatch,
    std::time::{Duration, Instant},
};

const STATS_INTERVAL_SECONDS: u64 = 10; // Log stats every 10 second

// We are adding our own stats because we do BLS decoding in batch verification,
// and we send one BLS message at a time. So it makes sense to have finer-grained stats
#[derive(Debug, Default, Clone)]
pub(crate) struct BLSSigVerifierStats {
    pub bls_messages_sent: u64,
    pub bls_messages_malformed: u64,
    pub sent_failed: u64,
    pub packets_received: u64,
}

pub struct BLSSigVerifier {
    sender: Sender<BLSMessage>,
    stats: BLSSigVerifierStats,
    last_stats_logged: Instant,
}

impl SigVerifier for BLSSigVerifier {
    type SendType = ();
    // TODO(wen): just a placeholder without any verification.
    fn verify_batches(&self, batches: Vec<PacketBatch>, _valid_packets: usize) -> Vec<PacketBatch> {
        batches
    }

    fn send_packets(
        &mut self,
        packet_batches: Vec<PacketBatch>,
    ) -> Result<(), SigVerifyServiceError<Self::SendType>> {
        // TODO(wen): just a placeholder without any batching.
        let mut packet_received = 0;
        let mut bls_messages_sent = 0;
        let mut bls_messages_malformed = 0;
        let mut sent_failed = 0;
        packet_batches.iter().for_each(|batch| {
            batch.iter().for_each(|packet| {
                packet_received += 1;
                match packet.deserialize_slice::<BLSMessage, _>(..) {
                    Ok(message) => match self.sender.try_send(message) {
                        Ok(()) => {
                            bls_messages_sent += 1;
                        }
                        Err(TrySendError::Full(_)) => {
                            warn!("BLS message channel is full, dropping message");
                            sent_failed += 1;
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            // There is no hope to recover, restart the validator.
                            panic!("BLS message channel is disconnected");
                        }
                    },
                    Err(e) => {
                        trace!("Failed to deserialize BLS message: {}", e);
                        bls_messages_malformed += 1;
                    }
                }
            });
        });
        if packet_received > 0 {
            self.stats.packets_received += packet_received;
            self.stats.bls_messages_sent += bls_messages_sent;
            self.stats.bls_messages_malformed += bls_messages_malformed;
            self.stats.sent_failed += sent_failed;
        }
        // We don't need lock on stats because stats are read and write in a single thread.
        self.report_stats();
        Ok(())
    }
}

impl BLSSigVerifier {
    fn report_stats(&mut self) {
        let now = Instant::now();
        let time_since_last_log = now.duration_since(self.last_stats_logged);
        if time_since_last_log < Duration::from_secs(STATS_INTERVAL_SECONDS) {
            return;
        }
        datapoint_info!(
            "bls_sig_verifier_stats",
            ("sent", self.stats.bls_messages_sent, u64),
            ("sent_failed", self.stats.sent_failed, u64),
            ("malformed", self.stats.bls_messages_malformed, u64),
            ("received", self.stats.packets_received, u64)
        );
        self.last_stats_logged = now;
    }

    pub fn new(sender: Sender<BLSMessage>) -> Self {
        Self {
            sender,
            stats: BLSSigVerifierStats::default(),
            last_stats_logged: Instant::now(),
        }
    }

    #[cfg(feature = "dev-context-only-utils")]
    #[allow(dead_code)]
    pub(crate) fn stats(&self) -> &BLSSigVerifierStats {
        &self.stats
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
        assert!(verifier.send_packets(packet_batches).is_ok());
        if let Some(receiver) = receiver {
            for msg in messages {
                match receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(received_msg) => assert_eq!(received_msg, *msg),
                    Err(e) => panic!("Failed to receive BLS message: {}", e),
                }
            }
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
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages);
        let stats = verifier.stats();
        assert_eq!(stats.bls_messages_sent, 2);
        assert_eq!(stats.packets_received, 2);
        assert_eq!(stats.sent_failed, 0);
        assert_eq!(stats.bls_messages_malformed, 0);

        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_notarization_vote(6, Hash::new_unique(), Hash::new_unique()),
            signature: Signature::default(),
            rank: 1,
        })];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages);
        let stats = verifier.stats();
        assert_eq!(stats.bls_messages_sent, 3);
        assert_eq!(stats.packets_received, 3);
        assert_eq!(stats.sent_failed, 0);
        assert_eq!(stats.bls_messages_malformed, 0);
    }

    #[test]
    fn test_blssigverifier_send_packets_malformed() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let mut verifier = BLSSigVerifier::new(sender);

        let packets = vec![Packet::default()];
        let packet_batches = vec![PacketBatch::new(packets)];
        assert!(verifier.send_packets(packet_batches).is_ok());
        let stats = verifier.stats();
        assert_eq!(stats.bls_messages_sent, 0);
        assert_eq!(stats.packets_received, 1);
        assert_eq!(stats.sent_failed, 0);
        assert_eq!(stats.bls_messages_malformed, 1);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());

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
        test_bls_message_transmission(&mut verifier, None, &messages);

        // Since we sent two packets and receiver can only hold one, we should see drop.
        let stats = verifier.stats();
        assert_eq!(stats.bls_messages_sent, 1);
        assert_eq!(stats.packets_received, 3);
        assert_eq!(stats.sent_failed, 1);
        assert_eq!(stats.bls_messages_malformed, 1);
    }

    #[test]
    #[should_panic(expected = "BLS message channel is disconnected")]
    fn test_blssigverifier_send_packets_receiver_closed() {
        solana_logger::setup();
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let mut verifier = BLSSigVerifier::new(sender);
        // Close the receiver, should get panic on next send
        drop(receiver);
        let messages = vec![BLSMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 0,
        })];
        test_bls_message_transmission(&mut verifier, None, &messages);
    }
}
