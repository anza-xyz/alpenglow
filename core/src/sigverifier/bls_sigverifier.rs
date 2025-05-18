//! The BLS signature verifier.
//! This is just a placeholder for now, until we have a real implementation.

use {
    crate::sigverify_stage::{SigVerifier, SigVerifyServiceError},
    alpenglow_vote::bls_message::BLSMessage,
    crossbeam_channel::Sender,
    solana_streamer::packet::PacketBatch,
};

#[derive(Default, Clone)]
pub struct BLSSigVerifier {
    sender: Option<Sender<BLSMessage>>,
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
        if let Some(sender) = &self.sender {
            packet_batches.iter().for_each(|batch| {
                batch.iter().for_each(|packet| {
                    if let Ok(message) = packet.deserialize_slice::<BLSMessage, _>(..) {
                        sender.send(message).unwrap();
                    }
                });
            });
        }
        Ok(())
    }
}

impl BLSSigVerifier {
    pub fn new(sender: Option<Sender<BLSMessage>>) -> Self {
        Self { sender }
    }
}
