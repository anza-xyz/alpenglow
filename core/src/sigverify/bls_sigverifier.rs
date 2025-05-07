//! The BLS signature verifier.
//! This is just a placeholder for now, until we have a real implementation.

use {
    crate::sigverify_stage::{SigVerifier, SigVerifyServiceError},
    solana_perf::sigverify::ed25519_verify_disabled,
    solana_streamer::packet::PacketBatch,
};

#[derive(Default, Clone)]
pub struct BLSSigVerifier {}

impl SigVerifier for BLSSigVerifier {
    type SendType = ();
    fn verify_batches(
        &self,
        mut batches: Vec<PacketBatch>,
        _valid_packets: usize,
    ) -> Vec<PacketBatch> {
        ed25519_verify_disabled(&mut batches);
        batches
    }

    fn send_packets(
        &mut self,
        _packet_batches: Vec<PacketBatch>,
    ) -> Result<(), SigVerifyServiceError<Self::SendType>> {
        Ok(())
    }
}
