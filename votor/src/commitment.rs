use {crossbeam_channel::Sender, log::trace, solana_sdk::clock::Slot};

pub enum AlpenglowCommitmentType {
    /// Our node has voted notarize for the slot
    Notarize,
    /// We have observed a finalization certificate for the slot
    Finalized,
}

pub struct AlpenglowCommitmentAggregationData {
    pub commitment_type: AlpenglowCommitmentType,
    pub slot: Slot,
}

pub fn alpenglow_update_commitment_cache(
    commitment_type: AlpenglowCommitmentType,
    slot: Slot,
    commitment_sender: &Sender<AlpenglowCommitmentAggregationData>,
) {
    if let Err(e) = commitment_sender.send(AlpenglowCommitmentAggregationData {
        commitment_type,
        slot,
    }) {
        trace!("commitment_sender failed: {:?}", e);
    }
}
