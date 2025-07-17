use {
    crossbeam_channel::{Sender, TrySendError},
    solana_sdk::clock::Slot,
};

#[derive(Debug)]
pub enum AlpenglowCommitmentType {
    /// Our node has voted notarize for the slot
    Notarize,
    /// We have observed a finalization certificate for the slot
    Finalized,
}

#[derive(Debug)]
pub struct AlpenglowCommitmentAggregationData {
    pub commitment_type: AlpenglowCommitmentType,
    pub slot: Slot,
}

pub fn alpenglow_update_commitment_cache(
    msg: AlpenglowCommitmentAggregationData,
    commitment_sender: &Sender<AlpenglowCommitmentAggregationData>,
) -> Result<(), AlpenglowCommitmentAggregationData> {
    match commitment_sender.try_send(msg) {
        Err(TrySendError::Disconnected(msg)) => {
            error!("Sending {msg:?}: commitment_sender has disconnected");
            // TODO(ashwin): Use return type to exit voting loop
            Err(msg)
        }
        Err(TrySendError::Full(msg)) => {
            error!("Sending {msg:?}: commitment_sender is full");
            Err(msg)
        }
        Ok(()) => Ok(()),
    }
}
