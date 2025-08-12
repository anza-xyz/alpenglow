use {
    crossbeam_channel::{Sender, TrySendError},
    solana_clock::Slot,
};

#[derive(Debug)]
pub enum AlpenglowUpdateCommitmentCacheError {
    ChannelDisconnected,
    ChannelFull,
}

pub enum AlpenglowCommitment {
    /// Our node has voted notarize for the slot
    Notarize(Slot),
    /// We have observed a finalization certificate for the slot
    Finalized(Slot),
}

pub fn alpenglow_update_commitment_cache(
    commitment: AlpenglowCommitment,
    sender: &Sender<AlpenglowCommitment>,
) -> Result<(), AlpenglowUpdateCommitmentCacheError> {
    sender.try_send(commitment).map_err(|err| match err {
        TrySendError::Disconnected(_) => {
            error!("commitment_sender disconnected");
            AlpenglowUpdateCommitmentCacheError::ChannelDisconnected
        }
        TrySendError::Full(_) => {
            error!("commitment_sender is backed up, something is wrong");
            AlpenglowUpdateCommitmentCacheError::ChannelFull
        }
    })
}
