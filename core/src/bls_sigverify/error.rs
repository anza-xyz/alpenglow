use {
    crossbeam_channel::{SendError, TrySendError},
    solana_votor_messages::consensus_message::ConsensusMessage,
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum BLSSigVerifyError {
    #[error("Send error")]
    Send(Box<SendError<ConsensusMessage>>),

    #[error("TrySend error")]
    TrySend(Box<TrySendError<ConsensusMessage>>),

    #[error(transparent)]
    Streamer(#[from] solana_streamer::streamer::StreamerError),
}

impl From<SendError<ConsensusMessage>> for BLSSigVerifyError {
    fn from(err: SendError<ConsensusMessage>) -> Self {
        Self::Send(Box::new(err))
    }
}

impl From<TrySendError<ConsensusMessage>> for BLSSigVerifyError {
    fn from(err: TrySendError<ConsensusMessage>) -> Self {
        Self::TrySend(Box::new(err))
    }
}
