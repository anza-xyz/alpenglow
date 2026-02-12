use {
    super::bls_vote_sigverify::Error as VerifyVotesError,
    crossbeam_channel::{SendError, TrySendError},
    solana_votor_messages::consensus_message::ConsensusMessage,
    thiserror::Error,
};

#[derive(Error, Debug)]
pub(super) enum BLSSigVerifyError {
    #[error("Send error")]
    Send(Box<SendError<ConsensusMessage>>),

    #[error("TrySend error")]
    TrySend(Box<TrySendError<ConsensusMessage>>),

    #[error(transparent)]
    Streamer(#[from] solana_streamer::streamer::StreamerError),

    #[error("verifying votes failed with {0}")]
    VerifyVotes(#[from] VerifyVotesError),
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
