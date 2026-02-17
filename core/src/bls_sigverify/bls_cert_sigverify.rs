use {
    super::stats::BLSSigVerifierStats,
    agave_bls_cert_verify::cert_verify::Error as BlsCertVerifyError,
    crossbeam_channel::{Sender, TrySendError},
    rayon::iter::{IntoParallelIterator, ParallelIterator},
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_runtime::bank::Bank,
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateType, ConsensusMessage},
        fraction::Fraction,
    },
    std::{
        collections::HashSet,
        num::NonZeroU64,
        sync::{atomic::Ordering, RwLock},
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("channel to consensus pool disconnected")]
    ConsensusPoolChannelDisconnected,
}

#[derive(Debug, Error)]
enum CertVerifyError {
    #[error("Failed to find key to rank map for slot {0}")]
    KeyToRankMapNotFound(Slot),

    #[error("Cert Verification Error {0:?}")]
    CertVerifyFailed(#[from] BlsCertVerifyError),

    #[error("Not enough stake {0}: {1} < {2}")]
    NotEnoughStake(u64, Fraction, Fraction),
}

pub(super) fn verify_and_send_certificates(
    certs_buffer: Vec<Certificate>,
    bank: &Bank,
    verified_certs: &RwLock<HashSet<CertificateType>>,
    stats: &BLSSigVerifierStats,
    channel_to_pool: &Sender<Vec<ConsensusMessage>>,
) -> Result<(), Error> {
    if certs_buffer.is_empty() {
        return Ok(());
    }

    stats.certs_batch_count.fetch_add(1, Ordering::Relaxed);
    let mut certs_batch_verify_time = Measure::start("certs_batch_verify");

    let messages: Vec<ConsensusMessage> = certs_buffer
        .into_par_iter()
        .filter_map(
            |cert| match verify_bls_certificate(&cert, bank, verified_certs, stats) {
                Ok(()) => {
                    stats.total_valid_packets.fetch_add(1, Ordering::Relaxed);
                    Some(ConsensusMessage::Certificate(cert))
                }
                Err(e) => {
                    trace!(
                        "Failed to verify BLS certificate: {:?}, error: {e}",
                        cert.cert_type
                    );

                    if let CertVerifyError::NotEnoughStake(..) = e {
                        stats
                            .received_not_enough_stake
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats
                            .received_bad_signature_certs
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    None
                }
            },
        )
        .collect();

    certs_batch_verify_time.stop();
    stats
        .certs_batch_elapsed_us
        .fetch_add(certs_batch_verify_time.as_us(), Ordering::Relaxed);

    send_to_consensus_pool(messages, channel_to_pool, stats)
}

fn send_to_consensus_pool(
    messages: Vec<ConsensusMessage>,
    channel_to_pool: &Sender<Vec<ConsensusMessage>>,
    stats: &BLSSigVerifierStats,
) -> Result<(), Error> {
    if messages.is_empty() {
        return Ok(());
    }

    let len = messages.len();

    match channel_to_pool.try_send(messages) {
        Ok(()) => {
            stats
                .verify_certs_consensus_sent
                .fetch_add(len as u64, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            stats
                .verify_certs_consensus_channel_full
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err(Error::ConsensusPoolChannelDisconnected),
    }
}

fn verify_bls_certificate(
    cert: &Certificate,
    bank: &Bank,
    verified_certs: &RwLock<HashSet<CertificateType>>,
    stats: &BLSSigVerifierStats,
) -> Result<(), CertVerifyError> {
    if verified_certs.read().unwrap().contains(&cert.cert_type) {
        stats.received_verified.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }

    // Does the signature verify?
    let (aggregate_stake, total_stake) = verify_certificate_signature(cert, bank)?;

    // Does cert represent enough stake?
    verify_stake(cert, aggregate_stake, total_stake)?;

    verified_certs.write().unwrap().insert(cert.cert_type);

    Ok(())
}

fn verify_certificate_signature(
    cert: &Certificate,
    bank: &Bank,
) -> Result<(u64, u64), CertVerifyError> {
    bank.verify_certificate(cert).map_err(|e| match e {
        BlsCertVerifyError::MissingRankMap => {
            CertVerifyError::KeyToRankMapNotFound(cert.cert_type.slot())
        }
        _ => e.into(),
    })
}

fn verify_stake(
    cert: &Certificate,
    aggregate_stake: u64,
    total_stake: u64,
) -> Result<(), CertVerifyError> {
    let (required_stake_fraction, _) = cert.cert_type.limits_and_vote_types();
    let total_stake = NonZeroU64::new(total_stake).expect("Total stake cannot be zero");
    let cert_stake_fraction = Fraction::new(aggregate_stake, total_stake);
    (cert_stake_fraction >= required_stake_fraction)
        .then_some(())
        .ok_or(CertVerifyError::NotEnoughStake(
            aggregate_stake,
            cert_stake_fraction,
            required_stake_fraction,
        ))
}
