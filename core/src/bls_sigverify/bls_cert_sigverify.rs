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
    mut certs: Vec<Certificate>,
    bank: &Bank,
    verified_certs: &RwLock<HashSet<CertificateType>>,
    stats: &BLSSigVerifierStats,
    channel_to_pool: &Sender<Vec<ConsensusMessage>>,
) -> Result<(), Error> {
    dedupe_certificates(&mut certs, verified_certs, stats);

    if certs.is_empty() {
        return Ok(());
    }

    stats.certs_batch_count.fetch_add(1, Ordering::Relaxed);
    let mut certs_batch_verify_time = Measure::start("certs_batch_verify");

    let messages: Vec<ConsensusMessage> = certs
        .into_par_iter()
        .filter_map(|cert| match verify_bls_certificate(&cert, bank, stats) {
            Ok(()) => Some(ConsensusMessage::Certificate(cert)),
            Err(e) => {
                trace!(
                    "Failed to verify BLS certificate: {:?}, error: {e}",
                    cert.cert_type
                );
                None
            }
        })
        .collect();

    if !messages.is_empty() {
        let mut cache_guard = verified_certs.write().unwrap();
        for msg in &messages {
            if let ConsensusMessage::Certificate(cert) = msg {
                cache_guard.insert(cert.cert_type);
            }
        }
    }

    stats
        .total_valid_packets
        .fetch_add(messages.len() as u64, Ordering::Relaxed);

    certs_batch_verify_time.stop();
    stats
        .certs_batch_elapsed_us
        .fetch_add(certs_batch_verify_time.as_us(), Ordering::Relaxed);

    send_certs_to_pool(messages, channel_to_pool, stats)
}

fn dedupe_certificates(
    certs: &mut Vec<Certificate>,
    verified_certs: &RwLock<HashSet<CertificateType>>,
    stats: &BLSSigVerifierStats,
) {
    if certs.is_empty() {
        return;
    }

    let already_verified = verified_certs.read().unwrap();

    certs.retain(|cert| {
        // check global cache
        if already_verified.contains(&cert.cert_type) {
            stats.received_verified.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    });
}

fn send_certs_to_pool(
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
    stats: &BLSSigVerifierStats,
) -> Result<(), CertVerifyError> {
    let (aggregate_stake, total_stake) = verify_certificate_signature(cert, bank, stats)?;
    verify_stake(cert, aggregate_stake, total_stake, stats)?;
    Ok(())
}

fn verify_certificate_signature(
    cert: &Certificate,
    bank: &Bank,
    stats: &BLSSigVerifierStats,
) -> Result<(u64, u64), CertVerifyError> {
    bank.verify_certificate(cert).map_err(|e| {
        if !matches!(e, BlsCertVerifyError::MissingRankMap) {
            stats
                .received_bad_signature_certs
                .fetch_add(1, Ordering::Relaxed);
        }

        match e {
            BlsCertVerifyError::MissingRankMap => {
                CertVerifyError::KeyToRankMapNotFound(cert.cert_type.slot())
            }
            _ => e.into(),
        }
    })
}

fn verify_stake(
    cert: &Certificate,
    aggregate_stake: u64,
    total_stake: u64,
    stats: &BLSSigVerifierStats,
) -> Result<(), CertVerifyError> {
    let (required_stake_fraction, _) = cert.cert_type.limits_and_vote_types();
    let total_stake = NonZeroU64::new(total_stake).expect("Total stake cannot be zero");
    let cert_stake_fraction = Fraction::new(aggregate_stake, total_stake);
    if cert_stake_fraction >= required_stake_fraction {
        Ok(())
    } else {
        stats
            .received_not_enough_stake
            .fetch_add(1, Ordering::Relaxed);
        Err(CertVerifyError::NotEnoughStake(
            aggregate_stake,
            cert_stake_fraction,
            required_stake_fraction,
        ))
    }
}
