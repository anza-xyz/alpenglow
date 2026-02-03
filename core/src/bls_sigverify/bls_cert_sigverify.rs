use {
    crate::bls_sigverify::{error::BLSSigVerifyError, stats::BLSSigVerifierStats},
    agave_bls_cert_verify::cert_verify::Error as BlsCertVerifyError,
    crossbeam_channel::{Sender, TrySendError},
    rayon::iter::{IntoParallelIterator, ParallelIterator},
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_runtime::{bank::Bank, epoch_stakes::BLSPubkeyToRankMap},
    solana_votor_messages::{
        consensus_message::{Certificate, CertificateType, ConsensusMessage},
        fraction::Fraction,
    },
    std::{
        collections::HashSet,
        num::NonZeroU64,
        sync::{atomic::Ordering, Arc, RwLock},
    },
    thiserror::Error,
};

pub(crate) fn get_key_to_rank_map(
    bank: &Bank,
    slot: Slot,
) -> Option<(&Arc<BLSPubkeyToRankMap>, u64)> {
    bank.epoch_stakes_from_slot(slot)
        .map(|stake| (stake.bls_pubkey_to_rank_map(), stake.total_stake()))
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

pub(crate) fn verify_and_send_certificates(
    certs_buffer: &mut Vec<Certificate>,
    bank: &Bank,
    verified_certs: &RwLock<HashSet<CertificateType>>,
    stats: &BLSSigVerifierStats,
    message_sender: &Sender<ConsensusMessage>,
) -> Result<(), BLSSigVerifyError> {
    let results = verify_certificates(certs_buffer, bank, verified_certs, stats);

    let valid_count = results.iter().filter(|&&valid| valid).count();
    stats
        .total_valid_packets
        .fetch_add(valid_count as u64, Ordering::Relaxed);

    for (cert, is_valid) in certs_buffer.drain(..).zip(results) {
        // Send the BLS certificate message to certificate pool.
        if is_valid {
            match message_sender.try_send(ConsensusMessage::Certificate(cert)) {
                Ok(()) => {
                    stats.sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Full(_)) => {
                    stats.sent_failed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e @ TrySendError::Disconnected(_)) => {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}

fn verify_certificates(
    certs_to_verify: &[Certificate],
    bank: &Bank,
    verified_certs: &RwLock<HashSet<CertificateType>>,
    stats: &BLSSigVerifierStats,
) -> Vec<bool> {
    if certs_to_verify.is_empty() {
        return vec![];
    }
    stats.certs_batch_count.fetch_add(1, Ordering::Relaxed);
    let mut certs_batch_verify_time = Measure::start("certs_batch_verify");

    let verified_results: Vec<bool> = certs_to_verify
        .into_par_iter()
        .map(|cert_to_verify| {
            match verify_bls_certificate(cert_to_verify, bank, verified_certs, stats) {
                Ok(()) => true,
                Err(e) => {
                    trace!(
                        "Failed to verify BLS certificate: {:?}, error: {e}",
                        cert_to_verify.cert_type
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
                    false
                }
            }
        })
        .collect();

    certs_batch_verify_time.stop();
    stats
        .certs_batch_elapsed_us
        .fetch_add(certs_batch_verify_time.as_us(), Ordering::Relaxed);
    verified_results
}

fn verify_bls_certificate(
    cert_to_verify: &Certificate,
    bank: &Bank,
    verified_certs: &RwLock<HashSet<CertificateType>>,
    stats: &BLSSigVerifierStats,
) -> Result<(), CertVerifyError> {
    if verified_certs
        .read()
        .unwrap()
        .contains(&cert_to_verify.cert_type)
    {
        stats.received_verified.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }

    let slot = cert_to_verify.cert_type.slot();
    let (aggregate_stake, total_stake) = bank.verify_certificate(cert_to_verify).map_err(|e| {
        if matches!(e, BlsCertVerifyError::MissingRankMap) {
            CertVerifyError::KeyToRankMapNotFound(slot)
        } else {
            CertVerifyError::CertVerifyFailed(e)
        }
    })?;

    let (required_stake_fraction, _) = cert_to_verify.cert_type.limits_and_vote_types();
    let my_fraction = Fraction::new(aggregate_stake, NonZeroU64::new(total_stake).unwrap());
    if my_fraction < required_stake_fraction {
        return Err(CertVerifyError::NotEnoughStake(
            aggregate_stake,
            my_fraction,
            required_stake_fraction,
        ));
    }

    verified_certs
        .write()
        .unwrap()
        .insert(cert_to_verify.cert_type);

    Ok(())
}
