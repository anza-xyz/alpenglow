#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    crate::{
        bls_sigverify::{error::BLSSigVerifyError, stats::BLSSigVerifierStats},
        cluster_info_vote_listener::VerifiedVoteSender,
    },
    crossbeam_channel::{Sender, TrySendError},
    rayon::iter::{
        IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
    },
    solana_bls_signatures::{
        pubkey::{Pubkey as BlsPubkey, PubkeyProjective, VerifiablePubkey},
        signature::SignatureProjective,
        BlsError,
    },
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_measure::measure::Measure,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_votor::{consensus_metrics::ConsensusMetricsEvent, consensus_rewards},
    solana_votor_messages::{
        consensus_message::{ConsensusMessage, VoteMessage},
        reward_certificate::AddVoteMessage,
    },
    std::{collections::HashMap, sync::atomic::Ordering},
};

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct VoteToVerify {
    pub vote_message: VoteMessage,
    pub bls_pubkey: BlsPubkey,
    pub pubkey: Pubkey,
}

/// Verifies votes and sends verified votes to the consensus pool.
/// Also returns a copy of the verified votes that the rewards container is interested is so that the caller can send them to it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn verify_and_send_votes(
    votes_to_verify: &[VoteToVerify],
    root_bank: &Bank,
    stats: &BLSSigVerifierStats,
    cluster_info: &ClusterInfo,
    leader_schedule: &LeaderScheduleCache,
    message_sender: &Sender<ConsensusMessage>,
    votes_for_repair_sender: &VerifiedVoteSender,
    last_voted_slots: &mut HashMap<Pubkey, Slot>,
    consensus_metrics: &mut Vec<ConsensusMetricsEvent>,
) -> Result<AddVoteMessage, BLSSigVerifyError> {
    let verified_votes = verify_votes(votes_to_verify, stats);

    let votes = verified_votes
        .iter()
        .filter_map(|v| {
            let vote = v.vote_message;
            consensus_rewards::wants_vote(cluster_info, leader_schedule, root_bank.slot(), &vote)
                .then_some(vote)
        })
        .collect();
    let add_vote_msg = AddVoteMessage { votes };

    stats
        .total_valid_packets
        .fetch_add(verified_votes.len() as u64, Ordering::Relaxed);

    let mut verified_votes_by_pubkey: HashMap<Pubkey, Vec<Slot>> = HashMap::new();
    for vote in verified_votes {
        stats.received_votes.fetch_add(1, Ordering::Relaxed);

        if vote.vote_message.vote.is_notarization_or_finalization() {
            let existing = last_voted_slots
                .entry(vote.pubkey)
                .or_insert(vote.vote_message.vote.slot());
            *existing = (*existing).max(vote.vote_message.vote.slot());
        }
        consensus_metrics.push(ConsensusMetricsEvent::Vote {
            id: vote.pubkey,
            vote: vote.vote_message.vote,
        });

        if vote.vote_message.vote.is_notarization_or_finalization()
            || vote.vote_message.vote.is_notarize_fallback()
        {
            let slot = vote.vote_message.vote.slot();
            let cur_slots: &mut Vec<Slot> =
                verified_votes_by_pubkey.entry(vote.pubkey).or_default();
            if !cur_slots.contains(&slot) {
                cur_slots.push(slot);
            }
        }

        // Send the votes to the consensus pool
        match message_sender.try_send(ConsensusMessage::Vote(vote.vote_message)) {
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

    // Send votes for repair
    for (pubkey, slots) in verified_votes_by_pubkey {
        match votes_for_repair_sender.try_send((pubkey, slots)) {
            Ok(()) => {
                stats.votes_for_repair_sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                trace!("Failed to send verified vote: {e}");
                stats
                    .votes_for_repair_sent_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    Ok(add_vote_msg)
}

fn verify_votes(
    votes_to_verify: &[VoteToVerify],
    stats: &BLSSigVerifierStats,
) -> Vec<VoteToVerify> {
    if votes_to_verify.is_empty() {
        return vec![];
    }

    stats.votes_batch_count.fetch_add(1, Ordering::Relaxed);

    // TODO: use wincode instead of bincode
    let payloads = votes_to_verify
        .iter()
        .map(|v| bincode::serialize(&v.vote_message.vote).expect("Failed to serialize vote"))
        .collect::<Vec<_>>();

    // Try optimistic verification
    if verify_votes_optimistic(votes_to_verify, &payloads, stats) {
        return votes_to_verify.to_vec();
    }

    // Fallback to individual verification
    verify_votes_fallback(votes_to_verify, &payloads, stats)
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
fn verify_votes_optimistic(
    votes_to_verify: &[VoteToVerify],
    payloads: &[Arc<Vec<u8>>],
    stats: &BLSSigVerifierStats,
) -> bool {
    let mut votes_batch_optimistic_time = Measure::start("votes_batch_optimistic");

    // aggregate signature
    let Ok(aggregate_signature) = aggregate_signatures(votes_to_verify) else {
        return false;
    };

    // aggregate public keys by payload
    let (distinct_payloads, aggregate_pubkeys_result) =
        aggregate_pubkeys_by_payload(votes_to_verify, payloads, stats);

    // final verification
    let verified = if let Ok(aggregate_pubkeys) = aggregate_pubkeys_result {
        if distinct_payloads.len() == 1 {
            let payload_slice = distinct_payloads[0].as_slice();
            aggregate_pubkeys[0]
                .verify_signature(&aggregate_signature, payload_slice)
                .is_ok()
        } else {
            let payload_slices: Vec<&[u8]> =
                distinct_payloads.iter().map(|p| p.as_slice()).collect();

            let aggregate_pubkeys_affine: Vec<BlsPubkey> =
                aggregate_pubkeys.into_iter().map(|pk| pk.into()).collect();

            SignatureProjective::par_verify_distinct_aggregated(
                &aggregate_pubkeys_affine,
                &aggregate_signature,
                &payload_slices,
            )
            .is_ok()
        }
    } else {
        false
    };

    votes_batch_optimistic_time.stop();
    stats
        .votes_batch_optimistic_elapsed_us
        .fetch_add(votes_batch_optimistic_time.as_us(), Ordering::Relaxed);

    verified
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
fn aggregate_signatures(votes: &[VoteToVerify]) -> Result<SignatureProjective, BlsError> {
    let signatures = votes.par_iter().map(|v| &v.vote_message.signature);
    SignatureProjective::par_aggregate(signatures)
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
fn aggregate_pubkeys_by_payload<'a>(
    votes: &[VoteToVerify],
    payloads: &'a [Arc<Vec<u8>>],
    stats: &BLSSigVerifierStats,
) -> (
    Vec<&'a Arc<Vec<u8>>>,
    Result<Vec<PubkeyProjective>, BlsError>,
) {
    let mut grouped_pubkeys: HashMap<&Arc<Vec<u8>>, Vec<&BlsPubkey>> = HashMap::new();
    for (v, payload) in votes.iter().zip(payloads.iter()) {
        grouped_pubkeys
            .entry(payload)
            .or_default()
            .push(&v.bls_pubkey);
    }

    let distinct_messages = grouped_pubkeys.len();
    stats
        .votes_batch_distinct_messages_count
        .fetch_add(distinct_messages as u64, Ordering::Relaxed);

    let (distinct_payloads, distinct_pubkeys_groups): (Vec<_>, Vec<_>) =
        grouped_pubkeys.into_iter().unzip();

    let aggregate_pubkeys_result = distinct_pubkeys_groups
        .into_par_iter()
        .map(|pks| PubkeyProjective::par_aggregate(pks.into_par_iter()))
        .collect();

    (distinct_payloads, aggregate_pubkeys_result)
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
fn verify_votes_fallback(
    votes_to_verify: &[VoteToVerify],
    payloads: &[Arc<Vec<u8>>],
    stats: &BLSSigVerifierStats,
) -> Vec<VoteToVerify> {
    let mut votes_batch_parallel_verify_time = Measure::start("votes_batch_parallel_verify");

    let verified_votes = votes_to_verify
        .into_par_iter()
        .zip(payloads.par_iter())
        .filter(|(vote_to_verify, payload)| {
            if vote_to_verify
                .bls_pubkey
                .verify_signature(&vote_to_verify.vote_message.signature, payload.as_slice())
                .is_ok()
            {
                true
            } else {
                stats
                    .received_bad_signature_votes
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
        })
        .map(|(v, _)| *v)
        .collect();
    votes_batch_parallel_verify_time.stop();
    stats
        .votes_batch_parallel_verify_count
        .fetch_add(1, Ordering::Relaxed);
    stats
        .votes_batch_parallel_verify_elapsed_us
        .fetch_add(votes_batch_parallel_verify_time.as_us(), Ordering::Relaxed);
    verified_votes
}
