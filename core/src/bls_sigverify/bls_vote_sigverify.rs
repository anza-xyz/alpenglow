#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    crate::{
        bls_sigverify::{error::BLSSigVerifyError, stats::BLSSigVerifierStats},
        cluster_info_vote_listener::VerifiedVoteSender,
    },
    crossbeam_channel::{Sender, TrySendError},
    rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator},
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
        vote::Vote,
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

impl VoteToVerify {
    pub(crate) fn verify(&self) -> bool {
        let Ok(payload) = bincode::serialize(&self.vote_message.vote) else {
            return false;
        };
        self.bls_pubkey
            .verify_signature(&self.vote_message.signature, &payload)
            .is_ok()
    }
}

/// Verifies votes and sends verified votes to the consensus pool.
///
/// All verified votes are sent to the rewards, consensus, and repair senders
/// inside this function.
#[allow(clippy::too_many_arguments)]
pub(super) fn verify_and_send_votes(
    votes_to_verify: &[VoteToVerify],
    root_bank: &Bank,
    stats: &BLSSigVerifierStats,
    cluster_info: &ClusterInfo,
    leader_schedule: &LeaderScheduleCache,
    message_sender: &Sender<ConsensusMessage>,
    votes_for_repair_sender: &VerifiedVoteSender,
    reward_votes_sender: &Sender<AddVoteMessage>,
    last_voted_slots: &mut HashMap<Pubkey, Slot>,
    consensus_metrics: &mut Vec<ConsensusMetricsEvent>,
) -> Result<(), BLSSigVerifyError> {
    let verified_votes = verify_votes(votes_to_verify, stats);

    stats
        .total_valid_packets
        .fetch_add(verified_votes.len() as u64, Ordering::Relaxed);

    send_votes_to_rewards(
        &verified_votes,
        root_bank,
        cluster_info,
        leader_schedule,
        reward_votes_sender,
        stats,
    );

    let votes_for_repair = process_and_send_votes_to_consensus(
        &verified_votes,
        message_sender,
        last_voted_slots,
        consensus_metrics,
        stats,
    )?;

    send_votes_to_repair(votes_for_repair, votes_for_repair_sender, stats);

    Ok(())
}

fn send_votes_to_rewards(
    verified_votes: &[VoteToVerify],
    root_bank: &Bank,
    cluster_info: &ClusterInfo,
    leader_schedule: &LeaderScheduleCache,
    reward_votes_sender: &Sender<AddVoteMessage>,
    stats: &BLSSigVerifierStats,
) {
    let votes = verified_votes
        .iter()
        .filter_map(|v| {
            let vote = v.vote_message;
            consensus_rewards::wants_vote(cluster_info, leader_schedule, root_bank.slot(), &vote)
                .then_some(vote)
        })
        .collect();

    match reward_votes_sender.try_send(AddVoteMessage { votes }) {
        Ok(()) => (),
        Err(TrySendError::Full(_)) => {
            stats
                .consensus_reward_send_failed
                .fetch_add(1, Ordering::Relaxed);
        }
        Err(TrySendError::Disconnected(_)) => {
            warn!("could not send votes to reward container, receive side of channel is closed");
        }
    }
}

/// Dispatches verified BLS votes to the consensus pool and aggregates vote
/// data for the repair service.
///
/// # Returns
///
/// Returns a `HashMap` mapping validator public keys to the vector of slots
/// they voted for, intended for the repair sender.
#[allow(clippy::too_many_arguments)]
fn process_and_send_votes_to_consensus(
    verified_votes: &[VoteToVerify],
    message_sender: &Sender<ConsensusMessage>,
    last_voted_slots: &mut HashMap<Pubkey, Slot>,
    consensus_metrics: &mut Vec<ConsensusMetricsEvent>,
    stats: &BLSSigVerifierStats,
) -> Result<HashMap<Pubkey, Vec<Slot>>, BLSSigVerifyError> {
    let mut votes_for_repair = HashMap::new();
    for vote in verified_votes {
        stats.received_votes.fetch_add(1, Ordering::Relaxed);

        let vote_msg = vote.vote_message;
        let slot = vote_msg.vote.slot();

        consensus_metrics.push(ConsensusMetricsEvent::Vote {
            id: vote.pubkey,
            vote: vote.vote_message.vote,
        });

        if vote.vote_message.vote.is_notarization_or_finalization() {
            last_voted_slots
                .entry(vote.pubkey)
                .and_modify(|s| *s = (*s).max(slot))
                .or_insert(vote.vote_message.vote.slot());
        }

        if vote.vote_message.vote.is_notarization_or_finalization()
            || vote.vote_message.vote.is_notarize_fallback()
        {
            let cur_slots: &mut Vec<Slot> = votes_for_repair.entry(vote.pubkey).or_default();
            if !cur_slots.contains(&slot) {
                cur_slots.push(slot);
            }
        }

        send_vote_to_consensus_pool(message_sender, vote_msg, stats)?;
    }

    Ok(votes_for_repair)
}

fn send_vote_to_consensus_pool(
    message_sender: &Sender<ConsensusMessage>,
    vote_msg: VoteMessage,
    stats: &BLSSigVerifierStats,
) -> Result<(), BLSSigVerifyError> {
    match message_sender.try_send(ConsensusMessage::Vote(vote_msg)) {
        Ok(()) => {
            stats.sent.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            stats.sent_failed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(e @ TrySendError::Disconnected(_)) => Err(e.into()),
    }
}

fn send_votes_to_repair(
    votes_for_repair: HashMap<Pubkey, Vec<Slot>>,
    votes_for_repair_sender: &VerifiedVoteSender,
    stats: &BLSSigVerifierStats,
) {
    for (pubkey, slots) in votes_for_repair {
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
}

fn verify_votes(
    votes_to_verify: &[VoteToVerify],
    stats: &BLSSigVerifierStats,
) -> Vec<VoteToVerify> {
    if votes_to_verify.is_empty() {
        return vec![];
    }
    stats.votes_batch_count.fetch_add(1, Ordering::Relaxed);

    // Try optimistic verification - fast to verify, but cannot identify invalid votes
    if verify_votes_optimistic(votes_to_verify, stats) {
        return votes_to_verify.to_vec();
    }

    // Fallback to individual verification
    verify_individual_votes(votes_to_verify, stats)
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
fn verify_votes_optimistic(votes_to_verify: &[VoteToVerify], stats: &BLSSigVerifierStats) -> bool {
    let mut votes_batch_optimistic_time = Measure::start("votes_batch_optimistic");

    // For BLS verification, minimizing the expensive pairing operation is key.
    // Each BLS signature verification requires two pairings.
    //
    // However, the BLS verification formula allows us to:
    // 1. Aggregate all signatures into a single signature.
    // 2. Aggregate public keys for each unique message.
    //
    // By verifying the aggregated signature against the aggregated public keys,
    // the number of pairings required is reduced to (1 + number of distinct messages).
    let (signature_result, (distinct_payloads, pubkeys_result)) = rayon::join(
        || aggregate_signatures(votes_to_verify),
        || aggregate_pubkeys_by_payload(votes_to_verify, stats),
    );

    let Ok(aggregate_signature) = signature_result else {
        return false;
    };

    let Ok(aggregate_pubkeys) = pubkeys_result else {
        return false;
    };

    let verified = if distinct_payloads.len() == 1 {
        // if one unique payload, just verify the aggregate signature for the single payload
        // this requires (2 pairings)
        aggregate_pubkeys[0]
            .verify_signature(&aggregate_signature, &distinct_payloads[0])
            .is_ok()
    } else {
        // if non-unique payload, we need to apply a pairing for each distinct message,
        // which is done inside `par_verify_distinct_aggregated`.
        let payload_slices: Vec<&[u8]> = distinct_payloads.iter().map(|p| p.as_slice()).collect();
        SignatureProjective::par_verify_distinct_aggregated(
            &aggregate_pubkeys,
            &aggregate_signature,
            &payload_slices,
        )
        .is_ok()
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
    // TODO(sam): Currently, `par_aggregate` performs full validation
    // (on-curve + subgroup check) for every signature. Since the subgroup
    // check is expensive, we can use an `unchecked` deserialization here
    // (performing only the cheap on-curve check) and rely on a single subgroup
    // check on the final aggregated signature. This should save more than 80%
    // of the time for signature aggregation.
    SignatureProjective::par_aggregate(signatures)
}

#[allow(clippy::type_complexity)]
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
fn aggregate_pubkeys_by_payload(
    votes: &[VoteToVerify],
    stats: &BLSSigVerifierStats,
) -> (Vec<Vec<u8>>, Result<Vec<PubkeyProjective>, BlsError>) {
    let mut grouped_votes: HashMap<&Vote, Vec<&BlsPubkey>> = HashMap::new();

    for v in votes {
        grouped_votes
            .entry(&v.vote_message.vote)
            .or_default()
            .push(&v.bls_pubkey);
    }

    let distinct_messages = grouped_votes.len();
    stats
        .votes_batch_distinct_messages_count
        .fetch_add(distinct_messages as u64, Ordering::Relaxed);

    let (distinct_payloads, distinct_pubkeys_results): (Vec<_>, Vec<_>) = grouped_votes
        .into_par_iter()
        .map(|(vote, pubkeys)| {
            (
                get_vote_payload(vote),
                // TODO(sam): https://github.com/anza-xyz/alpenglow/issues/708
                // should improve public key aggregation drastically (more than 80%)
                PubkeyProjective::par_aggregate(pubkeys.into_par_iter()),
            )
        })
        .unzip();
    let aggregate_pubkeys_result = distinct_pubkeys_results.into_iter().collect();

    (distinct_payloads, aggregate_pubkeys_result)
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
fn verify_individual_votes(
    votes_to_verify: &[VoteToVerify],
    stats: &BLSSigVerifierStats,
) -> Vec<VoteToVerify> {
    let mut votes_batch_parallel_verify_time = Measure::start("votes_batch_parallel_verify");

    let verified_votes: Vec<VoteToVerify> = votes_to_verify
        .into_par_iter()
        .filter_map(|vote| {
            // verify signature
            if !vote.verify() {
                // if fail, record stats and return `None`
                stats
                    .received_bad_signature_votes
                    .fetch_add(1, Ordering::Relaxed);
                return None;
            }
            // if success, return `VoteToVerify` to provide to `Sender`s
            Some(*vote)
        })
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

fn get_vote_payload(vote: &Vote) -> Vec<u8> {
    bincode::serialize(vote).expect("Failed to serialize vote")
}
