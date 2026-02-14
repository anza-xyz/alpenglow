#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    crate::{
        bls_sigverify::stats::BLSSigVerifierStats, cluster_info_vote_listener::VerifiedVoteSender,
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
    solana_votor::{
        consensus_metrics::{ConsensusMetricsEvent, ConsensusMetricsEventSender},
        consensus_rewards,
    },
    solana_votor_messages::{
        consensus_message::{ConsensusMessage, VoteMessage},
        reward_certificate::AddVoteMessage,
        vote::Vote,
    },
    std::{collections::HashMap, sync::atomic::Ordering, time::Instant},
    thiserror::Error,
};

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)]
pub(super) enum Error {
    #[error("channel to consensus pool disconnected")]
    ConsensusPoolChannelDisconnected,
    #[error("channel to rewards container disconnected")]
    RewardsChannelDisconnected,
    #[error("channel to repair disconnected")]
    RepairChannelDisconnected,
    #[error("channel to metrics disconnected")]
    MetricsChannelDisconnected,
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
#[derive(Clone, Debug)]
pub(super) struct VoteToVerify {
    pub vote_message: VoteMessage,
    pub bls_pubkey: BlsPubkey,
    pub pubkey: Pubkey,
}

impl VoteToVerify {
    fn verify(&self) -> bool {
        let Ok(payload) = bincode::serialize(&self.vote_message.vote) else {
            return false;
        };
        self.bls_pubkey
            .verify_signature(&self.vote_message.signature, &payload)
            .is_ok()
    }
}

/// Verifies votes and sends the verified votes to the consensus pool; and sends the desired subset
/// to rewards container and repair.
///
/// Returns the Vec of [`VoteToVerify`] to the caller to enable reuse.  The length of the returned
/// buffer might be lower than the input buffer.
#[allow(clippy::too_many_arguments)]
pub(super) fn verify_and_send_votes(
    votes_to_verify: Vec<VoteToVerify>,
    root_bank: &Bank,
    stats: &BLSSigVerifierStats,
    cluster_info: &ClusterInfo,
    leader_schedule: &LeaderScheduleCache,
    channel_to_pool: &Sender<Vec<ConsensusMessage>>,
    channel_to_repair: &VerifiedVoteSender,
    channel_to_reward: &Sender<AddVoteMessage>,
    channel_to_metrics: &ConsensusMetricsEventSender,
    last_voted_slots: &mut HashMap<Pubkey, Slot>,
) -> Result<Vec<VoteToVerify>, Error> {
    if votes_to_verify.is_empty() {
        return Ok(votes_to_verify);
    }
    stats
        .votes_to_verify
        .fetch_add(votes_to_verify.len() as u64, Ordering::Relaxed);
    stats
        .votes_to_verify_batches
        .fetch_add(1, Ordering::Relaxed);
    let verified_votes = verify_votes(votes_to_verify, stats);
    stats
        .verified_votes
        .fetch_add(verified_votes.len() as u64, Ordering::Relaxed);

    let (votes_for_pool, msgs_for_repair, msg_for_reward, msg_for_metrics) = process_verified_votes(
        &verified_votes,
        root_bank,
        cluster_info,
        leader_schedule,
        last_voted_slots,
    );

    send_votes_to_pool(votes_for_pool, channel_to_pool, stats)?;
    send_votes_to_repair(msgs_for_repair, channel_to_repair, stats)?;
    send_votes_to_rewards(msg_for_reward, channel_to_reward, stats)?;
    send_votes_to_metrics(msg_for_metrics, channel_to_metrics, stats)?;

    Ok(verified_votes)
}

fn inspect_for_repair(
    vote: &VoteToVerify,
    last_voted_slots: &mut HashMap<Pubkey, Slot>,
    msgs_for_repair: &mut HashMap<Pubkey, Vec<Slot>>,
) {
    let vote_slot = vote.vote_message.vote.slot();
    if vote.vote_message.vote.is_notarization_or_finalization() {
        last_voted_slots
            .entry(vote.pubkey)
            .and_modify(|s| *s = (*s).max(vote_slot))
            .or_insert(vote.vote_message.vote.slot());
    }

    if vote.vote_message.vote.is_notarization_or_finalization()
        || vote.vote_message.vote.is_notarize_fallback()
    {
        let slots: &mut Vec<_> = msgs_for_repair.entry(vote.pubkey).or_default();
        if !slots.contains(&vote_slot) {
            slots.push(vote_slot);
        }
    }
}

/// Processes the verified votes for various downstream services.
///
/// In particular, collects and returns the relevant messages for the consensus pool; rewards;
/// repair; and metrics;
///
/// Also updates `last_voted_slots`.
fn process_verified_votes(
    verified_votes: &[VoteToVerify],
    root_bank: &Bank,
    cluster_info: &ClusterInfo,
    leader_schedule: &LeaderScheduleCache,
    last_voted_slots: &mut HashMap<Pubkey, Slot>,
) -> (
    Vec<ConsensusMessage>,
    HashMap<Pubkey, Vec<Slot>>,
    AddVoteMessage,
    Vec<ConsensusMetricsEvent>,
) {
    let mut votes_for_reward = Vec::with_capacity(verified_votes.len());
    let mut msgs_for_repair = HashMap::new();
    let mut votes_for_pool = Vec::with_capacity(verified_votes.len());
    let mut votes_for_metrics = Vec::with_capacity(verified_votes.len());
    for vote in verified_votes {
        let vote_message = vote.vote_message;
        if consensus_rewards::wants_vote(
            cluster_info,
            leader_schedule,
            root_bank.slot(),
            &vote_message,
        ) {
            votes_for_reward.push(vote_message);
        }

        inspect_for_repair(vote, last_voted_slots, &mut msgs_for_repair);

        votes_for_pool.push(ConsensusMessage::Vote(vote_message));

        votes_for_metrics.push(ConsensusMetricsEvent::Vote {
            id: vote.pubkey,
            vote: vote.vote_message.vote,
        });
    }
    (
        votes_for_pool,
        msgs_for_repair,
        AddVoteMessage {
            votes: votes_for_reward,
        },
        votes_for_metrics,
    )
}

fn send_votes_to_metrics(
    votes: Vec<ConsensusMetricsEvent>,
    channel: &ConsensusMetricsEventSender,
    stats: &BLSSigVerifierStats,
) -> Result<(), Error> {
    let len = votes.len();
    let msg = (Instant::now(), votes);
    match channel.try_send(msg) {
        Ok(()) => {
            stats
                .verify_votes_metrics_sent
                .fetch_add(len as u64, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            stats
                .verify_votes_metrics_channel_full
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err(Error::MetricsChannelDisconnected),
    }
}

fn send_votes_to_rewards(
    msg: AddVoteMessage,
    channel: &Sender<AddVoteMessage>,
    stats: &BLSSigVerifierStats,
) -> Result<(), Error> {
    let len = msg.votes.len();
    match channel.try_send(msg) {
        Ok(()) => {
            stats
                .verify_votes_rewards_sent
                .fetch_add(len as u64, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            stats
                .verify_votes_rewards_channel_full
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err(Error::RewardsChannelDisconnected),
    }
}

fn send_votes_to_pool(
    votes: Vec<ConsensusMessage>,
    channel: &Sender<Vec<ConsensusMessage>>,
    stats: &BLSSigVerifierStats,
) -> Result<(), Error> {
    let len = votes.len();
    if len == 0 {
        return Ok(());
    }
    match channel.try_send(votes) {
        Ok(()) => {
            stats
                .verify_votes_consensus_sent
                .fetch_add(len as u64, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            stats
                .verify_votes_consensus_channel_full
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err(Error::ConsensusPoolChannelDisconnected),
    }
}

fn send_votes_to_repair(
    votes: HashMap<Pubkey, Vec<Slot>>,
    channel: &VerifiedVoteSender,
    stats: &BLSSigVerifierStats,
) -> Result<(), Error> {
    for (pubkey, slots) in votes {
        match channel.try_send((pubkey, slots)) {
            Ok(()) => {
                stats
                    .verify_votes_repair_sent
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) => {
                stats
                    .verify_votes_repair_channel_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => return Err(Error::RepairChannelDisconnected),
        }
    }
    Ok(())
}

fn verify_votes(
    votes_to_verify: Vec<VoteToVerify>,
    stats: &BLSSigVerifierStats,
) -> Vec<VoteToVerify> {
    // Try optimistic verification - fast to verify, but cannot identify invalid votes
    if verify_votes_optimistic(&votes_to_verify, stats) {
        return votes_to_verify;
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
    votes_to_verify: Vec<VoteToVerify>,
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
            Some(vote)
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
