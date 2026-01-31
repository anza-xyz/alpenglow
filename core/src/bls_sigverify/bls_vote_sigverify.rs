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
    },
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_measure::measure::Measure,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_votor::consensus_rewards,
    solana_votor_messages::{
        consensus_message::{ConsensusMessage, VoteMessage},
        reward_certificate::AddVoteMessage,
        vote::Vote,
    },
    std::{
        collections::HashMap,
        sync::{atomic::Ordering, Arc, RwLock},
    },
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct VoteToVerify {
    pub vote_message: VoteMessage,
    pub bls_pubkey: BlsPubkey,
    pub pubkey: Pubkey,
}

/// Verifies votes and sends verified votes to the consensus pool.
/// Also returns a copy of the verified votes that the rewards container is interested is so that the caller can send them to it.
pub(crate) fn verify_and_send_votes(
    votes_to_verify: &[VoteToVerify],
    root_bank: &Bank,
    vote_payload_cache: &RwLock<HashMap<Vote, Arc<Vec<u8>>>>,
    stats: &BLSSigVerifierStats,
    cluster_info: &ClusterInfo,
    leader_schedule: &LeaderScheduleCache,
    message_sender: &Sender<ConsensusMessage>,
    votes_for_repair_sender: &VerifiedVoteSender,
) -> Result<AddVoteMessage, BLSSigVerifyError> {
    let verified_votes = verify_votes(votes_to_verify, vote_payload_cache, stats);

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
    vote_payload_cache: &RwLock<HashMap<Vote, Arc<Vec<u8>>>>,
    stats: &BLSSigVerifierStats,
) -> Vec<VoteToVerify> {
    if votes_to_verify.is_empty() {
        return vec![];
    }

    stats.votes_batch_count.fetch_add(1, Ordering::Relaxed);
    let mut votes_batch_optimistic_time = Measure::start("votes_batch_optimistic");

    let payloads = votes_to_verify
        .iter()
        .map(|v| get_vote_payload(&v.vote_message.vote, vote_payload_cache))
        .collect::<Vec<_>>();
    let mut grouped_pubkeys: HashMap<&Arc<Vec<u8>>, Vec<&BlsPubkey>> = HashMap::new();
    for (v, payload) in votes_to_verify.iter().zip(payloads.iter()) {
        grouped_pubkeys
            .entry(payload)
            .or_default()
            .push(&v.bls_pubkey);
    }

    let distinct_messages = grouped_pubkeys.len();
    stats
        .votes_batch_distinct_messages_count
        .fetch_add(distinct_messages as u64, Ordering::Relaxed);

    let (distinct_payloads, distinct_pubkeys): (Vec<_>, Vec<_>) =
        grouped_pubkeys.into_iter().unzip();
    let aggregate_pubkeys_result: Result<Vec<PubkeyProjective>, _> = distinct_pubkeys
        .into_iter()
        .map(|pks| PubkeyProjective::par_aggregate(pks.into_par_iter()))
        .collect();

    let verified_optimistically = if let Ok(aggregate_pubkeys) = aggregate_pubkeys_result {
        let signatures = votes_to_verify
            .par_iter()
            .map(|v| &v.vote_message.signature);
        if let Ok(aggregate_signature) = SignatureProjective::par_aggregate(signatures) {
            if distinct_messages == 1 {
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
        }
    } else {
        // Public key aggregation failed.
        false
    };

    if verified_optimistically {
        votes_batch_optimistic_time.stop();
        stats
            .votes_batch_optimistic_elapsed_us
            .fetch_add(votes_batch_optimistic_time.as_us(), Ordering::Relaxed);
        return votes_to_verify.to_vec();
    }

    // Fallback: If the batch fails, verify each vote signature individually in parallel
    // to find the invalid ones.
    //
    // TODO(sam): keep a record of which validator's vote failed to incur penalty
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

fn get_vote_payload(
    vote: &Vote,
    vote_payload_cache: &RwLock<HashMap<Vote, Arc<Vec<u8>>>>,
) -> Arc<Vec<u8>> {
    let read_cache = vote_payload_cache.read().unwrap();
    if let Some(payload) = read_cache.get(vote) {
        return payload.clone();
    }
    drop(read_cache);

    // Not in cache, so get a write lock
    let mut write_cache = vote_payload_cache.write().unwrap();
    if let Some(payload) = write_cache.get(vote) {
        return payload.clone();
    }

    let payload = Arc::new(bincode::serialize(vote).expect("Failed to serialize vote"));
    write_cache.insert(*vote, payload.clone());
    payload
}
