//! The BLS signature verifier.
//! This is just a placeholder for now, until we have a real implementation.

mod stats;

use {
    crate::{
        cluster_info_vote_listener::VerifiedVoteSender,
        sigverify_stage::{SigVerifier, SigVerifyServiceError},
    },
    bitvec::prelude::{BitVec, Lsb0},
    crossbeam_channel::{Sender, TrySendError},
    itertools::multiunzip,
    rayon::iter::{
        IndexedParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator,
        ParallelIterator,
    },
    solana_bls_signatures::{
        pubkey::{Pubkey as BlsPubkey, PubkeyProjective, VerifiablePubkey},
        signature::SignatureProjective,
    },
    solana_clock::Slot,
    solana_perf::packet::PacketRefMut,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::SharableBank, epoch_stakes::BLSPubkeyToRankMap},
    solana_signer_store::decode,
    solana_streamer::packet::PacketBatch,
    solana_votor_messages::consensus_message::{
        Certificate, CertificateMessage, ConsensusMessage, VoteMessage,
    },
    stats::{BLSSigVerifierStats, StatsUpdater},
    std::{collections::HashMap, sync::Arc},
};

fn get_key_to_rank_map(bank: &Bank, slot: Slot) -> Option<&Arc<BLSPubkeyToRankMap>> {
    let stakes = bank.epoch_stakes_map();
    let epoch = bank.epoch_schedule().get_epoch(slot);
    stakes
        .get(&epoch)
        .map(|stake| stake.bls_pubkey_to_rank_map())
}

pub struct BLSSigVerifier {
    verified_votes_sender: VerifiedVoteSender,
    message_sender: Sender<ConsensusMessage>,
    root_bank: SharableBank,
    stats: BLSSigVerifierStats,
}

struct VoteToVerify<'a> {
    vote_message: VoteMessage,
    bls_pubkey: &'a BlsPubkey,
    packet: PacketRefMut<'a>,
}
struct CertToVerify<'a> {
    cert_message: CertificateMessage,
    packet: PacketRefMut<'a>,
}

impl SigVerifier for BLSSigVerifier {
    type SendType = ConsensusMessage;

    fn verify_batches(
        &self,
        mut batches: Vec<PacketBatch>,
        _valid_packets: usize,
    ) -> Vec<PacketBatch> {
        // TODO(sam): ideally we want to avoid heap allocation, but let's use
        //            `Vec` for now for clarity and then optimize for the final version
        let mut votes_to_verify = Vec::new();
        let mut certs_to_verify = Vec::new();

        let bank = self.root_bank.load();

        for mut packet in batches.iter_mut().flatten() {
            if packet.meta().discard() {
                continue;
            }

            let message: ConsensusMessage = match packet.deserialize_slice(..) {
                Ok(msg) => msg,
                Err(_) => {
                    packet.meta_mut().set_discard(true);
                    continue;
                }
            };

            match message {
                ConsensusMessage::Vote(vote_message) => {
                    if let Some(key_to_rank_map) =
                        get_key_to_rank_map(&bank, vote_message.vote.slot())
                    {
                        if let Some((_, bls_pubkey)) =
                            key_to_rank_map.get_pubkey(vote_message.rank.into())
                        {
                            votes_to_verify.push(VoteToVerify {
                                vote_message,
                                bls_pubkey,
                                packet,
                            });
                        } else {
                            packet.meta_mut().set_discard(true); // Invalid rank
                        }
                    } else {
                        packet.meta_mut().set_discard(true); // Missing epoch stakes
                    }
                }
                ConsensusMessage::Certificate(cert_message) => {
                    certs_to_verify.push(CertToVerify {
                        cert_message,
                        packet,
                    });
                }
            }
        }

        rayon::join(
            || self.verify_votes(&mut votes_to_verify),
            || self.verify_certificates(&mut certs_to_verify),
        );

        batches
    }

    fn send_packets(
        &mut self,
        packet_batches: Vec<PacketBatch>,
    ) -> Result<(), SigVerifyServiceError<Self::SendType>> {
        // TODO(wen): just a placeholder without any batching.
        let mut verified_votes = HashMap::new();
        let mut stats_updater = StatsUpdater::default();

        for packet in packet_batches.iter().flatten() {
            stats_updater.received += 1;

            if packet.meta().discard() {
                stats_updater.received_discarded += 1;
                continue;
            }

            let message = match packet.deserialize_slice(..) {
                Ok(msg) => msg,
                Err(e) => {
                    trace!("Failed to deserialize BLS message: {}", e);
                    stats_updater.received_malformed += 1;
                    continue;
                }
            };

            let slot = match &message {
                ConsensusMessage::Vote(vote_message) => vote_message.vote.slot(),
                ConsensusMessage::Certificate(certificate_message) => {
                    certificate_message.certificate.slot()
                }
            };

            let bank = self.root_bank.load();
            let Some(rank_to_pubkey_map) = get_key_to_rank_map(&bank, slot) else {
                stats_updater.received_no_epoch_stakes += 1;
                continue;
            };

            if let ConsensusMessage::Vote(vote_message) = &message {
                let vote = &vote_message.vote;
                stats_updater.received_votes += 1;
                if vote.is_notarization_or_finalization() || vote.is_notarize_fallback() {
                    let Some((pubkey, _)) = rank_to_pubkey_map.get_pubkey(vote_message.rank.into())
                    else {
                        stats_updater.received_malformed += 1;
                        continue;
                    };
                    let cur_slots: &mut Vec<Slot> = verified_votes.entry(*pubkey).or_default();
                    if !cur_slots.contains(&slot) {
                        cur_slots.push(slot);
                    }
                }
            }

            // Now send the BLS message to certificate pool.
            match self.message_sender.try_send(message) {
                Ok(()) => stats_updater.sent += 1,
                Err(TrySendError::Full(_)) => {
                    stats_updater.sent_failed += 1;
                }
                Err(e @ TrySendError::Disconnected(_)) => {
                    return Err(e.into());
                }
            }
        }
        self.send_verified_votes(verified_votes);
        self.stats.update(stats_updater);
        self.stats.maybe_report_stats();
        Ok(())
    }
}

impl BLSSigVerifier {
    pub fn new(
        root_bank: SharableBank,
        verified_votes_sender: VerifiedVoteSender,
        message_sender: Sender<ConsensusMessage>,
    ) -> Self {
        Self {
            root_bank,
            verified_votes_sender,
            message_sender,
            stats: BLSSigVerifierStats::new(),
        }
    }

    fn send_verified_votes(&mut self, verified_votes: HashMap<Pubkey, Vec<Slot>>) {
        let mut stats_updater = StatsUpdater::default();
        for (pubkey, slots) in verified_votes {
            match self.verified_votes_sender.try_send((pubkey, slots)) {
                Ok(()) => {
                    stats_updater.verified_votes_sent += 1;
                }
                Err(e) => {
                    trace!("Failed to send verified vote: {}", e);
                    stats_updater.verified_votes_sent_failed += 1;
                }
            }
        }
        self.stats.update(stats_updater);
    }

    fn verify_votes(&self, votes_to_verify: &mut [VoteToVerify]) {
        if votes_to_verify.is_empty() {
            return;
        }

        let (mut successful_votes, messages): (Vec<_>, Vec<_>) =
            multiunzip(votes_to_verify.iter_mut().filter_map(|vote| {
                match bincode::serialize(&vote.vote_message.vote) {
                    Ok(payload) => Some((vote, payload)),
                    Err(_) => {
                        vote.packet.meta_mut().set_discard(true);
                        None
                    }
                }
            }));

        if successful_votes.is_empty() {
            return;
        }

        let mut pubkeys = Vec::with_capacity(successful_votes.len());
        let mut signatures = Vec::with_capacity(successful_votes.len());
        let mut message_refs = Vec::with_capacity(successful_votes.len());

        for (vote, message) in successful_votes.iter().zip(messages.iter()) {
            pubkeys.push(vote.bls_pubkey);
            signatures.push(&vote.vote_message.signature);
            message_refs.push(message.as_ref());
        }

        // Optimistically verify signatures; this should be the most common case
        if SignatureProjective::verify_distinct(&pubkeys, &signatures, &message_refs)
            .unwrap_or(false)
        {
            return;
        }

        // Fallback: If the batch fails, verify each vote signature individually in parallel
        // to find the invalid ones.
        //
        // TODO(sam): keep a record of which validator's vote failed to incur penalty
        successful_votes
            .par_iter_mut()
            .zip(messages.par_iter())
            .for_each(|(vote_to_verify, signed_payload)| {
                if !vote_to_verify
                    .bls_pubkey
                    .verify_signature(&vote_to_verify.vote_message.signature, signed_payload)
                    .unwrap_or(false)
                {
                    vote_to_verify.packet.meta_mut().set_discard(true);
                }
            });
    }

    fn verify_certificates(&self, certs_to_verify: &mut [CertToVerify]) {
        certs_to_verify.par_iter_mut().for_each(|cert_to_verify| {
            if !self.verify_bls_certificate(&cert_to_verify.cert_message) {
                cert_to_verify.packet.meta_mut().set_discard(true);
            }
        });
    }

    fn verify_bls_certificate(&self, certificate_message: &CertificateMessage) -> bool {
        let signed_payload = match bincode::serialize(&certificate_message.certificate) {
            Ok(payload) => payload,
            Err(_) => return false,
        };

        let bank = self.root_bank.load();
        let Some(key_to_rank_map) =
            get_key_to_rank_map(&bank, certificate_message.certificate.slot())
        else {
            return false;
        };

        let max_len = key_to_rank_map.len();

        let decoded_bitmap = match decode(&certificate_message.bitmap, max_len) {
            Ok(decoded) => decoded,
            Err(_) => return false,
        };

        match decoded_bitmap {
            solana_signer_store::Decoded::Base2(bit_vec) => {
                let pubkeys_to_aggregate = match bit_vec
                    .iter_ones()
                    .filter_map(|rank| key_to_rank_map.get_pubkey(rank))
                    .map(|(_, bls_pubkey)| PubkeyProjective::try_from(bls_pubkey))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(pks) => pks,
                    Err(_) => return false,
                };
                let pubkey_refs: Vec<_> = pubkeys_to_aggregate.iter().collect();
                let Ok(aggregate_bls_pubkey) = PubkeyProjective::aggregate(&pubkey_refs) else {
                    return false;
                };

                aggregate_bls_pubkey
                    .verify_signature(&certificate_message.signature, &signed_payload)
                    .unwrap_or(false)
            }
            solana_signer_store::Decoded::Base3(bit_vec1, bit_vec2) => {
                // Helper closure to avoid duplicating the aggregation logic.
                let aggregate_from_bitmap = |bit_vec: &BitVec<u8, Lsb0>| {
                    let pubkeys: Result<Vec<_>, _> = bit_vec
                        .iter_ones()
                        .filter_map(|rank| key_to_rank_map.get_pubkey(rank))
                        .map(|(_, bls_pubkey)| PubkeyProjective::try_from(*bls_pubkey))
                        .collect();
                    PubkeyProjective::aggregate(&pubkeys?.iter().collect::<Vec<_>>())
                };

                // 1. Aggregate the two sets of public keys separately.
                let Ok(agg_pk1) = aggregate_from_bitmap(&bit_vec1) else {
                    return false;
                };
                let Ok(agg_pk2) = aggregate_from_bitmap(&bit_vec2) else {
                    return false;
                };

                let pubkeys_affine: Vec<BlsPubkey> = vec![agg_pk1.into(), agg_pk2.into()];
                let pubkey_refs: Vec<&BlsPubkey> = pubkeys_affine.iter().collect();

                // 2. Construct the two messages based on the certificate type.
                let messages_to_verify: Vec<Vec<u8>> = {
                    let certificate = &certificate_message.certificate;
                    match certificate {
                        Certificate::NotarizeFallback(slot, hash) => {
                            let msg1 =
                                bincode::serialize(&Certificate::Notarize(*slot, *hash)).ok()?;
                            let msg2 = bincode::serialize(certificate).ok()?;
                            vec![msg1, msg2]
                        }
                        Certificate::Skip(slot) => {
                            // For a Skip, the fallback group signs for a NotarizeFallback on the parent.
                            let parent_hash = bank.parent_hash();
                            let msg1 = bincode::serialize(certificate).ok()?;
                            let msg2 = bincode::serialize(&Certificate::NotarizeFallback(
                                *slot,
                                parent_hash,
                            ))
                            .ok()?;
                            vec![msg1, msg2]
                        }
                        // As per your logic, other certificate types are not valid for Base3.
                        _ => return false,
                    }
                };

                // If message serialization failed, `messages_to_verify` will be None.
                let Some(messages_to_verify) = Some(messages_to_verify) else {
                    return false;
                };
                let message_refs: Vec<&[u8]> =
                    messages_to_verify.iter().map(AsRef::as_ref).collect();

                // 3. Verify the one aggregate signature against the two aggregate pubkeys and two messages.
                SignatureProjective::verify_distinct_aggregated(
                    &pubkey_refs,
                    &certificate_message.signature,
                    &message_refs,
                )
                .unwrap_or(false)
            }
        }
    }
}

// Add tests for the BLS signature verifier
#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::Receiver,
        solana_bls_signatures::Signature,
        solana_hash::Hash,
        solana_perf::packet::{Packet, PinnedPacketBatch},
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            genesis_utils::{
                create_genesis_config_with_alpenglow_vote_accounts_no_program,
                ValidatorVoteKeypairs,
            },
        },
        solana_signer::Signer,
        solana_votor_messages::{
            consensus_message::{
                Certificate, CertificateMessage, CertificateType, ConsensusMessage, VoteMessage,
            },
            vote::Vote,
        },
        stats::STATS_INTERVAL_DURATION,
        std::time::{Duration, Instant},
    };

    fn create_keypairs_and_bls_sig_verifier(
        verified_vote_sender: VerifiedVoteSender,
        message_sender: Sender<ConsensusMessage>,
    ) -> (Vec<ValidatorVoteKeypairs>, BLSSigVerifier) {
        // Create 10 node validatorvotekeypairs vec
        let validator_keypairs = (0..10)
            .map(|_| ValidatorVoteKeypairs::new_rand())
            .collect::<Vec<_>>();
        let stakes_vec = (0..validator_keypairs.len())
            .map(|i| 1_000 - i as u64)
            .collect::<Vec<_>>();
        let genesis = create_genesis_config_with_alpenglow_vote_accounts_no_program(
            1_000_000_000,
            &validator_keypairs,
            stakes_vec,
        );
        let bank0 = Bank::new_for_tests(&genesis.genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank0);
        let root_bank = bank_forks.read().unwrap().sharable_root_bank();
        (
            validator_keypairs,
            BLSSigVerifier::new(root_bank, verified_vote_sender, message_sender),
        )
    }

    fn test_bls_message_transmission(
        verifier: &mut BLSSigVerifier,
        receiver: Option<&Receiver<ConsensusMessage>>,
        messages: &[ConsensusMessage],
        expect_send_packets_ok: bool,
    ) {
        let packets = messages
            .iter()
            .map(|msg| {
                let mut packet = Packet::default();
                packet
                    .populate_packet(None, msg)
                    .expect("Failed to populate packet");
                packet
            })
            .collect::<Vec<Packet>>();
        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        if expect_send_packets_ok {
            assert!(verifier.send_packets(packet_batches).is_ok());
            if let Some(receiver) = receiver {
                for msg in messages {
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(received_msg) => assert_eq!(received_msg, *msg),
                        Err(e) => warn!("Failed to receive BLS message: {}", e),
                    }
                }
            }
        } else {
            assert!(verifier.send_packets(packet_batches).is_err());
        }
    }

    #[test]
    fn test_blssigverifier_send_packets() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let (verified_vote_sender, verfied_vote_receiver) = crossbeam_channel::unbounded();
        // Create bank forks and epoch stakes

        let (validator_keypairs, mut verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);

        // TODO(wen): this is just a fake map, when we add verification we should use real map
        let mut bitmap = vec![0; 5];
        bitmap[1] = 255;
        bitmap[4] = 10;
        let vote_rank: usize = 2;

        let certificate = Certificate::new(CertificateType::Finalize, 4, None);

        let messages = vec![
            ConsensusMessage::Vote(VoteMessage {
                vote: Vote::new_finalization_vote(5),
                signature: Signature::default(),
                rank: vote_rank as u16,
            }),
            ConsensusMessage::Certificate(CertificateMessage {
                certificate,
                signature: Signature::default(),
                bitmap,
            }),
        ];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        assert_eq!(verifier.stats.sent, 2);
        assert_eq!(verifier.stats.received, 2);
        assert_eq!(verifier.stats.received_malformed, 0);
        let received_verified_votes = verfied_vote_receiver.try_recv().unwrap();
        assert_eq!(
            received_verified_votes,
            (validator_keypairs[vote_rank].vote_keypair.pubkey(), vec![5])
        );

        let vote_rank: usize = 3;
        let messages = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_notarization_vote(6, Hash::new_unique()),
            signature: Signature::default(),
            rank: vote_rank as u16,
        })];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        assert_eq!(verifier.stats.sent, 3);
        assert_eq!(verifier.stats.received, 3);
        assert_eq!(verifier.stats.received_malformed, 0);
        let received_verified_votes = verfied_vote_receiver.try_recv().unwrap();
        assert_eq!(
            received_verified_votes,
            (validator_keypairs[vote_rank].vote_keypair.pubkey(), vec![6])
        );

        // Pretend 10 seconds have passed, make sure stats are reset
        verifier.stats.last_stats_logged = Instant::now() - STATS_INTERVAL_DURATION;
        let vote_rank: usize = 9;
        let messages = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_notarization_fallback_vote(7, Hash::new_unique()),
            signature: Signature::default(),
            rank: vote_rank as u16,
        })];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);
        // Since we just logged all stats (including the packet just sent), stats should be reset
        assert_eq!(verifier.stats.sent, 0);
        assert_eq!(verifier.stats.received, 0);
        assert_eq!(verifier.stats.received_malformed, 0);
        let received_verified_votes = verfied_vote_receiver.try_recv().unwrap();
        assert_eq!(
            received_verified_votes,
            (validator_keypairs[vote_rank].vote_keypair.pubkey(), vec![7])
        );
    }

    #[test]
    fn test_blssigverifier_send_packets_malformed() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);

        let packets = vec![Packet::default()];
        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        assert!(verifier.send_packets(packet_batches).is_ok());
        assert_eq!(verifier.stats.sent, 0);
        assert_eq!(verifier.stats.received, 1);
        assert_eq!(verifier.stats.received_malformed, 1);
        assert_eq!(verifier.stats.received_no_epoch_stakes, 0);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());

        // Send a packet with no epoch stakes
        let messages = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5_000_000_000),
            signature: Signature::default(),
            rank: 0,
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, true);
        assert_eq!(verifier.stats.sent, 0);
        assert_eq!(verifier.stats.received, 2);
        assert_eq!(verifier.stats.received_malformed, 1);
        assert_eq!(verifier.stats.received_no_epoch_stakes, 1);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());

        // Send a packet with invalid rank
        let messages = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 1000, // Invalid rank
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, true);
        assert_eq!(verifier.stats.sent, 0);
        assert_eq!(verifier.stats.received, 3);
        assert_eq!(verifier.stats.received_malformed, 2);
        assert_eq!(verifier.stats.received_no_epoch_stakes, 1);

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());
    }

    #[test]
    fn test_blssigverifier_send_packets_channel_full() {
        solana_logger::setup();
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);
        let messages = vec![
            ConsensusMessage::Vote(VoteMessage {
                vote: Vote::new_finalization_vote(5),
                signature: Signature::default(),
                rank: 0,
            }),
            ConsensusMessage::Vote(VoteMessage {
                vote: Vote::new_notarization_fallback_vote(6, Hash::new_unique()),
                signature: Signature::default(),
                rank: 2,
            }),
        ];
        test_bls_message_transmission(&mut verifier, Some(&receiver), &messages, true);

        // We failed to send the second message because the channel is full.
        assert_eq!(verifier.stats.sent, 1);
        assert_eq!(verifier.stats.received, 2);
        assert_eq!(verifier.stats.received_malformed, 0);
    }

    #[test]
    fn test_blssigverifier_send_packets_receiver_closed() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);
        // Close the receiver, should get panic on next send
        drop(receiver);
        let messages = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 0,
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, false);
    }

    #[test]
    fn test_blssigverifier_send_discarded_packets() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);
        let message = ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 0,
        });
        let mut packet = Packet::default();
        packet
            .populate_packet(None, &message)
            .expect("Failed to populate packet");
        packet.meta_mut().set_discard(true);
        let packets = vec![packet];
        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        assert!(verifier.send_packets(packet_batches).is_ok());
        assert_eq!(verifier.stats.sent, 0);
        assert_eq!(verifier.stats.sent_failed, 0);
        assert_eq!(verifier.stats.verified_votes_sent, 0);
        assert_eq!(verifier.stats.verified_votes_sent_failed, 0);
        assert_eq!(verifier.stats.received, 1);
        assert_eq!(verifier.stats.received_discarded, 1);
        assert_eq!(verifier.stats.received_malformed, 0);
        assert_eq!(verifier.stats.received_no_epoch_stakes, 0);
        assert_eq!(verifier.stats.received_votes, 0);
        assert!(receiver.is_empty());
    }
}
