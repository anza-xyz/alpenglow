//! The BLS signature verifier.

mod stats;

use {
    crate::{
        cluster_info_vote_listener::VerifiedVoteSender,
        sigverify_stage::{SigVerifier, SigVerifyServiceError},
    },
    bitvec::prelude::{BitVec, Lsb0},
    crossbeam_channel::{Sender, TrySendError},
    itertools::Itertools,
    rayon::iter::{IntoParallelRefMutIterator, ParallelIterator},
    solana_bls_signatures::{
        pubkey::{Pubkey as BlsPubkey, PubkeyProjective, VerifiablePubkey},
        signature::{Signature as BlsSignature, SignatureProjective},
    },
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_perf::packet::PacketRefMut,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::SharableBank, epoch_stakes::BLSPubkeyToRankMap},
    solana_signer_store::{decode, DecodeError},
    solana_streamer::packet::PacketBatch,
    solana_votor_messages::{
        consensus_message::{
            Certificate, CertificateMessage, CertificateType, ConsensusMessage, VoteMessage,
        },
        vote::Vote,
    },
    stats::BLSSigVerifierStats,
    std::{
        collections::HashMap,
        sync::{atomic::Ordering, Arc},
    },
    thiserror::Error,
};

// TODO(sam): We deserialize the packets twice: in `verify_batches` and `send_packets`.

fn get_key_to_rank_map(bank: &Bank, slot: Slot) -> Option<&Arc<BLSPubkeyToRankMap>> {
    let stakes = bank.epoch_stakes_map();
    let epoch = bank.epoch_schedule().get_epoch(slot);
    stakes
        .get(&epoch)
        .map(|stake| stake.bls_pubkey_to_rank_map())
}

fn aggregate_keys_from_bitmap(
    bit_vec: &BitVec<u8, Lsb0>,
    key_to_rank_map: &Arc<BLSPubkeyToRankMap>,
) -> Option<PubkeyProjective> {
    let pubkeys: Result<Vec<_>, _> = bit_vec
        .iter_ones()
        .filter_map(|rank| key_to_rank_map.get_pubkey(rank))
        .map(|(_, bls_pubkey)| PubkeyProjective::try_from(*bls_pubkey))
        .collect();
    let pubkeys = pubkeys.ok()?;
    PubkeyProjective::par_aggregate(&pubkeys.iter().collect::<Vec<_>>()).ok()
}

#[derive(Debug, Error, PartialEq)]
enum CertVerifyError {
    #[error("Failed to find key to rank map for slot {0}")]
    KeyToRankMapNotFound(Slot),

    #[error("Failed to decode bitmap {0:?}")]
    BitmapDecodingFailed(DecodeError),

    #[error("Failed to aggregate public keys")]
    KeyAggregationFailed,

    #[error("Failed to serialize original vote")]
    SerializationFailed,

    #[error("The signature doesn't match")]
    SignatureVerificationFailed,

    #[error("Base 3 encoding on unexpected cert {0:?}")]
    Base3EncodingOnUnexpectedCert(CertificateType),
}

pub struct BLSSigVerifier {
    verified_votes_sender: VerifiedVoteSender,
    message_sender: Sender<ConsensusMessage>,
    root_bank: SharableBank,
    stats: BLSSigVerifierStats,
}

impl SigVerifier for BLSSigVerifier {
    type SendType = ConsensusMessage;

    fn verify_batches(
        &self,
        mut batches: Vec<PacketBatch>,
        _valid_packets: usize,
    ) -> Vec<PacketBatch> {
        let mut preprocess_time = Measure::start("preprocess");
        // TODO(sam): ideally we want to avoid heap allocation, but let's use
        //            `Vec` for now for clarity and then optimize for the final version
        let mut votes_to_verify = Vec::new();
        let mut certs_to_verify = Vec::new();

        let bank = self.root_bank.load();
        for mut packet in batches.iter_mut().flatten() {
            self.stats.received.fetch_add(1, Ordering::Relaxed);
            if packet.meta().discard() {
                self.stats
                    .received_discarded
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let message: ConsensusMessage = match packet.deserialize_slice(..) {
                Ok(msg) => msg,
                Err(_) => {
                    self.stats
                        .received_malformed
                        .fetch_add(1, Ordering::Relaxed);
                    packet.meta_mut().set_discard(true);
                    continue;
                }
            };

            match message {
                ConsensusMessage::Vote(vote_message) => {
                    // Missing epoch states
                    let Some(key_to_rank_map) =
                        get_key_to_rank_map(&bank, vote_message.vote.slot())
                    else {
                        self.stats
                            .received_no_epoch_stakes
                            .fetch_add(1, Ordering::Relaxed);
                        packet.meta_mut().set_discard(true);
                        continue;
                    };

                    // Invalid rank
                    let Some((_, bls_pubkey)) =
                        key_to_rank_map.get_pubkey(vote_message.rank.into())
                    else {
                        self.stats.received_bad_rank.fetch_add(1, Ordering::Relaxed);
                        packet.meta_mut().set_discard(true);
                        continue;
                    };

                    votes_to_verify.push(VoteToVerify {
                        vote_message,
                        bls_pubkey,
                        packet,
                    });
                }
                ConsensusMessage::Certificate(cert_message) => {
                    certs_to_verify.push(CertToVerify {
                        cert_message,
                        packet,
                    });
                }
            }
        }
        preprocess_time.stop();
        self.stats.preprocess_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .preprocess_elapsed_us
            .fetch_add(preprocess_time.as_us(), Ordering::Relaxed);

        rayon::join(
            || self.verify_votes(&mut votes_to_verify),
            || self.verify_certificates(&mut certs_to_verify, &bank),
        );
        batches
    }

    fn send_packets(
        &mut self,
        packet_batches: Vec<PacketBatch>,
    ) -> Result<(), SigVerifyServiceError<Self::SendType>> {
        let mut verified_votes = HashMap::new();
        for packet in packet_batches.iter().flatten() {
            if packet.meta().discard() {
                continue;
            }

            let message = match packet.deserialize_slice(..) {
                Ok(msg) => msg,
                Err(e) => {
                    error!("Failed to deserialize BLS message: {e}, should not happen because verification succeeded");
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
                error!("This should not happen because verification succeeded");
                continue;
            };

            if let ConsensusMessage::Vote(vote_message) = &message {
                let vote = &vote_message.vote;
                self.stats.received_votes.fetch_add(1, Ordering::Relaxed);
                if vote.is_notarization_or_finalization() || vote.is_notarize_fallback() {
                    let Some((pubkey, _)) = rank_to_pubkey_map.get_pubkey(vote_message.rank.into())
                    else {
                        self.stats
                            .received_malformed
                            .fetch_add(1, Ordering::Relaxed);
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
                Ok(()) => {
                    self.stats.sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Full(_)) => {
                    self.stats.sent_failed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e @ TrySendError::Disconnected(_)) => {
                    return Err(e.into());
                }
            }
        }
        self.send_verified_votes(verified_votes);
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
        for (pubkey, slots) in verified_votes {
            match self.verified_votes_sender.try_send((pubkey, slots)) {
                Ok(()) => {
                    self.stats
                        .verified_votes_sent
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    trace!("Failed to send verified vote: {e}");
                    self.stats
                        .verified_votes_sent_failed
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn verify_votes(&self, votes_to_verify: &mut [VoteToVerify]) {
        if votes_to_verify.is_empty() {
            return;
        }

        self.stats.votes_batch_count.fetch_add(1, Ordering::Relaxed);
        let mut votes_batch_optimistic_time = Measure::start("votes_batch_optimistic");
        let (pubkeys, signatures, messages): (Vec<_>, Vec<_>, Vec<_>) = votes_to_verify
            .iter()
            .map(|v| (v.bls_pubkey, &v.vote_message.signature, v.payload_slice()))
            .multiunzip();

        // Optimistically verify signatures; this should be the most common case
        if SignatureProjective::par_verify_distinct(&pubkeys, &signatures, &messages)
            .unwrap_or(false)
        {
            votes_batch_optimistic_time.stop();
            self.stats
                .votes_batch_optimistic_elapsed_us
                .fetch_add(votes_batch_optimistic_time.as_us(), Ordering::Relaxed);
            return;
        }
        // Fallback: If the batch fails, verify each vote signature individually in parallel
        // to find the invalid ones.
        //
        // TODO(sam): keep a record of which validator's vote failed to incur penalty
        let mut votes_batch_parallel_verify_time = Measure::start("votes_batch_parallel_verify");
        votes_to_verify.par_iter_mut().for_each(|vote_to_verify| {
            if !vote_to_verify
                .bls_pubkey
                .verify_signature(
                    &vote_to_verify.vote_message.signature,
                    vote_to_verify.payload_slice(),
                )
                .unwrap_or(false)
            {
                self.stats
                    .received_bad_signature_votes
                    .fetch_add(1, Ordering::Relaxed);
                vote_to_verify.packet.meta_mut().set_discard(true);
            }
        });
        votes_batch_parallel_verify_time.stop();
        self.stats
            .votes_batch_parallel_verify_count
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .votes_batch_parallel_verify_elapsed_us
            .fetch_add(votes_batch_parallel_verify_time.as_us(), Ordering::Relaxed);
    }

    fn verify_certificates(&self, certs_to_verify: &mut [CertToVerify], bank: &Bank) {
        if certs_to_verify.is_empty() {
            return;
        }
        self.stats.certs_batch_count.fetch_add(1, Ordering::Relaxed);
        let mut certs_batch_verify_time = Measure::start("certs_batch_verify");
        certs_to_verify.par_iter_mut().for_each(|cert_to_verify| {
            if let Err(e) = self.verify_bls_certificate(cert_to_verify, bank) {
                trace!(
                    "Failed to verify BLS certificate: {:?}, error: {e}",
                    cert_to_verify.cert_message.certificate
                );
                self.stats
                    .received_bad_signature_certs
                    .fetch_add(1, Ordering::Relaxed);
                cert_to_verify.packet.meta_mut().set_discard(true);
            }
        });
        certs_batch_verify_time.stop();
        self.stats
            .certs_batch_elapsed_us
            .fetch_add(certs_batch_verify_time.as_us(), Ordering::Relaxed);
    }

    fn verify_bls_certificate(
        &self,
        cert_to_verify: &CertToVerify,
        bank: &Bank,
    ) -> Result<(), CertVerifyError> {
        let slot = cert_to_verify.cert_message.certificate.slot();
        let Some(key_to_rank_map) = get_key_to_rank_map(bank, slot) else {
            return Err(CertVerifyError::KeyToRankMapNotFound(slot));
        };

        let max_len = key_to_rank_map.len();

        let decoded_bitmap = match decode(&cert_to_verify.cert_message.bitmap, max_len) {
            Ok(decoded) => decoded,
            Err(e) => {
                return Err(CertVerifyError::BitmapDecodingFailed(e));
            }
        };

        match decoded_bitmap {
            solana_signer_store::Decoded::Base2(bit_vec) => {
                self.verify_base2_certificate(cert_to_verify, &bit_vec, key_to_rank_map)
            }
            solana_signer_store::Decoded::Base3(bit_vec1, bit_vec2) => {
                self.verify_base3_certificate(cert_to_verify, &bit_vec1, &bit_vec2, key_to_rank_map)
            }
        }
    }

    fn verify_base2_certificate(
        &self,
        cert_to_verify: &CertToVerify,
        bit_vec: &BitVec<u8, Lsb0>,
        key_to_rank_map: &Arc<BLSPubkeyToRankMap>,
    ) -> Result<(), CertVerifyError> {
        let original_vote =
            certificate_to_vote_message_base2(&cert_to_verify.cert_message.certificate);

        let Ok(signed_payload) = bincode::serialize(&original_vote) else {
            return Err(CertVerifyError::SerializationFailed);
        };

        let Some(aggregate_bls_pubkey) = aggregate_keys_from_bitmap(bit_vec, key_to_rank_map)
        else {
            return Err(CertVerifyError::KeyAggregationFailed);
        };

        if let Ok(true) = aggregate_bls_pubkey
            .verify_signature(&cert_to_verify.cert_message.signature, &signed_payload)
        {
            Ok(())
        } else {
            Err(CertVerifyError::SignatureVerificationFailed)
        }
    }

    fn verify_base3_certificate(
        &self,
        cert_to_verify: &CertToVerify,
        bit_vec1: &BitVec<u8, Lsb0>,
        bit_vec2: &BitVec<u8, Lsb0>,
        key_to_rank_map: &Arc<BLSPubkeyToRankMap>,
    ) -> Result<(), CertVerifyError> {
        let Some((vote1, vote2)) =
            certificate_to_vote_messages_base3(&cert_to_verify.cert_message.certificate)
        else {
            return Err(CertVerifyError::Base3EncodingOnUnexpectedCert(
                cert_to_verify.cert_message.certificate.certificate_type(),
            ));
        };

        let Ok(signed_payload1) = bincode::serialize(&vote1) else {
            return Err(CertVerifyError::SerializationFailed);
        };
        let Ok(signed_payload2) = bincode::serialize(&vote2) else {
            return Err(CertVerifyError::SerializationFailed);
        };

        let messages_to_verify: Vec<&[u8]> = vec![&signed_payload1, &signed_payload2];

        // Aggregate the two sets of public keys separately from the two bitmaps.
        let Some(agg_pk1) = aggregate_keys_from_bitmap(bit_vec1, key_to_rank_map) else {
            return Err(CertVerifyError::KeyAggregationFailed);
        };
        let Some(agg_pk2) = aggregate_keys_from_bitmap(bit_vec2, key_to_rank_map) else {
            return Err(CertVerifyError::KeyAggregationFailed);
        };

        let pubkeys_affine: Vec<BlsPubkey> = vec![agg_pk1.into(), agg_pk2.into()];
        let pubkey_refs: Vec<&BlsPubkey> = pubkeys_affine.iter().collect();

        match SignatureProjective::par_verify_distinct_aggregated(
            &pubkey_refs,
            &cert_to_verify.cert_message.signature,
            &messages_to_verify,
        ) {
            Ok(true) => Ok(()),
            _ => Err(CertVerifyError::SignatureVerificationFailed),
        }
    }
}

#[derive(Debug)]
struct VoteToVerify<'a> {
    vote_message: VoteMessage,
    bls_pubkey: &'a BlsPubkey,
    packet: PacketRefMut<'a>,
}

impl VoteToVerify<'_> {
    fn payload_slice(&self) -> &[u8] {
        // TODO(sam): This logic of extracting the message payload for signature verification
        //            is brittle, but another bincode serialization would be wasteful.
        //            Revisit this to figure out the best way to handle this.

        // TODO(sam): It is very likely that we receive the same kind of votes in batches, so
        //            consider caching.

        // The bincode serialization of `ConsensusMessage::Vote(VoteMessage)` has the layout:
        // [ 4-byte Enum Discriminant | Vote Payload | 96-byte Signature | 2-byte Rank ]
        const BINCODE_ENUM_DISCRIMINANT_SIZE: usize = 4;

        // Calculate the end of the `Vote Payload` by subtracting the known size of the trailing
        // fields (Signature and Rank) from the total packet size
        let vote_payload_end = self
            .packet
            .meta()
            .size
            .saturating_sub(std::mem::size_of::<BlsSignature>())
            .saturating_sub(std::mem::size_of::<u16>());

        &self.packet.data(..).unwrap()[BINCODE_ENUM_DISCRIMINANT_SIZE..vote_payload_end]
    }
}

struct CertToVerify<'a> {
    cert_message: CertificateMessage,
    packet: PacketRefMut<'a>,
}

// TODO(sam): These functions should probably live inside the votor or votor-messages crate
fn certificate_to_vote_message_base2(certificate: &Certificate) -> Vote {
    match certificate {
        Certificate::Notarize(slot, hash) => Vote::new_notarization_vote(*slot, *hash),

        Certificate::FinalizeFast(slot, hash) => Vote::new_notarization_vote(*slot, *hash),

        Certificate::Finalize(slot) => Vote::new_finalization_vote(*slot),

        Certificate::NotarizeFallback(slot, hash) => {
            // In the Base2 path, a NotarizeFallback certificate must have been formed
            // exclusively from Notarize votes, which populate the first bitmap
            Vote::new_notarization_vote(*slot, *hash)
        }

        Certificate::Skip(slot) => {
            // In the Base2 path, a Skip certificate must have been formed
            // exclusively from Skip votes, which populate the first bitmap
            Vote::new_skip_vote(*slot)
        }
    }
}

fn certificate_to_vote_messages_base3(certificate: &Certificate) -> Option<(Vote, Vote)> {
    match certificate {
        Certificate::NotarizeFallback(slot, hash) => {
            // Per the protocol, the first bitmap is for `Notarize` votes and the
            // second is for `NotarizeFallback` votes
            let vote1 = Vote::new_notarization_vote(*slot, *hash);
            let vote2 = Vote::new_notarization_fallback_vote(*slot, *hash);
            Some((vote1, vote2))
        }

        Certificate::Skip(slot) => {
            // Per the protocol, the first bitmap is for `Skip` votes and the
            // second is for `SkipFallback` votes
            let vote1 = Vote::new_skip_vote(*slot);
            let vote2 = Vote::new_skip_fallback_vote(*slot);
            Some((vote1, vote2))
        }

        // Other certificate types do not use Base3 encoding.
        _ => None,
    }
}

// Add tests for the BLS signature verifier
#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::Receiver,
        solana_bls_signatures::{Signature, Signature as BLSSignature},
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
        solana_signer_store::encode_base2,
        solana_votor::consensus_pool::vote_certificate_builder::VoteCertificateBuilder,
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
        verifier.verify_batches(packet_batches.clone(), 0);
        if expect_send_packets_ok {
            assert!(verifier.send_packets(packet_batches).is_ok());
            if let Some(receiver) = receiver {
                for msg in messages {
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(received_msg) => assert_eq!(received_msg, *msg),
                        Err(e) => warn!("Failed to receive BLS message: {e}"),
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
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 2);
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 2);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 0);
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
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 3);
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 3);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 0);
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
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 0);
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 0);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 0);
        let received_verified_votes = verfied_vote_receiver.try_recv().unwrap();
        assert_eq!(
            received_verified_votes,
            (validator_keypairs[vote_rank].vote_keypair.pubkey(), vec![7])
        );
    }

    #[test]
    fn test_blssigverifier_verify_malformed() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (_, mut verifier) = create_keypairs_and_bls_sig_verifier(verified_vote_sender, sender);

        let packets = vec![Packet::default()];
        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        verifier.verify_batches(packet_batches, 0);
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 1);
        assert_eq!(
            verifier
                .stats
                .received_no_epoch_stakes
                .load(Ordering::Relaxed),
            0
        );

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());

        // Send a packet with no epoch stakes
        let messages = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5_000_000_000),
            signature: Signature::default(),
            rank: 0,
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, true);
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 0);
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 2);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 1);
        assert_eq!(
            verifier
                .stats
                .received_no_epoch_stakes
                .load(Ordering::Relaxed),
            1
        );

        // Expect no messages since the packet was malformed
        assert!(receiver.is_empty());

        // Send a packet with invalid rank
        let messages = vec![ConsensusMessage::Vote(VoteMessage {
            vote: Vote::new_finalization_vote(5),
            signature: Signature::default(),
            rank: 1000, // Invalid rank
        })];
        test_bls_message_transmission(&mut verifier, None, &messages, true);
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 0);
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 3);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 2);
        assert_eq!(
            verifier
                .stats
                .received_no_epoch_stakes
                .load(Ordering::Relaxed),
            1
        );

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
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 2);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 0);
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
        let packet_batches = verifier.verify_batches(packet_batches, 0);
        assert!(verifier.send_packets(packet_batches).is_ok());
        assert_eq!(verifier.stats.sent.load(Ordering::Relaxed), 0);
        assert_eq!(verifier.stats.sent_failed.load(Ordering::Relaxed), 0);
        assert_eq!(
            verifier.stats.verified_votes_sent.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            verifier
                .stats
                .verified_votes_sent_failed
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(verifier.stats.received.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received_discarded.load(Ordering::Relaxed), 1);
        assert_eq!(verifier.stats.received_malformed.load(Ordering::Relaxed), 0);
        assert_eq!(
            verifier
                .stats
                .received_no_epoch_stakes
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(verifier.stats.received_votes.load(Ordering::Relaxed), 0);
        assert!(receiver.is_empty());
    }

    #[test]
    fn test_blssigverifier_verify_votes_all_valid() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (validator_keypairs, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let num_votes = 5;
        let mut packets = Vec::with_capacity(num_votes);

        let vote = Vote::new_skip_vote(42);
        let vote_payload = bincode::serialize(&vote).expect("Failed to serialize vote");

        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(num_votes) {
            let rank = i as u16;
            let bls_keypair = &validator_keypair.bls_keypair;

            // Sign the payload with the BLS keypair.
            let signature: BLSSignature = bls_keypair.sign(&vote_payload).into();

            // Construct the full message.
            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });

            // Serialize the message into a packet for processing.
            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packets.push(packet);
        }

        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        let result_batches = verifier.verify_batches(packet_batches, num_votes);

        let mut processed_packets = 0;
        for batch in result_batches {
            for packet in batch.iter() {
                processed_packets += 1;
                assert!(
                    !packet.meta().discard(),
                    "Packet at rank {} should not be discarded",
                    processed_packets - 1
                );
            }
        }
        assert_eq!(processed_packets, num_votes, "Did not process all packets");
    }

    #[test]
    fn test_blssigverifier_verify_votes_one_invalid_signature() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (validator_keypairs, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let num_votes = 5;
        let invalid_rank = 2;
        let mut packets = Vec::with_capacity(num_votes);

        let vote = Vote::new_skip_vote(42);
        let valid_vote_payload = bincode::serialize(&vote).expect("Failed to serialize vote");
        let invalid_vote_payload =
            bincode::serialize(&Vote::new_skip_vote(99)).expect("Failed to serialize vote");

        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(num_votes) {
            let rank = i as u16;
            let bls_keypair = &validator_keypair.bls_keypair;

            // Generate a signature. If it's the invalid rank, sign the wrong payload.
            let signature = if rank == invalid_rank {
                bls_keypair.sign(&invalid_vote_payload).into() // create invalid signature
            } else {
                bls_keypair.sign(&valid_vote_payload).into()
            };

            // NOTE: The message itself always contains the correct vote data.
            // The signature is what makes it invalid.
            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });

            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packets.push(packet);
        }

        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        let result_batches = verifier.verify_batches(packet_batches, num_votes);

        let mut processed_packets = 0;
        for batch in result_batches {
            for (i, packet) in batch.iter().enumerate() {
                processed_packets += 1;
                if i as u16 == invalid_rank {
                    assert!(
                        packet.meta().discard(),
                        "Packet at invalid rank {i} should be discarded",
                    );
                } else {
                    assert!(
                        !packet.meta().discard(),
                        "Packet at valid rank {i} should not be discarded",
                    );
                }
            }
        }
        assert_eq!(processed_packets, num_votes, "Did not process all packets");
    }

    #[test]
    fn test_blssigverifier_verify_votes_empty_batch() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (_, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let packet_batches: Vec<PacketBatch> = vec![];
        let result_batches = verifier.verify_batches(packet_batches, 0);

        assert!(
            result_batches.is_empty(),
            "Result should be an empty vec for empty input"
        );
    }

    #[test]
    fn test_verify_certificate_base2_valid() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (validator_keypairs, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let num_signers = 7; // > 60% of 10 validators
        let slot = 10;
        let block_hash = Hash::new_unique();

        let certificate = Certificate::Notarize(slot, block_hash);

        let original_vote = Vote::new_notarization_vote(slot, block_hash);
        let signed_payload = bincode::serialize(&original_vote).unwrap();
        let vote_messages: Vec<VoteMessage> = (0..num_signers)
            .map(|i| {
                let signature = validator_keypairs[i].bls_keypair.sign(&signed_payload);
                VoteMessage {
                    vote: original_vote,
                    signature: signature.into(),
                    rank: i as u16,
                }
            })
            .collect();

        let mut builder = VoteCertificateBuilder::new(certificate);
        builder
            .aggregate(&vote_messages)
            .expect("Failed to aggregate votes");
        let cert_message = builder.build().expect("Failed to build certificate");
        let consensus_message = ConsensusMessage::Certificate(cert_message);
        let mut packet = Packet::default();
        packet.populate_packet(None, &consensus_message).unwrap();
        let packet_batches = vec![PinnedPacketBatch::new(vec![packet]).into()];

        let result_batches = verifier.verify_batches(packet_batches, 1);
        assert!(!result_batches[0].iter().next().unwrap().meta().discard());
    }

    #[test]
    fn test_verify_certificate_base3_valid() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (validator_keypairs, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let slot = 20;
        let block_hash = Hash::new_unique();

        let notarize_vote = Vote::new_notarization_vote(slot, block_hash);
        let notarize_payload = bincode::serialize(&notarize_vote).unwrap();
        let notarize_fallback_vote = Vote::new_notarization_fallback_vote(slot, block_hash);
        let notarize_fallback_payload = bincode::serialize(&notarize_fallback_vote).unwrap();

        let mut all_vote_messages = Vec::new();
        // Ranks 0-3 (4 validators) sign the Notarize payload.
        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(4) {
            let signature = validator_keypair.bls_keypair.sign(&notarize_payload);
            all_vote_messages.push(VoteMessage {
                vote: notarize_vote,
                signature: signature.into(),
                rank: i as u16,
            });
        }
        // Ranks 4-6 (3 validators) sign the NotarizeFallback payload.
        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(7).skip(4) {
            let signature = validator_keypair
                .bls_keypair
                .sign(&notarize_fallback_payload);
            all_vote_messages.push(VoteMessage {
                vote: notarize_fallback_vote,
                signature: signature.into(),
                rank: i as u16,
            });
        }

        let certificate = Certificate::NotarizeFallback(slot, block_hash);
        let mut builder = VoteCertificateBuilder::new(certificate);
        builder
            .aggregate(&all_vote_messages)
            .expect("Failed to aggregate votes");
        let cert_message = builder.build().expect("Failed to build certificate");
        let consensus_message = ConsensusMessage::Certificate(cert_message);
        let mut packet = Packet::default();
        packet.populate_packet(None, &consensus_message).unwrap();
        let packet_batches = vec![PinnedPacketBatch::new(vec![packet]).into()];

        let result_batches = verifier.verify_batches(packet_batches, 1);
        assert!(!result_batches[0].iter().next().unwrap().meta().discard());
    }

    #[test]
    fn test_verify_certificate_invalid_signature() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (_validator_keypairs, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let num_signers = 7;
        let slot = 10;
        let block_hash = Hash::new_unique();
        let certificate = Certificate::Notarize(slot, block_hash);
        let mut bitmap = BitVec::<u8, Lsb0>::new();
        bitmap.resize(num_signers, false);
        for i in 0..num_signers {
            bitmap.set(i, true);
        }
        let encoded_bitmap = encode_base2(&bitmap).unwrap();

        let cert_message = CertificateMessage {
            certificate,
            signature: BlsSignature::default(), // Use a default/wrong signature
            bitmap: encoded_bitmap,
        };
        let consensus_message = ConsensusMessage::Certificate(cert_message);
        let mut packet = Packet::default();
        packet.populate_packet(None, &consensus_message).unwrap();
        let packet_batches = vec![PinnedPacketBatch::new(vec![packet]).into()];

        let result_batches = verifier.verify_batches(packet_batches, 1);
        assert!(
            result_batches[0].iter().next().unwrap().meta().discard(),
            "Packet with invalid certificate signature should be discarded"
        );
    }

    #[test]
    fn test_verify_mixed_valid_batch() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (validator_keypairs, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let mut packets = Vec::new();
        let num_votes = 2;

        let vote = Vote::new_skip_vote(42);
        let vote_payload = bincode::serialize(&vote).unwrap();
        for (i, validator_keypair) in validator_keypairs.iter().enumerate().take(num_votes) {
            let rank = i as u16;
            let bls_keypair = &validator_keypair.bls_keypair;
            let signature: BLSSignature = bls_keypair.sign(&vote_payload).into();
            let consensus_message = ConsensusMessage::Vote(VoteMessage {
                vote,
                signature,
                rank,
            });
            let mut packet = Packet::default();
            packet.populate_packet(None, &consensus_message).unwrap();
            packets.push(packet);
        }

        let num_cert_signers = 7;
        let certificate = Certificate::Notarize(10, Hash::new_unique());
        let cert_original_vote = Vote::new_notarization_vote(10, certificate.to_block().unwrap().1);
        let cert_payload = bincode::serialize(&cert_original_vote).unwrap();

        let cert_vote_messages: Vec<VoteMessage> = (0..num_cert_signers)
            .map(|i| {
                let signature = validator_keypairs[i].bls_keypair.sign(&cert_payload);
                VoteMessage {
                    vote: cert_original_vote,
                    signature: signature.into(),
                    rank: i as u16,
                }
            })
            .collect();
        let mut builder =
            solana_votor::consensus_pool::vote_certificate_builder::VoteCertificateBuilder::new(
                certificate,
            );
        builder
            .aggregate(&cert_vote_messages)
            .expect("Failed to aggregate votes for certificate");
        let cert_message = builder.build().expect("Failed to build certificate");
        let consensus_message = ConsensusMessage::Certificate(cert_message);
        let mut packet = Packet::default();
        packet.populate_packet(None, &consensus_message).unwrap();
        packets.push(packet);
        let packet_batches = vec![PinnedPacketBatch::new(packets).into()];
        let result_batches = verifier.verify_batches(packet_batches, num_votes + 1);

        for packet in result_batches.iter().flatten() {
            assert!(
                !packet.meta().discard(),
                "No packets should be discarded in a mixed valid batch"
            );
        }
    }

    #[test]
    fn test_verify_vote_with_invalid_rank() {
        let (verified_vote_sender, _) = crossbeam_channel::unbounded();
        let (message_sender, _) = crossbeam_channel::unbounded();
        let (validator_keypairs, verifier) =
            create_keypairs_and_bls_sig_verifier(verified_vote_sender, message_sender);

        let invalid_rank = 999;
        let vote = Vote::new_skip_vote(42);
        let vote_payload = bincode::serialize(&vote).unwrap();
        let bls_keypair = &validator_keypairs[0].bls_keypair;
        let signature: BLSSignature = bls_keypair.sign(&vote_payload).into();

        let consensus_message = ConsensusMessage::Vote(VoteMessage {
            vote,
            signature,
            rank: invalid_rank,
        });

        let mut packet = Packet::default();
        packet.populate_packet(None, &consensus_message).unwrap();
        let packet_batches = vec![PinnedPacketBatch::new(vec![packet]).into()];
        let result_batches = verifier.verify_batches(packet_batches, 1);

        assert!(
            result_batches[0].iter().next().unwrap().meta().discard(),
            "Packet with invalid rank should be discarded"
        );
    }
}
