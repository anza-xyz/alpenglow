#[cfg(feature = "alpenglow")]
use {solana_poh::poh_recorder::VoteCertificate, solana_sdk::clock::Slot, std::sync::Arc};

use {
    super::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_metrics::LeaderSlotMetricsTracker,
        packet_deserializer::{PacketDeserializer, ReceivePacketResults},
        unprocessed_transaction_storage::UnprocessedTransactionStorage,
        BankingStageStats,
    },
    agave_banking_stage_ingress_types::BankingPacketReceiver,
    crossbeam_channel::RecvTimeoutError,
    solana_measure::{measure::Measure, measure_us},
    solana_sdk::{saturating_add_assign, timing::timestamp},
    std::{sync::atomic::Ordering, time::Duration},
};

pub struct PacketReceiver {
    id: u32,
    packet_deserializer: PacketDeserializer,
}

impl PacketReceiver {
    pub fn new(id: u32, banking_packet_receiver: BankingPacketReceiver) -> Self {
        Self {
            id,
            packet_deserializer: PacketDeserializer::new(banking_packet_receiver),
        }
    }

    /// Receive incoming packets, push into unprocessed buffer with packet indexes
    pub fn receive_and_buffer_packets(
        &mut self,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        banking_stage_stats: &mut BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> Result<(), RecvTimeoutError> {
        let (result, recv_time_us) = measure_us!({
            let recv_timeout = Self::get_receive_timeout(unprocessed_transaction_storage);
            let mut recv_and_buffer_measure = Measure::start("recv_and_buffer");
            self.packet_deserializer
                .receive_packets(
                    recv_timeout,
                    unprocessed_transaction_storage.max_receive_size(),
                    |packet| {
                        packet.check_insufficent_compute_unit_limit()?;
                        packet.check_excessive_precompiles()?;
                        Ok(packet)
                    },
                )
                // Consumes results if Ok, otherwise we keep the Err
                .map(|receive_packet_results| {
                    self.buffer_packets(
                        receive_packet_results,
                        unprocessed_transaction_storage,
                        banking_stage_stats,
                        slot_metrics_tracker,
                    );
                    recv_and_buffer_measure.stop();

                    // Only incremented if packets are received
                    banking_stage_stats
                        .receive_and_buffer_packets_elapsed
                        .fetch_add(recv_and_buffer_measure.as_us(), Ordering::Relaxed);
                })
        });

        slot_metrics_tracker.increment_receive_and_buffer_packets_us(recv_time_us);

        result
    }

    #[cfg(feature = "alpenglow")]
    pub fn process_vote_certificate(
        &mut self,
        slot: Slot,
        vote_certificate: VoteCertificate,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        banking_stage_stats: &mut BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        match unprocessed_transaction_storage {
            UnprocessedTransactionStorage::VoteStorage(_vote_storage) => {
                if !vote_certificate.is_empty() {
                    let mut recv_and_buffer_measure = Measure::start("recv_and_buffer_certificate");
                    let deserialized_certificate =
                        PacketDeserializer::deserialize_and_collect_packets(
                            // Each vote in the certificate must be in its own PacketBatch
                            vote_certificate.len(),
                            &[Arc::new(vote_certificate)],
                            |packet| {
                                // TODO: CHECK WITH TEAM IF THIS IS SAFE
                                packet
                                    .check_insufficent_compute_unit_limit()
                                    .expect("Certificate tx exceeded compute limit");
                                packet
                                    .check_excessive_precompiles()
                                    .expect("Certificate tx excessive precompiles");
                                Ok(packet)
                            },
                        );

                    self.buffer_vote_certificate(
                        slot,
                        deserialized_certificate,
                        unprocessed_transaction_storage,
                        banking_stage_stats,
                        slot_metrics_tracker,
                    );
                    recv_and_buffer_measure.stop();

                    // Only incremented if packets are received
                    banking_stage_stats
                        .receive_and_buffer_packets_elapsed
                        .fetch_add(recv_and_buffer_measure.as_us(), Ordering::Relaxed);
                }
            }
            UnprocessedTransactionStorage::LocalTransactionStorage(_) => {
                panic!("should never reach here");
            }
        }
    }

    fn get_receive_timeout(
        unprocessed_transaction_storage: &UnprocessedTransactionStorage,
    ) -> Duration {
        // Gossip thread (does not process) should not continuously receive with 0 duration.
        // This can cause the thread to run at 100% CPU because it is continuously polling.
        if !unprocessed_transaction_storage.should_not_process()
            && !unprocessed_transaction_storage.is_empty()
        {
            // If there are buffered packets, run the equivalent of try_recv to try reading more
            // packets. This prevents starving BankingStage::consume_buffered_packets due to
            // buffered_packet_batches containing transactions that exceed the cost model for
            // the current bank.
            Duration::from_millis(0)
        } else {
            // Default wait time
            Duration::from_millis(100)
        }
    }

    fn buffer_packets(
        &self,
        ReceivePacketResults {
            deserialized_packets,
            packet_stats,
        }: ReceivePacketResults,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        banking_stage_stats: &mut BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        let packet_count = deserialized_packets.len();
        debug!("@{:?} txs: {} id: {}", timestamp(), packet_count, self.id);

        slot_metrics_tracker.increment_received_packet_counts(packet_stats);

        let mut dropped_packets_count = 0;
        let mut newly_buffered_packets_count = 0;
        let mut newly_buffered_forwarded_packets_count = 0;
        Self::push_unprocessed(
            unprocessed_transaction_storage,
            deserialized_packets,
            &mut dropped_packets_count,
            &mut newly_buffered_packets_count,
            &mut newly_buffered_forwarded_packets_count,
            banking_stage_stats,
            slot_metrics_tracker,
        );

        banking_stage_stats
            .receive_and_buffer_packets_count
            .fetch_add(packet_count, Ordering::Relaxed);
        banking_stage_stats
            .dropped_packets_count
            .fetch_add(dropped_packets_count, Ordering::Relaxed);
        banking_stage_stats
            .newly_buffered_packets_count
            .fetch_add(newly_buffered_packets_count, Ordering::Relaxed);
        banking_stage_stats
            .current_buffered_packets_count
            .swap(unprocessed_transaction_storage.len(), Ordering::Relaxed);
    }

    fn push_unprocessed(
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        deserialized_packets: Vec<ImmutableDeserializedPacket>,
        dropped_packets_count: &mut usize,
        newly_buffered_packets_count: &mut usize,
        newly_buffered_forwarded_packets_count: &mut usize,
        banking_stage_stats: &mut BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        if !deserialized_packets.is_empty() {
            let _ = banking_stage_stats
                .batch_packet_indexes_len
                .increment(deserialized_packets.len() as u64);

            *newly_buffered_packets_count += deserialized_packets.len();
            *newly_buffered_forwarded_packets_count += deserialized_packets
                .iter()
                .filter(|p| p.original_packet().meta().forwarded())
                .count();
            slot_metrics_tracker
                .increment_newly_buffered_packets_count(deserialized_packets.len() as u64);

            let insert_packet_batches_summary =
                unprocessed_transaction_storage.insert_batch(deserialized_packets);
            slot_metrics_tracker
                .accumulate_insert_packet_batches_summary(&insert_packet_batches_summary);
            saturating_add_assign!(
                *dropped_packets_count,
                insert_packet_batches_summary.total_dropped_packets()
            );
        }
    }

    // TODO, merge the *_vote_certificate functions with the functions above
    // using common functions to make tracking Agave changes easier
    #[cfg(feature = "alpenglow")]
    fn buffer_vote_certificate(
        &self,
        slot: Slot,
        ReceivePacketResults {
            deserialized_packets: vote_certificate,
            packet_stats,
        }: ReceivePacketResults,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        banking_stage_stats: &mut BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        let packet_count = vote_certificate.len();
        debug!("@{:?} txs: {} id: {}", timestamp(), packet_count, self.id);

        slot_metrics_tracker.increment_received_packet_counts(packet_stats);

        let mut newly_buffered_packets_count = 0;
        Self::push_vote_certificate(
            slot,
            vote_certificate,
            unprocessed_transaction_storage,
            &mut newly_buffered_packets_count,
            banking_stage_stats,
            slot_metrics_tracker,
        );

        banking_stage_stats
            .newly_buffered_packets_count
            .fetch_add(newly_buffered_packets_count, Ordering::Relaxed);
        banking_stage_stats
            .current_buffered_packets_count
            .swap(unprocessed_transaction_storage.len(), Ordering::Relaxed);
    }

    #[cfg(feature = "alpenglow")]
    fn push_vote_certificate(
        slot: Slot,
        vote_certificate: Vec<ImmutableDeserializedPacket>,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        newly_buffered_packets_count: &mut usize,
        banking_stage_stats: &mut BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        if !vote_certificate.is_empty() {
            let _ = banking_stage_stats
                .batch_packet_indexes_len
                .increment(vote_certificate.len() as u64);

            *newly_buffered_packets_count += vote_certificate.len();
            slot_metrics_tracker
                .increment_newly_buffered_packets_count(vote_certificate.len() as u64);

            match unprocessed_transaction_storage {
                UnprocessedTransactionStorage::VoteStorage(vote_storage) => {
                    vote_storage.insert_vote_certificate(
                        slot,
                        vote_certificate.into_iter().map(Arc::new).collect(),
                    );
                }
                UnprocessedTransactionStorage::LocalTransactionStorage(_) => {
                    panic!("should never reach here");
                }
            }
        }
    }
}
