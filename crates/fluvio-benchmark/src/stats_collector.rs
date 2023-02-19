use std::{
    time::{Instant, Duration},
    collections::HashMap,
};

use async_channel::{Receiver, Sender};
use tracing::debug;
use anyhow::Result;

use crate::{BenchmarkError, benchmark_config::BenchmarkConfig};
use crate::stats::AllStatsSync;

// We expect every message produced to be read number_of_consumers_per_partition times.
// We also expect a total of num_producers_per_batch * num_records_per_batch unique messages.
#[derive(Default)]
pub struct BatchStats {
    collected_records: HashMap<u64, RecordMetadata>,
    pub last_flush_time: Option<Instant>,
}
impl BatchStats {
    pub fn record_sent(
        &mut self,
        hash: u64,
        send_time: Instant,
        num_bytes: u64,
    ) -> Result<(), BenchmarkError> {
        let val = self.collected_records.entry(hash).or_default();
        val.mark_send_time(send_time, num_bytes)
    }

    pub fn record_recv(
        &mut self,
        hash: u64,
        recv_time: Instant,
        consumer_id: u64,
    ) -> Result<(), BenchmarkError> {
        let val = self.collected_records.entry(hash).or_default();
        val.mark_recv_time(recv_time, consumer_id)
    }

    pub fn flush_recv(&mut self, flush_time: Instant) {
        if let Some(previous) = self.last_flush_time {
            if flush_time > previous {
                self.last_flush_time = Some(flush_time);
            }
        } else {
            self.last_flush_time = Some(flush_time)
        }
    }

    pub fn iter(&self) -> std::collections::hash_map::Values<u64, RecordMetadata> {
        self.collected_records.values()
    }
}

pub struct StatsWorker {
    receiver: Receiver<StatsCollectorMessage>,
    current_batch: BatchStats,
    config: BenchmarkConfig,
    tx_stop_consume: Vec<Sender<()>>,
    all_stats: AllStatsSync,
}

impl StatsWorker {
    pub fn new(
        tx_stop_consume: Vec<Sender<()>>,
        receiver: Receiver<StatsCollectorMessage>,
        config: BenchmarkConfig,
        all_stats: AllStatsSync,
    ) -> Self {
        Self {
            receiver,
            current_batch: BatchStats::default(),
            config,
            tx_stop_consume,
            all_stats,
        }
    }

    pub async fn collect_send_recv_messages(&mut self) -> Result<()> {
        let num_produced = self.config.total_number_of_messages_produced_per_batch();
        let num_consumed = self.config.total_number_of_messages_produced_per_batch()
            * self.config.number_of_expected_times_each_message_consumed();
        let num_flushed = self.config.num_concurrent_producer_workers;
        let total_expected_messages = num_produced + num_consumed + num_flushed;
        debug!(
            "Stats listening for {num_produced} sent messages and {num_consumed} received messages"
        );
        for _ in 0..total_expected_messages {
            match self.receiver.recv().await {
                Ok(message) => match message {
                    StatsCollectorMessage::MessageSent {
                        hash,
                        send_time,
                        num_bytes,
                    } => {
                        self.current_batch.record_sent(hash, send_time, num_bytes)?;
                    }
                    StatsCollectorMessage::MessageReceived => {}
                    StatsCollectorMessage::MessageHash { .. } => {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Received unexpected message hash".to_string(),
                        )
                        .into());
                    }
                    StatsCollectorMessage::ProducerFlushed { flush_time } => {
                        self.current_batch.flush_recv(flush_time)
                    }
                },
                Err(_) => {
                    return Err(BenchmarkError::ErrorWithExplanation(
                        "StatsCollectorChannelClosed".to_string(),
                    )
                    .into())
                }
            }
        }
        debug!("All expected messages sent and received");
        for tx in self.tx_stop_consume.iter() {
            tx.send(()).await?;
        }
        Ok(())
    }

    pub async fn validate(&mut self) -> Result<()> {
        let number_of_consumed_messages = self.config.total_number_of_messages_produced_per_batch()
            * self.config.number_of_expected_times_each_message_consumed();
        for _ in 0..number_of_consumed_messages {
            match self.receiver.recv().await {
                Ok(message) => match message {
                    StatsCollectorMessage::MessageSent { .. } => {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Received unexpected message sent".to_string(),
                        )
                        .into());
                    }
                    StatsCollectorMessage::MessageReceived => {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Received unexpected message received".to_string(),
                        )
                        .into());
                    }
                    StatsCollectorMessage::MessageHash {
                        hash,
                        recv_time,
                        consumer_id,
                    } => {
                        self.current_batch
                            .record_recv(hash, recv_time, consumer_id)?;
                    }
                    StatsCollectorMessage::ProducerFlushed { .. } => {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Received unexpected message flushed".to_string(),
                        )
                        .into());
                    }
                },
                Err(_) => {
                    return Err(BenchmarkError::ErrorWithExplanation(
                        "StatsCollectorChannelClosed".to_string(),
                    )
                    .into())
                }
            }
        }

        let expected_num_times_consumed =
            self.config.number_of_expected_times_each_message_consumed();
        for value in self.current_batch.collected_records.values() {
            value.validate(expected_num_times_consumed as usize)?;
        }
        debug!("Batch validated");

        Ok(())
    }

    pub async fn compute_stats(&self) {
        self.all_stats
            .lock()
            .await
            .compute_stats(&self.config, &self.current_batch);
    }

    pub fn new_batch(&mut self) {
        self.current_batch = BatchStats::default();
    }
}

pub enum StatsCollectorMessage {
    MessageSent {
        hash: u64,
        send_time: Instant,
        num_bytes: u64,
    },
    ProducerFlushed {
        flush_time: Instant,
    },
    MessageReceived,

    MessageHash {
        hash: u64,
        recv_time: Instant,
        consumer_id: u64,
    },
}

#[derive(Default)]
pub struct RecordMetadata {
    pub num_bytes: Option<u64>,
    pub send_time: Option<Instant>,
    pub first_received_time: Option<Instant>,
    pub last_received_time: Option<Instant>,
    pub receivers_list: Vec<u64>,
}
impl RecordMetadata {
    pub fn mark_send_time(
        &mut self,
        send_time: Instant,
        num_bytes: u64,
    ) -> Result<(), BenchmarkError> {
        if self.send_time.is_some() {
            Err(BenchmarkError::ErrorWithExplanation(
                "Message already marked as sent".to_string(),
            ))
        } else {
            self.num_bytes = Some(num_bytes);
            self.send_time = Some(send_time);
            Ok(())
        }
    }

    pub fn mark_recv_time(
        &mut self,
        recv_time: Instant,
        receiving_consumer_id: u64,
    ) -> Result<(), BenchmarkError> {
        if self.receivers_list.contains(&receiving_consumer_id) {
            Err(BenchmarkError::ErrorWithExplanation(
                "Message received twice by same consumer".to_string(),
            ))
        } else {
            if let Some(first_recv) = self.first_received_time {
                if recv_time < first_recv {
                    self.first_received_time = Some(recv_time)
                }
            } else {
                self.first_received_time = Some(recv_time)
            }
            if let Some(last_recv) = self.last_received_time {
                if recv_time > last_recv {
                    self.last_received_time = Some(recv_time)
                }
            } else {
                self.last_received_time = Some(recv_time)
            }
            self.receivers_list.push(receiving_consumer_id);
            Ok(())
        }
    }

    pub fn validate(&self, expected_num_times_consumed: usize) -> Result<(), BenchmarkError> {
        if self.send_time.is_none() {
            return Err(BenchmarkError::ErrorWithExplanation(
                "Message was never marked as sent".to_string(),
            ));
        }
        if self.receivers_list.len() != expected_num_times_consumed {
            let err_message = format!(
                "Message was expected to be received {} times but was only received {} times",
                expected_num_times_consumed,
                self.receivers_list.len()
            );
            Err(BenchmarkError::ErrorWithExplanation(err_message))
        } else {
            Ok(())
        }
    }
    pub fn first_recv_latency(&self) -> Duration {
        self.first_received_time.expect("Invalid record") - self.send_time.expect("Invalid record")
    }

    pub fn last_recv_latency(&self) -> Duration {
        self.last_received_time.expect("Invalid record") - self.send_time.expect("Invalid record")
    }
}
