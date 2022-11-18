use std::{time::Instant, collections::HashMap};
use async_std::channel::Receiver;

use crate::{BenchmarkError, benchmark_config::benchmark_settings::BenchmarkSettings};

pub struct SampleStats {}

// We expect every message produced to be read number_of_consumers_per_partition times.
// We also expect a total of num_producers_per_batch * num_records_per_batch unique messages.
pub struct BatchStats {
    collected_records: HashMap<u64, RecordMetadata>,
}
impl BatchStats {
    pub fn record_sent(&mut self, hash: u64, send_time: Instant) -> Result<(), BenchmarkError> {
        let val = self.collected_records.entry(hash).or_default();
        val.mark_send_time(send_time)
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
}

pub struct StatsCollector {
    receiver: Receiver<StatsCollectorMessage>,
    current_batch: BatchStats,
    settings: BenchmarkSettings,
}

impl StatsCollector {
    pub async fn collect_stats(&mut self) -> Result<(), BenchmarkError> {
        let total_expected_messages = self.settings.total_number_of_messages_produced_per_batch()
            * (1 + self // Plus one is for the produce message
                .settings
                .number_of_expected_times_each_message_consumed());
        for _ in 0..total_expected_messages {
            match self.receiver.recv().await {
                Ok(message) => match message {
                    StatsCollectorMessage::MessageSent { hash, send_time } => {
                        self.current_batch.record_sent(hash, send_time)?;
                    }
                    StatsCollectorMessage::MessageReceived {
                        hash,
                        recv_time,
                        consumer_id,
                    } => {
                        self.current_batch
                            .record_recv(hash, recv_time, consumer_id)?;
                    }
                },
                Err(_) => {
                    return Err(BenchmarkError::ErrorWithExplanation(
                        "StatsCollectorChannelClosed".to_string(),
                    ))
                }
            }
        }

        Ok(())
    }

    pub fn validate(&mut self) -> Result<(), BenchmarkError> {
        let expected_num_times_consumed = self
            .settings
            .number_of_expected_times_each_message_consumed();
        for value in self.current_batch.collected_records.values() {
            value.validate(expected_num_times_consumed as usize)?;
        }
        Ok(())
    }
}

pub enum StatsCollectorMessage {
    MessageSent {
        hash: u64,
        send_time: Instant,
    },

    MessageReceived {
        hash: u64,
        recv_time: Instant,
        consumer_id: u64,
    },
}

#[derive(Default)]
pub struct RecordMetadata {
    send_time: Option<Instant>,
    first_received_time: Option<Instant>,
    last_received_time: Option<Instant>,
    receivers_list: Vec<u64>,
}
impl RecordMetadata {
    pub fn mark_send_time(&mut self, send_time: Instant) -> Result<(), BenchmarkError> {
        if self.send_time.is_some() {
            Err(BenchmarkError::ErrorWithExplanation(
                "Message already marked as sent".to_string(),
            ))
        } else {
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
}
