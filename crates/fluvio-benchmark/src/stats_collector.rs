use std::{
    time::{Instant, Duration},
    collections::HashMap,
};
use async_std::channel::{Receiver, Sender};
use log::debug;

use crate::{
    BenchmarkError, benchmark_config::benchmark_settings::BenchmarkSettings, stats::compute_stats,
};

pub struct SampleStats {}

// We expect every message produced to be read number_of_consumers_per_partition times.
// We also expect a total of num_producers_per_batch * num_records_per_batch unique messages.
#[derive(Default)]
pub struct BatchStats {
    collected_records: HashMap<u64, RecordMetadata>,
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

    pub fn iter<'a>(&'a self) -> std::collections::hash_map::Values<'a, u64, RecordMetadata> {
        self.collected_records.values()
    }
}

pub struct StatsWorker {
    receiver: Receiver<StatsCollectorMessage>,
    current_batch: BatchStats,
    settings: BenchmarkSettings,
    tx_stop_consume: Vec<Sender<()>>,
}

impl StatsWorker {
    pub fn new(
        tx_stop_consume: Vec<Sender<()>>,
        receiver: Receiver<StatsCollectorMessage>,
        settings: BenchmarkSettings,
    ) -> Self {
        Self {
            receiver,
            current_batch: BatchStats::default(),
            settings,
            tx_stop_consume,
        }
    }

    pub async fn collect_send_recv_messages(&mut self) -> Result<(), BenchmarkError> {
        let num_produced = self.settings.total_number_of_messages_produced_per_batch();
        let num_consumed = self.settings.total_number_of_messages_produced_per_batch()
            * self
                .settings
                .number_of_expected_times_each_message_consumed();
        let total_expected_messages = num_produced + num_consumed;
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
                        ));
                    }
                },
                Err(_) => {
                    return Err(BenchmarkError::ErrorWithExplanation(
                        "StatsCollectorChannelClosed".to_string(),
                    ))
                }
            }
        }
        debug!("All expected messages sent and received");
        for tx in self.tx_stop_consume.iter() {
            tx.send(()).await?;
        }
        Ok(())
    }

    pub async fn validate(&mut self) -> Result<(), BenchmarkError> {
        let number_of_consumed_messages =
            self.settings.total_number_of_messages_produced_per_batch()
                * self
                    .settings
                    .number_of_expected_times_each_message_consumed();
        for _ in 0..number_of_consumed_messages {
            match self.receiver.recv().await {
                Ok(message) => match message {
                    StatsCollectorMessage::MessageSent { .. } => {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Received unexpected message sent".to_string(),
                        ));
                    }
                    StatsCollectorMessage::MessageReceived => {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Received unexpected message received".to_string(),
                        ));
                    }
                    StatsCollectorMessage::MessageHash {
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

        let expected_num_times_consumed = self
            .settings
            .number_of_expected_times_each_message_consumed();
        for value in self.current_batch.collected_records.values() {
            value.validate(expected_num_times_consumed as usize)?;
        }
        debug!("Batch validated");

        Ok(())
    }

    pub fn compute_stats(&self) {
        compute_stats(&self.current_batch);
    }

    pub fn new_batch(&mut self) {
        // TODO compute stats
        self.current_batch = BatchStats::default();
    }
}

pub enum StatsCollectorMessage {
    MessageSent {
        hash: u64,
        send_time: Instant,
        num_bytes: u64,
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
    receivers_list: Vec<u64>,
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
