use std::pin::Pin;
use std::time::{Duration, Instant};

use async_channel::{Sender, Receiver};
use anyhow::Result;

use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio::Fluvio;
use fluvio::{Offset, dataplane::link::ErrorCode};
use fluvio::dataplane::record::ConsumerRecord;
use fluvio_future::future::timeout;
use futures_util::{Stream, StreamExt};
use crate::{BenchmarkError, hash_record};
use crate::{benchmark_config::BenchmarkConfig, stats_collector::StatsCollectorMessage};

pub struct ConsumerWorker {
    consumer_id: u64,
    tx_to_stats_collector: Sender<StatsCollectorMessage>,
    stream: Pin<Box<dyn Stream<Item = Result<ConsumerRecord, ErrorCode>> + Send>>,
    received: Vec<(ConsumerRecord, Instant)>,
    rx_stop: Receiver<()>,
}

impl ConsumerWorker {
    pub async fn new(
        config: BenchmarkConfig,
        consumer_id: u64,
        tx_to_stats_collector: Sender<StatsCollectorMessage>,
        rx_stop: Receiver<()>,
        assigned_partition: u64,
        preallocation_hint: u64,
    ) -> Result<Self> {
        let fluvio = Fluvio::connect().await?;
        let stream = fluvio
            .consumer_with_config(
                ConsumerConfigExtBuilder::default()
                    .topic(config.topic_name)
                    .partition(assigned_partition as u32)
                    .offset_start(Offset::absolute(0)?)
                    .max_bytes(config.consumer_max_bytes as i32)
                    .isolation(config.consumer_isolation)
                    .build()?,
            )
            .await?;

        Ok(Self {
            consumer_id,
            tx_to_stats_collector,
            stream: Box::pin(stream),
            received: Vec::with_capacity(preallocation_hint as usize),
            rx_stop,
        })
    }

    pub async fn consume(&mut self) -> Result<()> {
        self.received.clear();
        loop {
            match timeout(Duration::from_millis(20), self.stream.next()).await {
                Ok(record_opt) => {
                    if let Some(Ok(record)) = record_opt {
                        self.received.push((record, Instant::now()));
                        self.tx_to_stats_collector
                            .send(StatsCollectorMessage::MessageReceived)
                            .await?;
                    } else {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Consumer unable to get record from fluvio".to_string(),
                        )
                        .into());
                    }
                }
                // timeout
                Err(_) => {
                    if self.rx_stop.try_recv().is_ok() {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn send_results(&mut self) -> Result<()> {
        for (record, recv_time) in self.received.iter() {
            let data = record.get_value().as_utf8_lossy_string();
            self.tx_to_stats_collector
                .send(StatsCollectorMessage::MessageHash {
                    hash: hash_record(&data),
                    recv_time: *recv_time,
                    consumer_id: self.consumer_id,
                })
                .await?;
        }
        Ok(())
    }
}
