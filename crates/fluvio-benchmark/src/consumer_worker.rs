use std::pin::Pin;
use std::time::{Duration, Instant};

use async_std::channel::Receiver;
use async_std::prelude::FutureExt;
use async_std::stream::StreamExt;
use async_std::{channel::Sender, stream::Stream};
use fluvio::{
    PartitionConsumer, consumer::ConsumerConfigBuilder, Fluvio, Offset, dataplane::link::ErrorCode,
};
use fluvio::dataplane::record::ConsumerRecord;

use crate::{BenchmarkError, hash_record};
use crate::{
    benchmark_config::benchmark_settings::BenchmarkSettings, stats_collector::StatsCollectorMessage,
};

pub struct ConsumerWorker {
    fluvio_consumer: PartitionConsumer,
    consumer_id: u64,
    settings: BenchmarkSettings,
    tx_to_stats_collector: Sender<StatsCollectorMessage>,
    assigned_partition: u64,
    stream: Pin<Box<dyn Stream<Item = Result<ConsumerRecord, ErrorCode>>>>,
    received: Vec<(ConsumerRecord, Instant)>,
    preallocation_hint: u64,
}

impl ConsumerWorker {
    pub async fn new(
        settings: BenchmarkSettings,
        consumer_id: u64,
        tx_to_stats_collector: Sender<StatsCollectorMessage>,
        assigned_partition: u64,
        preallocation_hint: u64,
    ) -> Self {
        let fluvio = Fluvio::connect().await.unwrap();
        let mut config_builder = ConsumerConfigBuilder::default();
        config_builder.max_bytes(settings.consumer_max_bytes as i32);

        let config = config_builder.build().unwrap();

        let fluvio_consumer =
            fluvio::consumer(settings.topic_name.clone(), assigned_partition as u32)
                .await
                .unwrap();
        let stream = fluvio_consumer
            .stream_with_config(Offset::absolute(0).unwrap(), config)
            .await
            .unwrap();

        Self {
            fluvio_consumer,
            consumer_id,
            settings,
            tx_to_stats_collector,
            assigned_partition,
            stream: Box::pin(stream),
            received: Vec::with_capacity(preallocation_hint as usize),
            preallocation_hint,
        }
    }

    pub async fn consume(&mut self, stop_rx: Receiver<()>) -> Result<(), BenchmarkError> {
        self.received.clear();
        loop {
            match self.stream.next().timeout(Duration::from_millis(20)).await {
                Ok(record_opt) => {
                    if let Some(Ok(record)) = record_opt {
                        self.received.push((record, Instant::now()));
                    } else {
                        return Err(BenchmarkError::ErrorWithExplanation(
                            "Consumer unable to get record from fluvio".to_string(),
                        ));
                    }
                }
                // timeout
                Err(_) => {
                    if let Ok(_) = stop_rx.try_recv() {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn send_results(&mut self) {
        for (record, recv_time) in self.received.iter() {
            let data = String::from_utf8_lossy(record.value());
            self.tx_to_stats_collector
                .send(StatsCollectorMessage::MessageReceived {
                    hash: hash_record(&data),
                    recv_time: *recv_time,
                    consumer_id: self.consumer_id,
                });
        }
    }
}
