use super::{TestDriver, NANOS_IN_MILLIS};
use crate::test_meta::chart_builder::FluvioTimeData;
use tracing::debug;

use std::future::Future;
use std::pin::Pin;
use rdkafka::producer::{FutureProducer as KafkaProducer, FutureRecord as KafkaRecord};
use rdkafka::producer::future_producer::OwnedDeliveryResult as KafkaProducerResult;
use rdkafka::util::AsyncRuntime;

use std::time::{Duration, SystemTime};

pub struct KafkaAsyncStdRuntime;

impl AsyncRuntime for KafkaAsyncStdRuntime {
    type Delay = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        fluvio_future::task::spawn(task);
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        Box::pin(fluvio_future::timer::sleep(duration))
    }
}

impl TestDriver {
    pub async fn kafka_send(
        &mut self,
        p: &mut KafkaProducer,
        topic: &str,
        msg_buf: Vec<u8>,
    ) -> KafkaProducerResult {
        let bytes_sent: usize = msg_buf.len();

        let now = SystemTime::now();

        let result = p
            .send::<Vec<u8>, _, _>(
                KafkaRecord::to(topic).payload(&msg_buf),
                Duration::from_secs(0),
            )
            .await;

        let produce_time_ns = now.elapsed().unwrap().as_nanos() as u64;
        let timestamp = self.test_elapsed().as_nanos() as f32 / NANOS_IN_MILLIS;

        debug!(
            "(#{}) Produce latency (ns): {:?} ({} B)",
            self.producer_latency.len() + 1,
            produce_time_ns,
            bytes_sent
        );

        self.producer_latency.record(produce_time_ns).unwrap();

        self.producer_bytes += bytes_sent as usize;

        // Make both use milliseconds
        self.producer_time_latency.push(FluvioTimeData {
            test_elapsed_ms: timestamp,
            data: (produce_time_ns as f32) / NANOS_IN_MILLIS,
        });

        self.producer_time_rate.push(FluvioTimeData {
            test_elapsed_ms: timestamp,
            data: bytes_sent as f32,
        });

        // Convert Bytes/ns to Bytes/s
        // 1_000_000_000 ns in 1 second
        const NS_IN_SECOND: f64 = 1_000_000_000.0;
        let rate = (bytes_sent as f64 / produce_time_ns as f64) * NS_IN_SECOND;
        debug!("Producer throughput Bytes/s: {:?}", rate);
        self.producer_rate.record(rate as u64).unwrap();

        result
    }
}
