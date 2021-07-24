use super::{TestDriver, NANOS_IN_MILLIS};
use crate::test_meta::chart_builder::FluvioTimeData;

use fluvio::{TopicProducer, RecordKey, FluvioError};
use std::time::SystemTime;
use tracing::debug;

impl TestDriver {
    // Wrapper to producer send. We measure the latency and accumulation of message payloads sent.
    pub async fn fluvio_send(
        &mut self,
        p: &TopicProducer,
        msg_buf: Vec<(RecordKey, Vec<u8>)>,
    ) -> Result<(), FluvioError> {
        let bytes_sent: usize = msg_buf
            .iter()
            .map(|m| &m.1)
            .fold(0, |total, msg| total + msg.len());

        let now = SystemTime::now();

        let result = p.send_all(msg_buf).await;

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
