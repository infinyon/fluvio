use super::{FluvioTestDriver, NANOS_IN_MILLIS};
use serde::{Serialize, Deserialize};
use pulsar::{
    producer, producer::Producer as PulsarProducer, Error as PulsarError, SerializeMessage,
    AsyncStdExecutor, DeserializeMessage, message::Payload,
};
use pulsar::producer::SendFuture;
use tracing::debug;

use std::time::SystemTime;
use crate::test_meta::chart_builder::FluvioTimeData;

#[derive(Serialize, Deserialize)]
pub struct PulsarTestData {
    pub data: Vec<u8>,
}

impl SerializeMessage for PulsarTestData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for PulsarTestData {
    type Output = Result<PulsarTestData, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

impl FluvioTestDriver {
    pub async fn pulsar_send(
        &mut self,
        p: &mut PulsarProducer<AsyncStdExecutor>,
        msg_buf: Vec<PulsarTestData>,
    ) -> Result<Vec<SendFuture>, PulsarError> {
        let bytes_sent: usize = msg_buf
            .iter()
            .map(|m| &m.data)
            .fold(0, |total, msg| total + msg.len());

        let now = SystemTime::now();

        let result = p.send_all(msg_buf).await.expect("Send failed");

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

        Ok(result)
    }
}
