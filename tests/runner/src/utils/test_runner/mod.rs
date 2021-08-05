pub mod test_meta;
pub mod test_driver;

#[allow(unused_imports)]
use fluvio_command::CommandExt;
use crate::test_meta::{TestCase, TestResult};
use crate::test_meta::environment::{EnvDetail, EnvironmentSetup};
use crate::test_meta::derive_attr::TestRequirements;
use fluvio::{Fluvio, FluvioError};
use std::sync::Arc;
use fluvio::metadata::topic::TopicSpec;
use hdrhistogram::Histogram;
use fluvio::{TopicProducer, RecordKey, PartitionConsumer};
use std::time::Duration;
use futures_lite::stream::StreamExt;
use tracing::debug;
use fluvio::Offset;
use fluvio_future::timer::sleep;

// Rename: *_latency, *_num, *_bytes
#[derive(Clone)]
pub struct FluvioTestDriver {
    pub client: Arc<Fluvio>,
    pub num_topics: usize,
    pub num_producers: usize,
    pub num_consumers: usize,
    pub bytes_produced: usize,
    pub bytes_consumed: usize,
    pub produce_latency: Histogram<u64>,
    pub consume_latency: Histogram<u64>,
    pub topic_create_latency: Histogram<u64>,
}

impl FluvioTestDriver {
    pub fn get_results(&self) -> TestResult {
        TestResult::default()
    }

    // Wrapper to getting a producer. We keep track of the number of producers we create
    pub async fn get_producer(&mut self, topic: &str) -> TopicProducer {
        match self.client.topic_producer(topic).await {
            Ok(client) => {
                self.num_producers += 1;
                return client;
            }
            Err(err) => {
                println!(
                    "unable to get producer to topic: {}, error: {} sleeping 10 second ",
                    topic, err
                );
                sleep(Duration::from_secs(10)).await;
            }
        }

        panic!("can't get producer");
    }

    // Wrapper to producer send. We measure the latency and accumulation of message payloads sent.
    pub async fn send_count(
        &mut self,
        p: &TopicProducer,
        key: RecordKey,
        message: String,
    ) -> Result<(), FluvioError> {
        use std::time::SystemTime;
        let now = SystemTime::now();

        let result = p.send(key, message.clone()).await;

        let produce_time = now.elapsed().unwrap().as_nanos();

        debug!(
            "(#{}) Produce latency (ns): {:?}",
            self.produce_latency.len() + 1,
            produce_time as u64
        );

        self.produce_latency.record(produce_time as u64).unwrap();

        self.bytes_produced += message.len();

        result
    }

    pub async fn get_consumer(&mut self, topic: &str) -> PartitionConsumer {
        match self.client.partition_consumer(topic.to_string(), 0).await {
            Ok(client) => {
                self.num_consumers += 1;
                return client;
            }
            Err(err) => {
                println!(
                    "unable to get consumer to topic: {}, error: {} sleeping 10 second ",
                    topic, err
                );
                sleep(Duration::from_secs(10)).await;
            }
        }

        panic!("can't get consumer");
    }

    pub async fn stream_count(&mut self, consumer: PartitionConsumer, offset: Offset) {
        use std::time::SystemTime;
        let mut stream = consumer.stream(offset).await.expect("stream");

        loop {
            // Take a timestamp
            let now = SystemTime::now();

            if let Some(Ok(record)) = stream.next().await {
                // Record latency
                let consume_time = now.elapsed().clone().unwrap().as_nanos();
                self.consume_latency.record(consume_time as u64).unwrap();

                // Record bytes consumed
                self.bytes_consumed += record.as_ref().len();
            } else {
                debug!("No more bytes left to consume");
                break;
            }
        }
    }

    // TODO: This is a workaround. Handle stream inside impl
    pub async fn consume_latency_record(&mut self, latency: u64) {
        self.consume_latency.record(latency).unwrap();
        debug!(
            "(#{}) Recording consumer latency (ns): {:?}",
            self.consume_latency.len(),
            latency
        );
    }

    // TODO: This is a workaround. Handle stream inside impl
    pub async fn consume_bytes_record(&mut self, bytes_len: usize) {
        self.bytes_consumed += bytes_len;
        debug!(
            "Recording consumer bytes len: {:?} (total: {})",
            bytes_len, self.bytes_consumed
        );
    }

    pub async fn create_topic(&mut self, option: &EnvironmentSetup) -> Result<(), ()> {
        use std::time::SystemTime;

        println!("Creating the topic: {}", &option.topic_name);

        let admin = self.client.admin().await;

        let topic_spec =
            TopicSpec::new_computed(option.partition as i32, option.replication() as i32, None);

        // Create topic and record how long it takes
        let now = SystemTime::now();

        let topic_create = admin
            .create(option.topic_name.clone(), false, topic_spec)
            .await;

        let topic_time = now.elapsed().unwrap().as_nanos();

        if topic_create.is_ok() {
            println!("topic \"{}\" created", option.topic_name);
            self.topic_create_latency.record(topic_time as u64).unwrap();
            self.num_topics += 1;
        } else {
            println!("topic \"{}\" already exists", option.topic_name);
        }

        Ok(())
    }

    pub fn is_env_acceptable(test_reqs: &TestRequirements, test_case: &TestCase) -> bool {
        // if `min_spu` undefined, min 1
        if let Some(min_spu) = test_reqs.min_spu {
            if min_spu > test_case.environment.spu() {
                println!("Test requires {} spu", min_spu);
                return false;
            }
        }

        // if `cluster_type` undefined, no cluster restrictions
        // if `cluster_type = local` is defined, then environment must be local or skip
        // if `cluster_type = k8`, then environment must be k8 or skip
        if let Some(cluster_type) = &test_reqs.cluster_type {
            if &test_case.environment.cluster_type() != cluster_type {
                println!("Test requires cluster type {:?} ", cluster_type);
                return false;
            }
        }

        true
    }
}
