#[allow(unused_imports)]
use fluvio_command::CommandExt;
use crate::test_meta::TestCase;
use crate::test_meta::test_result::TestResult;
use crate::test_meta::environment::{EnvDetail, EnvironmentSetup};
use crate::test_meta::derive_attr::TestRequirements;
use fluvio::{Fluvio, FluvioError};
use std::sync::Arc;
use fluvio::metadata::topic::TopicSpec;
use hdrhistogram::Histogram;
use fluvio::{TopicProducer, RecordKey, PartitionConsumer};
use futures_lite::stream::StreamExt;
use tracing::debug;
use fluvio::Offset;

pub enum TestDriverType {
    Fluvio(Fluvio),
}
#[derive(Clone)]
pub struct TestDriver {
    pub admin_client: Arc<TestDriverType>,
    pub topic_num: usize,
    pub producer_num: usize,
    pub consumer_num: usize,
    pub producer_bytes: usize,
    pub consumer_bytes: usize,
    pub producer_latency_histogram: Histogram<u64>,
    pub consumer_latency_histogram: Histogram<u64>,
    pub topic_create_latency_histogram: Histogram<u64>,
}

impl TestDriver {
    pub fn get_results(&self) -> TestResult {
        TestResult::default()
    }

    // Wrapper to getting a producer. We keep track of the number of producers we create
    pub async fn create_producer(&mut self, topic: &str) -> TopicProducer {
        debug!(topic, "creating producer");
        let fluvio_client = self.create_client().await.expect("cant' create client");
        match fluvio_client.topic_producer(topic).await {
            Ok(client) => {
                self.producer_num += 1;
                client
            }
            Err(err) => {
                panic!("could not create producer: {:#?}", err);
            }
        }
    }

    // Wrapper to producer send. We measure the latency and accumulation of message payloads sent.
    pub async fn send_count(
        &mut self,
        p: &TopicProducer,
        key: RecordKey,
        message: Vec<u8>,
    ) -> Result<(), FluvioError> {
        use std::time::SystemTime;
        let now = SystemTime::now();

        let result = p.send(key, message.clone()).await;

        let produce_time = now.elapsed().unwrap().as_nanos();

        debug!(
            "(#{}) Produce latency (ns): {:?}",
            self.producer_latency_histogram.len() + 1,
            produce_time as u64
        );

        self.producer_latency_histogram
            .record(produce_time as u64)
            .unwrap();

        self.consumer_bytes += message.len();

        result
    }

    pub async fn get_consumer(&mut self, topic: &str) -> PartitionConsumer {
        let fluvio_client = self.create_client().await.expect("cant' create client");
        match fluvio_client.partition_consumer(topic.to_string(), 0).await {
            Ok(client) => {
                self.consumer_num += 1;
                client
            }
            Err(err) => {
                panic!("can't create consumer: {:#?}", err);
            }
        }
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
                self.consumer_latency_histogram
                    .record(consume_time as u64)
                    .unwrap();

                // Record bytes consumed
                self.consumer_bytes += record.as_ref().len();
            } else {
                debug!("No more bytes left to consume");
                break;
            }
        }
    }

    // TODO: This is a workaround. Handle stream inside impl
    pub async fn consume_latency_record(&mut self, latency: u64) {
        self.consumer_latency_histogram.record(latency).unwrap();
        debug!(
            "(#{}) Recording consumer latency (ns): {:?}",
            self.consumer_latency_histogram.len(),
            latency
        );
    }

    // TODO: This is a workaround. Handle stream inside impl
    pub async fn consume_bytes_record(&mut self, bytes_len: usize) {
        self.consumer_bytes += bytes_len;
        debug!(
            "Recording consumer bytes len: {:?} (total: {})",
            bytes_len, self.consumer_bytes
        );
    }

    pub async fn create_topic(&mut self, option: &EnvironmentSetup) -> Result<(), ()> {
        use std::time::SystemTime;

        let topic_name = option.topic_name();
        println!("Creating the topic: {}", &topic_name);

        let TestDriverType::Fluvio(fluvio_client) = self.admin_client.as_ref();
        let admin = fluvio_client.admin().await;

        let topic_spec =
            TopicSpec::new_computed(option.partition as i32, option.replication() as i32, None);

        // Create topic and record how long it takes
        let now = SystemTime::now();

        let topic_create = admin.create(topic_name.clone(), false, topic_spec).await;

        let topic_time = now.elapsed().unwrap().as_nanos();

        if topic_create.is_ok() {
            println!("topic \"{}\" created", topic_name);
            self.topic_create_latency_histogram
                .record(topic_time as u64)
                .unwrap();
            self.topic_num += 1;
        } else {
            println!("topic \"{}\" already exists", topic_name);
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

    /// create new fluvio client
    async fn create_client(&self) -> Result<Fluvio, FluvioError> {
        Fluvio::connect().await
    }
}
