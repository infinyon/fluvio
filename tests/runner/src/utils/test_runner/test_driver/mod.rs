#[allow(unused_imports)]
use fluvio_command::CommandExt;
use crate::test_meta::TestCase;
use crate::test_meta::test_timer::TestTimer;
use crate::test_meta::test_result::TestResult;
use crate::test_meta::environment::{EnvDetail, EnvironmentSetup};
use crate::test_meta::derive_attr::TestRequirements;
use fluvio::{Fluvio, FluvioError};
use std::sync::Arc;
use fluvio::metadata::topic::TopicSpec;
use hdrhistogram::Histogram;
use fluvio::{TopicProducer, RecordKey, PartitionConsumer};
use std::time::Duration;
use tracing::debug;

const NS_IN_SECOND: f64 = 1_000_000_000.0;
//const NANOS_IN_MILLIS: f32 = 1_000_000.0;

pub enum TestDriverType {
    Fluvio(Fluvio),
}

pub enum TestProducer {
    Fluvio(TopicProducer),
}

pub enum TestConsumer {
    Fluvio(PartitionConsumer),
}

#[derive(Clone)]
pub struct TestDriver {
    pub client: Arc<TestDriverType>,
    pub timer: TestTimer,
    pub topic_num: usize,
    pub producer_num: usize,
    pub consumer_num: usize,
    pub producer_bytes: usize,
    pub consumer_bytes: usize,
    pub producer_latency_histogram: Histogram<u64>,
    pub consumer_latency_histogram: Histogram<u64>,
    pub topic_create_latency_histogram: Histogram<u64>,
    pub producer_rate_histogram: Histogram<u64>,
    pub consumer_rate_histogram: Histogram<u64>,
}

impl TestDriver {
    pub fn new(client: Arc<TestDriverType>) -> Self {
        Self {
            client,
            timer: TestTimer::new(),
            topic_num: 0,
            producer_num: 0,
            consumer_num: 0,
            producer_bytes: 0,
            consumer_bytes: 0,
            producer_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consumer_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            topic_create_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2)
                .unwrap(),
            producer_rate_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consumer_rate_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
        }
    }

    pub fn get_results(&self) -> TestResult {
        TestResult::default()
    }

    pub fn start_timer(&mut self) {
        self.timer.start()
    }

    pub fn stop_timer(&mut self) {
        self.timer.stop()
    }

    pub fn is_test_running(&self) -> bool {
        self.timer.is_running()
    }

    pub fn test_elapsed(&self) -> Duration {
        self.timer.elapsed()
    }

    // Wrapper to getting a producer. We keep track of the number of producers we create
    pub async fn get_producer(&mut self, topic: &str) -> TestProducer {
        let TestDriverType::Fluvio(fluvio_client) = self.client.as_ref();
        match fluvio_client.topic_producer(topic).await {
            Ok(producer) => {
                self.producer_num += 1;
                return TestProducer::Fluvio(producer);
            }
            Err(err) => {
                panic!("could not create producer: {:#?}", err);
            }
        }
    }

    // Wrapper to producer send. We measure the latency and accumulation of message payloads sent.
    pub async fn fluvio_send(
        &mut self,
        p: &TopicProducer,
        msg_buf: Vec<(RecordKey, Vec<u8>)>,
    ) -> Result<(), FluvioError> {
        use std::time::SystemTime;
        let now = SystemTime::now();

        let bytes_sent: usize = msg_buf
            .iter()
            .map(|m| &m.1)
            .fold(0, |total, msg| total + msg.len());

        let result = p.send_all(msg_buf).await;

        let produce_time_ns = now.elapsed().unwrap().as_nanos() as u64;

        debug!(
            "(#{}) Produce latency (ns): {:?}",
            self.producer_latency_histogram.len() + 1,
            produce_time_ns
        );

        self.producer_latency_histogram
            .record(produce_time_ns)
            .unwrap();

        self.producer_bytes += bytes_sent as usize;
        //let timestamp = self.test_elapsed().as_nanos() as f32 / NANOS_IN_MILLIS;

        let rate = (bytes_sent as f64 / produce_time_ns as f64) * NS_IN_SECOND;
        debug!("Producer throughput Bytes/s: {:?}", rate);
        self.producer_rate_histogram.record(rate as u64).unwrap();

        result
    }

    pub async fn get_consumer(&mut self, topic: &str) -> TestConsumer {
        let TestDriverType::Fluvio(fluvio_client) = self.client.as_ref();
        match fluvio_client.partition_consumer(topic.to_string(), 0).await {
            Ok(consumer) => {
                self.consumer_num += 1;
                return TestConsumer::Fluvio(consumer);
            }
            Err(err) => {
                panic!("can't create consumer: {:#?}", err);
            }
        }
    }

    // TODO: This is a workaround. Handle stream inside impl
    pub async fn consume_record(&mut self, bytes_len: usize, consumer_latency: u64) {
        self.consumer_latency_histogram
            .record(consumer_latency)
            .unwrap();
        debug!(
            "(#{}) Recording consumer latency (ns): {:?}",
            self.consumer_latency_histogram.len(),
            consumer_latency
        );

        self.consumer_bytes += bytes_len;
        debug!(
            "Recording consumer bytes len: {:?} (total: {})",
            bytes_len, self.consumer_bytes
        );

        let rate = (bytes_len as f64 / consumer_latency as f64) * NS_IN_SECOND;
        debug!("Consumer throughput Bytes/s: {:?}", rate);
        self.consumer_rate_histogram.record(rate as u64).unwrap();
    }

    pub async fn create_topic(&mut self, option: &EnvironmentSetup) -> Result<(), ()> {
        use std::time::SystemTime;

        println!("Creating the topic: {}", &option.topic_name);

        let TestDriverType::Fluvio(fluvio_client) = self.client.as_ref();
        let admin = fluvio_client.admin().await;

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
            self.topic_create_latency_histogram
                .record(topic_time as u64)
                .unwrap();
            self.topic_num += 1;
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
