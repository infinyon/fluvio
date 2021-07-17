#[allow(unused_imports)]
use fluvio_command::CommandExt;
use crate::test_meta::test_timer::TestTimer;
use crate::test_meta::{TestCase, test_result::TestResult, chart_builder::FluvioTimeData};
use crate::test_meta::environment::{EnvDetail, EnvironmentSetup};
use crate::test_meta::derive_attr::TestRequirements;
use fluvio::{Fluvio, FluvioError};
use std::sync::Arc;
use fluvio::metadata::topic::TopicSpec;
use hdrhistogram::Histogram;
use fluvio::{TopicProducer, RecordKey, PartitionConsumer};
use std::time::{Duration, SystemTime};
use tracing::debug;
use fluvio_future::timer::sleep;

// # of nanoseconds in a millisecond
const NANOS_IN_MILLIS: f32 = 1_000_000.0;

#[derive(Clone)]
pub struct FluvioTestDriver {
    pub client: Arc<Fluvio>,
    pub timer: TestTimer,
    pub topic_num: usize,
    pub producer_num: usize,
    pub consumer_num: usize,
    pub producer_bytes: usize,
    pub consumer_bytes: usize,
    pub producer_latency: Histogram<u64>,
    pub producer_time_latency: Vec<FluvioTimeData>,
    pub consumer_latency: Histogram<u64>,
    pub consumer_time_latency: Vec<FluvioTimeData>,
    pub e2e_latency: Histogram<u64>,
    pub e2e_time_latency: Vec<FluvioTimeData>,
    pub topic_create_latency: Histogram<u64>,
    pub producer_rate: Histogram<u64>,
    pub producer_time_rate: Vec<FluvioTimeData>,
    pub consumer_rate: Histogram<u64>,
    pub consumer_time_rate: Vec<FluvioTimeData>,
    pub memory_usage: Histogram<u64>,
    pub memory_time_usage: Vec<FluvioTimeData>,
    pub cpu_usage: Histogram<u64>,
    pub cpu_time_usage: Vec<FluvioTimeData>,
}

impl FluvioTestDriver {
    pub fn new(client: Arc<Fluvio>) -> Self {
        Self {
            client,
            timer: TestTimer::new(),
            topic_num: 0,
            producer_num: 0,
            consumer_num: 0,
            producer_bytes: 0,
            consumer_bytes: 0,
            producer_latency: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            producer_time_latency: Vec::new(),
            consumer_latency: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consumer_time_latency: Vec::new(),
            e2e_latency: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            e2e_time_latency: Vec::new(),
            topic_create_latency: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            producer_rate: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            producer_time_rate: Vec::new(),
            consumer_rate: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consumer_time_rate: Vec::new(),
            memory_usage: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            memory_time_usage: Vec::new(),
            cpu_usage: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            cpu_time_usage: Vec::new(),
        }
    }

    pub async fn start_system_poll(&self) {
        while self.is_test_running() {
            println!("Poll");
            sleep(Duration::from_millis(500));
        }
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

    pub fn get_results(&self) -> TestResult {
        TestResult {
            success: false,
            duration: Duration::new(0, 0),
            topic_num: self.topic_num as u64,
            producer_num: self.producer_num as u64,
            consumer_num: self.consumer_num as u64,
            producer_bytes: self.producer_bytes as u64,
            consumer_bytes: self.consumer_bytes as u64,
            producer_latency_histogram: self.producer_latency.clone(),
            producer_time_latency: self.producer_time_latency.clone(),
            consumer_latency_histogram: self.consumer_latency.clone(),
            consumer_time_latency: self.consumer_time_latency.clone(),
            e2e_latency_histogram: self.e2e_latency.clone(),
            e2e_time_latency: self.consumer_time_latency.clone(),
            topic_create_latency_histogram: self.topic_create_latency.clone(),
            producer_rate_histogram: self.producer_rate.clone(),
            producer_time_rate: self.producer_time_rate.clone(),
            consumer_rate_histogram: self.consumer_rate.clone(),
            consumer_time_rate: self.consumer_time_rate.clone(),
            memory_usage_histogram: self.memory_usage.clone(),
            memory_time_usage: self.memory_time_usage.clone(),
            cpu_usage_histogram: self.cpu_usage.clone(),
            cpu_time_usage: self.cpu_time_usage.clone(),
        }
    }

    // Wrapper to getting a producer. We keep track of the number of producers we create
    pub async fn get_producer(&mut self, topic: &str) -> TopicProducer {
        match self.client.topic_producer(topic).await {
            Ok(client) => {
                self.producer_num += 1;
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
        let bytes_sent = message.as_bytes().len() as u64;

        let now = SystemTime::now();
        let result = p.send(key, message).await;
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

    pub async fn get_consumer(&mut self, topic: &str, partition: i32) -> PartitionConsumer {
        match self
            .client
            .partition_consumer(topic.to_string(), partition)
            .await
        {
            Ok(client) => {
                self.consumer_num += 1;
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

    // This doesn't work yet
    //pub async fn stream_count(&mut self, consumer: PartitionConsumer, offset: Offset) {
    //    use std::time::SystemTime;
    //    let mut stream = consumer.stream(offset).await.expect("stream");

    //    loop {
    //        // Take a timestamp
    //        let now = SystemTime::now();

    //        if let Some(Ok(record)) = stream.next().await {
    //            // Record latency
    //            let consume_time = now.elapsed().clone().unwrap().as_nanos();
    //            self.consume_latency.record(consume_time as u64).unwrap();

    //            // Record bytes consumed
    //            self.bytes_consumed += record.as_ref().len();
    //        } else {
    //            debug!("No more bytes left to consume");
    //            break;
    //        }
    //    }
    //}

    // TODO: This is a workaround. Handle stream inside impl
    pub async fn consume_record(
        &mut self,
        bytes_len: usize,
        consumer_latency: u64,
        e2e_latency: u64,
    ) {
        let now = self.test_elapsed().as_nanos() as f32 / NANOS_IN_MILLIS;

        self.consumer_latency.record(consumer_latency).unwrap();

        // Make both use milliseconds
        self.consumer_time_latency.push(FluvioTimeData {
            test_elapsed_ms: now,
            data: (consumer_latency as f32) / NANOS_IN_MILLIS,
        });
        self.consumer_time_rate.push(FluvioTimeData {
            test_elapsed_ms: now,
            data: bytes_len as f32,
        });
        self.e2e_latency.record(e2e_latency).unwrap();

        // Make both use milliseconds
        self.e2e_time_latency.push(FluvioTimeData {
            test_elapsed_ms: now,
            data: (e2e_latency as f32) / NANOS_IN_MILLIS,
        });
        debug!(
            "(#{}) Recording consumer latency (ns): {:?}",
            self.consumer_latency.len(),
            consumer_latency
        );

        self.consumer_bytes += bytes_len;
        debug!(
            "Recording consumer bytes len: {:?} (total: {})",
            bytes_len, self.consumer_bytes
        );

        debug!(
            "(#{}) Recording e2e (ns): {:?}",
            self.e2e_latency.len(),
            e2e_latency
        );

        // Convert Bytes/ns to Bytes/s
        // 1_000_000_000 ns in 1 second
        const NS_IN_SECOND: f64 = 1_000_000_000.0;
        let rate = (bytes_len as f64 / consumer_latency as f64) * NS_IN_SECOND;
        debug!("Consumer throughput Bytes/s: {:?}", rate);
        self.consumer_rate.record(rate as u64).unwrap();
    }

    pub async fn create_topic(&mut self, option: &EnvironmentSetup) -> Result<(), ()> {
        println!("Creating the topic: {}", &option.topic_name);

        let admin = self.client.admin().await;

        let topic_spec =
            TopicSpec::new_computed(option.partition as i32, option.replication() as i32, None);

        // Create topic and record how long it takes
        let now = SystemTime::now();
        //let timestamp = self.test_elapsed().as_millis();

        let topic_create = admin
            .create(option.topic_name.clone(), false, topic_spec)
            .await;

        let topic_time = now.elapsed().unwrap().as_nanos();

        if topic_create.is_ok() {
            println!(
                "topic \"{}\" created. Replication: {} Partition: {}",
                option.topic_name,
                option.replication(),
                option.partition
            );
            self.topic_create_latency.record(topic_time as u64).unwrap();
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

    pub fn generate_message() {}

    pub fn validate_message() {}
}
