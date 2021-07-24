#[allow(unused_imports)]
use fluvio_command::CommandExt;
use pulsar::producer::SendFuture;
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

use serde::{Serialize, Deserialize};
use pulsar::{
    message::proto, producer, producer::Producer as PulsarProducer, Error as PulsarError, Pulsar,
    SerializeMessage, AsyncStdExecutor, Consumer as PulsarConsumer, DeserializeMessage,
    message::Payload,
};

use std::future::Future;
use std::pin::Pin;
use rdkafka::config::ClientConfig as KafkaClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer as KafkaConsumer, DefaultConsumerContext};
use rdkafka::producer::{FutureProducer as KafkaProducer, FutureRecord as KafkaRecord};
use rdkafka::producer::future_producer::OwnedDeliveryResult as KafkaProducerResult;
use rdkafka::util::AsyncRuntime;

// # of nanoseconds in a millisecond
const NANOS_IN_MILLIS: f32 = 1_000_000.0;

pub enum TestDriverType {
    Fluvio(Fluvio),
    Pulsar,
    Kafka,
}

pub enum TestProducer {
    Fluvio(TopicProducer),
    Pulsar(PulsarProducer<AsyncStdExecutor>),
    Kafka(KafkaProducer),
}

pub enum TestConsumer {
    Fluvio(PartitionConsumer),
    Pulsar(PulsarConsumer<PulsarTestData, AsyncStdExecutor>),
    Kafka(KafkaConsumer<DefaultConsumerContext, KafkaAsyncStdRuntime>),
}

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

#[derive(Clone)]
pub struct FluvioTestDriver {
    pub client: Arc<TestDriverType>,
    other_cluster_addr: Option<String>,
    producer_batch_kbytes: usize,
    producer_batch_ms: u64,
    producer_record_bytes: usize,
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
    pub fn new(client: Arc<TestDriverType>) -> Self {
        Self {
            client,
            other_cluster_addr: None,
            producer_batch_kbytes: 10,
            producer_batch_ms: 10,
            producer_record_bytes: 1000,
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

    pub fn with_cluster_addr(mut self, addr: Option<String>) -> Self {
        self.other_cluster_addr = addr;
        self
    }

    pub fn with_producer_batch_size(mut self, size_kb: usize) -> Self {
        self.producer_batch_kbytes = size_kb;
        self
    }

    pub fn with_producer_batch_ms(mut self, time_ms: u64) -> Self {
        self.producer_batch_ms = time_ms;
        self
    }

    pub fn with_producer_record_size(mut self, size_bytes: usize) -> Self {
        self.producer_record_bytes = size_bytes;
        self
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
    pub async fn get_producer(&mut self, topic: &str) -> TestProducer {
        match self.client.as_ref() {
            TestDriverType::Fluvio(fluvio_client) => {
                match fluvio_client.topic_producer(topic).await {
                    Ok(producer) => {
                        self.producer_num += 1;
                        return TestProducer::Fluvio(producer);
                    }
                    Err(err) => {
                        println!(
                            "unable to get producer to topic: {}, error: {} sleeping 10 second ",
                            topic, err
                        );
                        sleep(Duration::from_secs(10)).await;
                    }
                }
            }
            TestDriverType::Pulsar => {
                let addr = match &self.other_cluster_addr {
                    Some(addr) => addr,
                    None => "pulsar://127.0.0.1:6650",
                };

                let pulsar: Pulsar<_> = Pulsar::builder(addr, AsyncStdExecutor)
                    .build()
                    .await
                    .expect("Failed to make Pulsar builder");

                // Based on looking at the Pulsar-rs producer code, it seems that batch size is based on # of messages, not bytes
                let calc_batch_size =
                    (self.producer_batch_kbytes * 1000) / self.producer_record_bytes;
                debug!(
                    "Using calculated batch size of {} messages",
                    calc_batch_size
                );

                let producer = pulsar
                    .producer()
                    .with_topic(topic)
                    .with_name("flv-test-pulsar")
                    .with_options(producer::ProducerOptions {
                        schema: Some(proto::Schema {
                            r#type: proto::schema::Type::String as i32,
                            ..Default::default()
                        }),
                        compression: None,
                        batch_size: Some(calc_batch_size as u32),
                        ..Default::default()
                    })
                    .build()
                    .await
                    .expect("Failed to create producer");

                self.producer_num += 1;
                return TestProducer::Pulsar(producer);
            }

            TestDriverType::Kafka => {
                let broker = match &self.other_cluster_addr {
                    Some(addr) => addr,
                    None => "localhost:9092",
                };

                // https://docs.rs/rdkafka/0.26.0/rdkafka/producer/index.html#producer-configuration
                let producer = KafkaClientConfig::new()
                    .set("bootstrap.servers", broker)
                    .set("message.timeout.ms", "5000")
                    //.set("request.required.ack", "1")
                    .set(
                        "queue.buffering.max.kbytes",
                        format!("{}", self.producer_batch_kbytes),
                    )
                    .set(
                        "queue.buffering.max.ms",
                        format!("{}", self.producer_batch_ms),
                    )
                    .create()
                    .expect("Kafka producer creation failed");

                self.producer_num += 1;
                return TestProducer::Kafka(producer);
            }
        }
        panic!("can't get producer");
    }

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

    pub async fn pulsar_send(
        &mut self,
        p: &mut PulsarProducer<AsyncStdExecutor>,
        //msg_buf: Vec<(RecordKey, Vec<u8>)>,
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

    pub async fn kafka_send(
        &mut self,
        p: &mut KafkaProducer,
        topic: &str,
        msg_buf: Vec<u8>,
    ) -> KafkaProducerResult {
        //let bytes_sent: usize = msg_buf
        //    .iter()
        //    .map(|m| &m.data)
        //    .fold(0, |total, msg| total + msg.len());
        let bytes_sent: usize = msg_buf.len();

        let now = SystemTime::now();

        //let result = p.send_all(msg_buf).await.expect("Send failed");
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

    pub async fn get_consumer(&mut self, topic: &str, partition: i32) -> TestConsumer {
        match self.client.as_ref() {
            TestDriverType::Fluvio(fluvio_client) => {
                match fluvio_client
                    .partition_consumer(topic.to_string(), partition)
                    .await
                {
                    Ok(client) => {
                        self.consumer_num += 1;
                        return TestConsumer::Fluvio(client);
                    }
                    Err(err) => {
                        panic!("unable to get consumer to topic: {}, error: {}", topic, err);
                    }
                }
            }
            TestDriverType::Pulsar => {
                let addr = match &self.other_cluster_addr {
                    Some(addr) => addr,
                    None => "pulsar://localhost:6650",
                };

                let pulsar: Pulsar<_> = Pulsar::builder(addr, AsyncStdExecutor)
                    .build()
                    .await
                    .expect("Failed to create Pulsar builder");

                let consumer: PulsarConsumer<PulsarTestData, _> = pulsar
                    .consumer()
                    .with_topic(topic)
                    .with_consumer_name("flv-test-kafka-consumer")
                    .with_subscription("flv-test-kafka-subscription")
                    .build()
                    .await
                    .expect("Failed to create Pulsar consumer");

                self.consumer_num += 1;
                TestConsumer::Pulsar(consumer)
            }
            TestDriverType::Kafka => {
                let broker = match &self.other_cluster_addr {
                    Some(addr) => addr,
                    None => "localhost:9092",
                };

                // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                let consumer: KafkaConsumer<_, KafkaAsyncStdRuntime> = KafkaClientConfig::new()
                    .set("bootstrap.servers", broker)
                    .set("session.timeout.ms", "6000")
                    .set("enable.auto.commit", "false")
                    .set("auto.offset.reset", "earliest")
                    .set("group.id", "flv-test-kafka")
                    .create()
                    .expect("Consumer creation failed");

                consumer.subscribe(&[topic]).unwrap();

                self.consumer_num += 1;
                TestConsumer::Kafka(consumer)
            }
        }
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
        match self.client.as_ref() {
            TestDriverType::Fluvio(fluvio_client) => {
                println!("Creating the topic: {}", &option.topic_name);
                let admin = fluvio_client.admin().await;

                let topic_spec = TopicSpec::new_computed(
                    option.partition as i32,
                    option.replication() as i32,
                    None,
                );

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
            }
            _ => {
                println!("No topic creation support for other cluster types");
            }
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
