#[allow(unused_imports)]
use fluvio_command::CommandExt;
use crate::test_meta::{TestCase, TestOption, TestResult};
use crate::test_meta::environment::{EnvDetail, EnvironmentSetup};
use crate::test_meta::derive_attr::TestRequirements;
use fluvio::{Fluvio, FluvioError};
use std::sync::{Arc, RwLock};
use fluvio::metadata::topic::TopicSpec;
use hdrhistogram::Histogram;
use fluvio::{TopicProducer, RecordKey, PartitionConsumer};
use std::time::Duration;

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
    //pub topic_create_latency: Histogram<u64>,
}

impl FluvioTestDriver {
    pub fn get_results(&self) -> TestResult {
        TestResult::default()
    }

    // Wrapper to getting a producer. We keep track of the number of producers we create
    pub async fn get_producer(&mut self, topic: &str) -> TopicProducer {
        use fluvio_future::timer::sleep;

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

        let produce_time = now.elapsed().clone().unwrap().as_nanos();

        println!(
            "(#{}) Produce latency (ns): {:?}",
            self.produce_latency.len() + 1,
            produce_time as u64
        );

        self.produce_latency.record(produce_time as u64).unwrap();

        self.bytes_produced += message.len();

        result
    }

    pub async fn get_consumer(&mut self, topic: &str) -> PartitionConsumer {
        use fluvio_future::timer::sleep;

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

    // Might need to impl Stream so I can meter the consumer stream
}

#[derive(Debug)]
pub struct FluvioTestMeta {
    pub name: String,
    pub test_fn: fn(Arc<RwLock<FluvioTestDriver>>, TestCase) -> Result<TestResult, TestResult>,
    pub validate_fn: fn(Vec<String>) -> Box<dyn TestOption>,
    pub requirements: fn() -> TestRequirements,
    // Can't store Arc<Fluvio> bc of how we collect tests. Just too early to have a connection
    //// Can I hold onto Arc<Fluvio> so I can control producer and consumer creation?
    //pub client: Option<Arc<Fluvio>>,

    // TestResult?
    // producer count
    // consumer count
}

inventory::collect!(FluvioTestMeta);

impl FluvioTestMeta {
    pub fn all_test_names() -> Vec<&'static str> {
        inventory::iter::<Self>
            .into_iter()
            .map(|x| x.name.as_str())
            .collect::<Vec<&str>>()
    }

    pub fn from_name<S: AsRef<str>>(test_name: S) -> Option<&'static Self> {
        inventory::iter::<Self>
            .into_iter()
            .find(|t| t.name == test_name.as_ref())
    }

    // TODO: Expose # partition selection
    pub async fn create_topic(client: Arc<Fluvio>, option: &EnvironmentSetup) -> Result<(), ()> {
        if !option.is_benchmark() {
            println!("Creating the topic: {}", &option.topic_name);
        }

        let admin = client.admin().await;
        let topic_spec = TopicSpec::new_computed(1, option.replication() as i32, None);

        let topic_create = admin
            .create(option.topic_name.clone(), false, topic_spec)
            .await;

        if topic_create.is_ok() {
            if !option.is_benchmark() {
                println!("topic \"{}\" created", option.topic_name);
            }
        } else if !option.is_benchmark() {
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

        // Benchmark support is experimental!
        // Tests must opt-in to be run with the benchmark flag
        if test_case.environment.is_benchmark() {
            if let Some(opt_in) = test_reqs.benchmark {
                if !opt_in {
                    // Explicit opt-out
                    println!("Test `{}` opted out of benchmarks. Add `#[fluvio_test(benchmark=true)]` to test", test_case.environment.test_name);
                    return false;
                }
            } else {
                // Test is not opted into benchmark with attribute
                println!(
                    "Test `{}` must opt into benchmarks. Add `#[fluvio_test(benchmark=true)]` to test", test_case.environment.test_name
                );
                return false;
            }
        }

        true
    }

    pub fn set_topic(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        if let Some(topic) = &test_reqs.topic {
            test_case.environment.set_topic_name(topic.to_string());
        }
    }

    pub fn set_timeout(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        // Set timer
        if let Some(timeout) = test_reqs.timeout {
            test_case.environment.set_timeout(timeout)
        }
    }

    pub fn customize_test(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        Self::set_topic(test_reqs, test_case);
        Self::set_timeout(test_reqs, test_case);
    }
}
