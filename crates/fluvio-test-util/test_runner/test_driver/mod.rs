//use fluvio::consumer::{PartitionSelectionStrategy, ConsumerConfig};

use tracing::debug;
use anyhow::Result;

use fluvio::consumer::PartitionSelectionStrategy;
use fluvio::{Fluvio};
use fluvio::metadata::topic::TopicSpec;
use fluvio::{TopicProducer, RecordKey, PartitionConsumer, MultiplePartitionConsumer};
use fluvio::TopicProducerConfig;
use fluvio::metadata::topic::CleanupPolicy;
use fluvio::metadata::topic::SegmentBasedPolicy;
use fluvio::metadata::topic::TopicStorageConfig;
use fluvio_types::PartitionId;

#[allow(unused_imports)]
use fluvio_command::CommandExt;
use crate::setup::TestCluster;
use crate::test_meta::TestCase;
use crate::test_meta::test_result::TestResult;
use crate::test_meta::environment::{EnvDetail, EnvironmentSetup};
use crate::test_meta::derive_attr::TestRequirements;

pub struct TestDriver {
    pub client: Option<Fluvio>,
    pub cluster: Option<TestCluster>,
}

// Using `.clone()` will always be disconnected.
// Must run `.connect()` before cluster operations
impl Clone for TestDriver {
    fn clone(&self) -> Self {
        if let Some(cluster_opts) = self.cluster.as_ref() {
            TestDriver::new(Some(cluster_opts.to_owned()))
        } else {
            TestDriver::new(None)
        }
    }
}

impl TestDriver {
    pub fn new(cluster_opt: Option<TestCluster>) -> Self {
        Self {
            client: None,
            cluster: cluster_opt,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let client = self.create_client().await?;

        self.client = Some(client);

        Ok(())
    }

    pub fn disconnect(&mut self) {
        self.client = None;
    }

    pub fn client(&self) -> &Fluvio {
        self.client
            .as_ref()
            .expect("Not connected to Fluvio cluster")
    }

    pub fn get_results(&self) -> TestResult {
        TestResult::default()
    }

    pub fn get_cluster(&self) -> Option<&TestCluster> {
        self.cluster.as_ref()
    }

    // Wrapper to getting a producer with config
    pub async fn create_producer_with_config(
        &self,
        topic: &str,
        config: TopicProducerConfig,
    ) -> TopicProducer {
        debug!(topic, "creating producer");
        let fluvio_client = self.create_client().await.expect("cant' create client");
        match fluvio_client
            .topic_producer_with_config(topic, config)
            .await
        {
            Ok(client) => {
                //self.producer_num += 1;
                client
            }
            Err(err) => {
                panic!("could not create producer: {err:#?}");
            }
        }
    }

    // Wrapper to getting a producer. We keep track of the number of producers we create
    pub async fn create_producer(&self, topic: &str) -> TopicProducer {
        self.create_producer_with_config(topic, Default::default())
            .await
    }

    // Wrapper to producer send. We measure the latency and accumulation of message payloads sent.
    pub async fn send_count(
        &self,
        p: &TopicProducer,
        key: RecordKey,
        message: Vec<u8>,
    ) -> Result<()> {
        use std::time::SystemTime;
        let now = SystemTime::now();

        let result = p.send(key, message.clone()).await;

        let _produce_time = now.elapsed().unwrap().as_nanos();

        //debug!(
        //    "(#{}) Produce latency (ns): {:?}",
        //    self.producer_latency_histogram.len() + 1,
        //    produce_time as u64
        //);

        //self.producer_latency_histogram
        //    .record(produce_time as u64)
        //    .unwrap();

        //self.producer_bytes += message.len();

        result?;
        Ok(())
    }

    pub async fn get_consumer(&self, topic: &str, partition: PartitionId) -> PartitionConsumer {
        let fluvio_client = self.create_client().await.expect("cant' create client");
        match fluvio_client
            .partition_consumer(topic.to_string(), partition)
            .await
        {
            Ok(client) => {
                //self.consumer_num += 1;
                client
            }
            Err(err) => {
                panic!("can't create consumer: {err:#?}");
            }
        }
    }

    // TODO: Create a multi-partition api w/ a list of partitions based off this
    pub async fn get_all_partitions_consumer(&self, topic: &str) -> MultiplePartitionConsumer {
        let fluvio_client = self.create_client().await.expect("cant' create client");
        match fluvio_client
            .consumer(PartitionSelectionStrategy::All(topic.to_string()))
            .await
        {
            Ok(client) => {
                //self.consumer_num += 1;
                client
            }
            Err(err) => {
                panic!("can't create consumer: {err:#?}");
            }
        }
    }

    // Re-enable when we re-enable metrics
    pub async fn consume_latency_record(&mut self, _latency: u64) {
        unimplemented!()
        //self.consumer_latency_histogram.record(latency).unwrap();
        //debug!(
        //    "(#{}) Recording consumer latency (ns): {:?}",
        //    self.consumer_latency_histogram.len(),
        //    latency
        //);
    }

    // Re-enable when we re-enable metrics
    pub async fn consume_bytes_record(&mut self, _bytes_len: usize) {
        unimplemented!()
        //self.consumer_bytes += bytes_len;
        //debug!(
        //    "Recording consumer bytes len: {:?} (total: {})",
        //    bytes_len, self.consumer_bytes
        //);
    }

    // This needs to support retention time and segment size
    // this can create multiple topics
    pub async fn create_topic(&self, option: &EnvironmentSetup) -> Result<(), ()> {
        use std::time::SystemTime;

        let topic_name = option.base_topic_name();

        if option.topic > 1 {
            println!(
                "Creating {} topics. Base name: {}",
                option.topic, &topic_name
            );
        } else {
            println!("Creating the topic: {}", &topic_name);
        }

        let admin = self.client().admin().await;

        let mut topic_spec =
            TopicSpec::new_computed(option.partition as u32, option.replication() as u32, None);

        // Topic Retention time
        topic_spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
            time_in_seconds: option.topic_retention.as_secs() as u32,
        }));

        // Topic segment size
        let storage = TopicStorageConfig {
            segment_size: Some(option.topic_segment_size),
            max_partition_size: Some(option.topic_max_partition_size),
        };
        topic_spec.set_storage(storage);

        for n in 0..option.topic {
            // Create topic and record how long it takes
            let now = SystemTime::now();

            let topic_name = if option.topic > 1 {
                format!("{}-{}", topic_name.clone(), n)
            } else {
                topic_name.clone()
            };

            let topic_create = admin
                .create(topic_name.clone(), false, topic_spec.clone())
                .await;

            let _topic_time = now.elapsed().unwrap().as_nanos();

            if topic_create.is_ok() {
                println!("topic \"{topic_name}\" created");
                //self.topic_create_latency_histogram
                //    .record(topic_time as u64)
                //    .unwrap();
                //self.topic_num += 1;
            } else {
                println!("topic \"{topic_name}\" already exists");
            }
        }

        Ok(())
    }

    pub fn is_env_acceptable(test_reqs: &TestRequirements, test_case: &TestCase) -> bool {
        // if `min_spu` undefined, min 1
        if let Some(min_spu) = test_reqs.min_spu {
            if min_spu > test_case.environment.spu() {
                println!("Test requires {min_spu} spu");
                return false;
            }
        }

        // if `cluster_type` undefined, no cluster restrictions
        // if `cluster_type = local` is defined, then environment must be local or skip
        // if `cluster_type = k8`, then environment must be k8 or skip
        if let Some(cluster_type) = &test_reqs.cluster_type {
            if &test_case.environment.cluster_type() != cluster_type {
                println!("Test requires cluster type {cluster_type:?} ");
                return false;
            }
        }

        true
    }

    /// create new fluvio client
    async fn create_client(&self) -> Result<Fluvio> {
        Fluvio::connect().await
    }
}
