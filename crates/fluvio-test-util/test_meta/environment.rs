use crate::setup::environment::{EnvironmentType};
use crate::test_runner::test_meta::FluvioTestMeta;
use clap::{Parser, builder::PossibleValuesParser};
use std::fmt::Debug;
use std::time::Duration;
use humantime::parse_duration;
use uuid::Uuid;
use fluvio::{Compression, DeliverySemantic};

pub trait EnvDetail: Debug + Clone {
    fn set_base_topic_name(&mut self, topic: String);
    fn base_topic_name(&self) -> String;
    fn topic_salt(&self) -> Option<String>;
    fn is_topic_set(&self) -> bool;
    fn replication(&self) -> u16;
    fn client_log(&self) -> Option<String>;
    fn spu(&self) -> u16;
    fn remove_cluster_before(&self) -> bool;
    fn cluster_start(&self) -> bool;
    fn cluster_delete(&self) -> bool;
    fn develop_mode(&self) -> bool;
    fn skip_checks(&self) -> bool;
    fn tls_user(&self) -> String;
    fn authorization_config_map(&self) -> Option<String>;
    fn server_log(&self) -> Option<String>;
    fn log_dir(&self) -> Option<String>;
    fn timeout(&self) -> Duration;
    fn set_timeout(&mut self, timeout: Duration);
    fn cluster_type(&self) -> EnvironmentType;
}

impl EnvDetail for EnvironmentSetup {
    // set the base topic name
    fn set_base_topic_name(&mut self, topic: String) {
        // Append a random string to the end. Multiple tests will use different topics
        let maybe_salted = if self.topic_random {
            let salt = Uuid::new_v4().simple().to_string();

            // Save the salt for tests to use
            self.topic_salt = Some(salt.clone());

            format!("{topic}-{salt}")
        } else {
            topic
        };

        self.topic_name = Some(maybe_salted);
    }

    // Return the topic name base
    fn base_topic_name(&self) -> String {
        if let Some(topic_name) = self.topic_name.clone() {
            topic_name
        } else {
            "topic".to_string()
        }
    }

    fn topic_salt(&self) -> Option<String> {
        self.topic_salt.clone()
    }

    fn is_topic_set(&self) -> bool {
        self.topic_name.is_some()
    }

    fn replication(&self) -> u16 {
        self.replication
    }

    fn client_log(&self) -> Option<String> {
        self.client_log.clone()
    }

    fn spu(&self) -> u16 {
        self.spu
    }

    // attempt to start new test cluster
    fn cluster_start(&self) -> bool {
        self.cluster_start
    }

    /// before we start test run, remove cluster
    fn remove_cluster_before(&self) -> bool {
        self.cluster_start_fresh
    }

    // delete test cluster after the test
    fn cluster_delete(&self) -> bool {
        self.cluster_delete
    }

    // For k8 cluster. Use development helm chart
    fn develop_mode(&self) -> bool {
        self.develop
    }

    fn skip_checks(&self) -> bool {
        self.skip_checks
    }

    fn tls_user(&self) -> String {
        self.tls_user.clone()
    }

    fn authorization_config_map(&self) -> Option<String> {
        self.authorization_config_map.clone()
    }

    fn server_log(&self) -> Option<String> {
        self.server_log.clone()
    }

    fn log_dir(&self) -> Option<String> {
        self.log_dir.clone()
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    fn cluster_type(&self) -> EnvironmentType {
        if self.local {
            EnvironmentType::Local
        } else {
            EnvironmentType::K8
        }
    }
}

// TODO: reserve space for compression
/// cli options
#[derive(Debug, Clone, Parser, Default, Eq, PartialEq)]
pub struct EnvironmentSetup {
    /// Name of the test
    #[clap(value_parser = PossibleValuesParser::new(FluvioTestMeta::all_test_names()))]
    pub test_name: String,

    /// Ensure that test starts with a new cluster before test.
    /// Will delete existing cluster. Implies `--cluster-start`
    #[clap(long)]
    pub cluster_start_fresh: bool,

    /// attempt to start a new cluster before test
    #[clap(short = 's', long)]
    pub cluster_start: bool,

    /// delete cluster after test
    #[clap(short = 'd', long)]
    pub cluster_delete: bool,

    /// topic name used
    #[clap(long)]
    pub topic_name: Option<String>,

    /// # topics - Appends id as "-#" (zero-based) to topic name if > 1
    #[clap(long, default_value = "1")]
    pub topic: u16,

    /// Append random as "-<random>" to topic name (before id, if --num-topics > 1)
    #[clap(long)]
    pub topic_random: bool,

    // This is used to randomize topic names
    #[clap(skip)]
    pub topic_salt: Option<String>,

    /// Segment size (bytes) per topic
    #[clap(long, default_value = "1000000000")]
    pub topic_segment_size: u32,

    /// Max partition size (bytes) per topic
    #[clap(long, default_value = "10000000000")]
    pub topic_max_partition_size: u64,

    /// Retention time per topic
    /// ex. 30s, 15m, 2h, 1w
    #[clap(long, default_value = "7d", value_parser=parse_duration)]
    pub topic_retention: Duration,

    /// Number of replicas per topic
    #[clap(short, long, default_value = "1")]
    pub replication: u16,

    /// Number of partitions per topic
    #[clap(short, long, default_value = "1")]
    pub partition: u16,

    /// Number of spu
    #[clap(long, default_value = "1")]
    pub spu: u16,

    /// # Producers to use (if test uses them)
    #[clap(long, default_value = "1")]
    pub producer: u32,

    /// Producer batch size (bytes)
    #[clap(long)]
    pub producer_batch_size: Option<usize>,

    /// Producer Linger (milliseconds)
    #[clap(long)]
    pub producer_linger: Option<u64>,

    /// producer record size (bytes)
    #[clap(long, default_value = "1000")]
    pub producer_record_size: usize,

    /// producer compression algorithm. (none, gzip, snappy or lz4)
    #[clap(long)]
    pub producer_compression: Option<Compression>,

    /// producer delivery semantic. (at-most-once, at-least-once)
    #[clap(long, default_value = "at-least-once")]
    pub producer_delivery_semantic: DeliverySemantic,

    /// # Consumers to use (if test uses them)
    #[clap(long, default_value = "1")]
    pub consumer: u32,

    // todo: add consumer config options
    /// enable tls
    #[clap(long)]
    pub tls: bool,

    /// tls user, only used if tls is used
    #[clap(long, default_value = "root")]
    pub tls_user: String,

    /// run local environment
    #[clap(long)]
    pub local: bool,

    /// run develop image, this is for k8. (Run `make minikube_image` first.)
    #[clap(long)]
    pub develop: bool,

    // log apply to fluvio client
    #[clap(long)]
    pub client_log: Option<String>,

    // log apply to fluvio
    #[clap(long)]
    pub server_log: Option<String>,

    // log dir
    #[clap(long)]
    pub log_dir: Option<String>,

    /// authorization ConfigMap
    #[clap(long)]
    pub authorization_config_map: Option<String>,

    /// skip pre-install checks
    #[clap(long)]
    pub skip_checks: bool,

    /// Disable timeout - overrides use of `--timeout`
    #[clap(long)]
    pub disable_timeout: bool,

    /// Global timeout for a test. Will report as fail when reached (unless --expect-timeout)
    /// ex. 30s, 15m, 2h, 1w
    #[clap(long, default_value = "1h", value_parser=parse_duration)]
    pub timeout: Duration,

    /// K8: use specific image version
    #[clap(long)]
    pub image_version: Option<String>,

    /// K8: use sc address
    #[clap(long)]
    pub proxy_addr: Option<String>,

    /// Will report fail unless test times out
    #[clap(long, conflicts_with = "expect_fail")]
    pub expect_timeout: bool,

    /// Expect a test to fail. (fail-> pass. pass or timeout -> fail)
    #[clap(long, conflicts_with = "expect_timeout")]
    pub expect_fail: bool,
}
