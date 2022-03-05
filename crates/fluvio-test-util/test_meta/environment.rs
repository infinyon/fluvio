use crate::setup::environment::{EnvironmentType};
use crate::test_runner::test_meta::FluvioTestMeta;
use structopt::StructOpt;
use std::fmt::Debug;
use std::num::ParseIntError;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use humantime::parse_duration;
use uuid::Uuid;

// Add # of topics
// Add topic partitions
// Add retention-time, segment-size

// Add # producers
// payload size
// producer batch-size, linger
// (reserve space for compression)

// Add # consumers

pub trait EnvDetail: Debug + Clone {
    fn set_topic_name(&mut self, topic: String);
    fn topic_name(&self) -> String;
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
    fn set_topic_name(&mut self, topic: String) {
        // Append a random string to the end. Multiple tests will use different topics
        let maybe_random = if self.topic_random {
            let random = Uuid::new_v4().to_simple().to_string();
            format!("{topic}-{random}")
        } else {
            topic
        };

        self.topic_name = Some(maybe_random);
    }

    // Return the topic name base
    fn topic_name(&self) -> String {
        if let Some(topic_name) = self.topic_name.clone() {
            topic_name
        } else {
            "topic".to_string()
        }
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

/// cli options
#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
pub struct EnvironmentSetup {
    /// Name of the test
    #[structopt(possible_values=&FluvioTestMeta::all_test_names())]
    pub test_name: String,

    /// Ensure that test starts with a new cluster before test.
    /// Will delete existing cluster. Implies `--cluster-start`
    #[structopt(long)]
    pub cluster_start_fresh: bool,

    /// attempt to start a new cluster before test
    #[structopt(short("s"), long)]
    pub cluster_start: bool,

    /// delete cluster after test
    #[structopt(short("d"), long)]
    pub cluster_delete: bool,

    /// topic name used
    #[structopt(short("t"), long)]
    pub topic_name: Option<String>,

    /// # topics - Appends id as "-#" (zero-based) to topic name if > 1
    #[structopt(long, default_value = "1")]
    pub num_topic: u16,

    /// Append random as "-<random>" to topic name (before id, if --num-topics > 1)
    #[structopt(long)]
    pub topic_random: bool,

    //    // This is for storing the random str for topic names
    //    topic_random_str: Option<String>,
    //
    /// Segment size (bytes) per topic
    #[structopt(long, default_value = "1000000000")]
    pub topic_segment_size: u32,

    /// Retention time per topic
    /// ex. 30s, 15m, 2h, 1w
    #[structopt(long, default_value = "7d" ,parse(try_from_str = parse_duration))]
    pub topic_retention: Duration,

    /// Number of replicas per topic
    #[structopt(short, long, default_value = "1")]
    pub replication: u16,

    /// Number of partitions per topic
    #[structopt(short, long, default_value = "1")]
    pub partition: u16,

    /// Number of spu
    #[structopt(long, default_value = "1")]
    pub spu: u16,

    /// # Producers to use (if test uses them)
    #[structopt(long, default_value = "1")]
    pub producer: u16,

    /// Producer batch size (bytes)
    #[structopt(long)]
    pub producer_batch_size: Option<u32>,

    /// Producer Linger (milliseconds)
    #[structopt(long)]
    pub producer_linger: Option<u32>,

    /// producer record size (bytes)
    #[structopt(long, default_value = "1000")]
    pub producer_record_size: u32,

    /// # Consumers to use (if test uses them)
    #[structopt(long, default_value = "1")]
    pub consumer: u16,

    // todo: add consumer config options
    /// enable tls
    #[structopt(long)]
    pub tls: bool,

    /// tls user, only used if tls is used
    #[structopt(long, default_value = "root")]
    pub tls_user: String,

    /// run local environment
    #[structopt(long)]
    pub local: bool,

    /// run develop image, this is for k8. (Run `make minikube_image` first.)
    #[structopt(long)]
    pub develop: bool,

    // log apply to fluvio client
    #[structopt(long)]
    pub client_log: Option<String>,

    // log apply to fluvio
    #[structopt(long)]
    pub server_log: Option<String>,

    // log dir
    #[structopt(long)]
    pub log_dir: Option<String>,

    /// authorization ConfigMap
    #[structopt(long)]
    pub authorization_config_map: Option<String>,

    /// skip pre-install checks
    #[structopt(long)]
    pub skip_checks: bool,

    /// Disable timeout - overrides use of `--timeout`
    #[structopt(long)]
    pub disable_timeout: bool,

    /// Global timeout for a test. Will report as fail when reached
    /// ex. 30s, 15m, 2h, 1w
    #[structopt(long, default_value = "1h" ,parse(try_from_str = parse_duration))]
    pub timeout: Duration,

    /// K8: use specific image version
    #[structopt(long)]
    pub image_version: Option<String>,

    /// K8: use sc address
    #[structopt(long)]
    pub proxy_addr: Option<String>,
}
