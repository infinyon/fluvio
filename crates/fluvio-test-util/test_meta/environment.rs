use crate::setup::environment::{EnvironmentType};
use crate::test_runner::test_meta::FluvioTestMeta;
use structopt::StructOpt;
use std::fmt::Debug;
use std::num::ParseIntError;
use std::time::Duration;

pub trait EnvDetail: Debug + Clone {
    fn set_topic_name(&mut self, topic: String);
    fn topic_name(&self) -> String;
    fn is_topic_set(&self) -> bool;
    fn replication(&self) -> u16;
    fn client_log(&self) -> Option<String>;
    fn spu(&self) -> u16;
    fn skip_cluster_start(&self) -> bool;
    fn remove_cluster_before(&self) -> bool;
    fn skip_cluster_delete(&self) -> bool;
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
    fn set_topic_name(&mut self, topic: String) {
        self.topic_name = Some(topic);
    }

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

    // don't attempt to clean up and start new test cluster
    // don't create a topic
    fn skip_cluster_start(&self) -> bool {
        self.disable_install
    }

    /// before we start test run, remove cluster
    // don't create a topic
    fn remove_cluster_before(&self) -> bool {
        !self.disable_install
    }

    // don't attempt to delete test cluster
    fn skip_cluster_delete(&self) -> bool {
        self.keep_cluster
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

    /// don't attempt to delete cluster or start a new cluster before test
    /// topic creation will be skipped
    #[structopt(short, long)]
    pub disable_install: bool,

    /// don't delete cluster after test
    #[structopt(short, long)]
    pub keep_cluster: bool,

    /// topic name used
    #[structopt(short("t"), long)]
    pub topic_name: Option<String>,

    /// number of spu
    #[structopt(short, long, default_value = "1")]
    pub spu: u16,

    /// number of replicas
    #[structopt(short, long, default_value = "1")]
    pub replication: u16,

    /// number of partitions
    #[structopt(short, long, default_value = "1")]
    pub partition: u16,

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

    /// In seconds, the maximum time a test will run before considered a fail (default: 1 hour)
    #[structopt(long, parse(try_from_str = parse_timeout_seconds), default_value = "3600")]
    pub timeout: Duration,

    /// K8: use specific image version
    #[structopt(long)]
    pub image_version: Option<String>,

    /// K8: use sc address
    #[structopt(long)]
    pub proxy_addr: Option<String>,
}

#[allow(clippy::unnecessary_wraps)]
fn parse_timeout_seconds(timeout_str: &str) -> Result<Duration, ParseIntError> {
    let parsed = timeout_str.parse::<u64>().expect("Parsing seconds failed");
    Ok(Duration::from_secs(parsed))
}
