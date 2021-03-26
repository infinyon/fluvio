use std::fmt::Debug;

use structopt::StructOpt;
use structopt::clap::AppSettings;
use serde::{Serialize, Deserialize};
use syn::{AttributeArgs, Error as SynError, Lit, Meta, NestedMeta, Path, Result};
use syn::spanned::Spanned;
use std::any::Any;
use crate::setup::environment::EnvironmentType;

pub trait TestOption: Debug {
    fn as_any(&self) -> &dyn Any;
}

pub struct TestCase {
    pub environment: EnvironmentSetup,
    pub option: Box<dyn TestOption>,
}

impl TestCase {
    pub fn new(environment: EnvironmentSetup, option: Box<dyn TestOption>) -> Self {
        Self {
            environment,
            option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, PartialEq)]
pub enum TestCli {
    #[structopt(external_subcommand)]
    Args(Vec<String>),
}

impl Default for TestCli {
    fn default() -> Self {
        TestCli::Args(Vec::new())
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(
    name = "fluvio-test-runner",
    about = "Test fluvio platform",
    global_settings = &[AppSettings::ColoredHelp])]
pub struct BaseCli {
    pub test_name: String,

    #[structopt(flatten)]
    pub environment: EnvironmentSetup,

    #[structopt(subcommand)]
    pub test_cmd_args: Option<TestCli>,
}

pub trait EnvDetail: Debug + Clone {
    fn set_topic_name(&mut self, topic: String);
    fn topic_name(&self) -> String;
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
    fn timeout(&self) -> u16;
    fn set_timeout(&mut self, timeout: u16);
    fn cluster_type(&self) -> EnvironmentType;
}

impl EnvDetail for EnvironmentSetup {
    fn set_topic_name(&mut self, topic: String) {
        self.topic_name = topic;
    }

    fn topic_name(&self) -> String {
        self.topic_name.clone()
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

    fn timeout(&self) -> u16 {
        self.timeout
    }

    fn set_timeout(&mut self, timeout: u16) {
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
    /// don't attempt to delete cluster or start a new cluster before test
    /// topic creation will be skipped
    #[structopt(short, long)]
    pub disable_install: bool,

    /// don't delete cluster after test
    #[structopt(short, long)]
    pub keep_cluster: bool,

    /// topic name used
    #[structopt(short("t"), long, default_value = "topic")]
    pub topic_name: String,

    /// number of spu
    #[structopt(short, long, default_value = "1")]
    pub spu: u16,

    /// number of replicas
    #[structopt(short, long, default_value = "1")]
    pub replication: u16,

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
    #[structopt(long, default_value = "3600")]
    pub timeout: u16,
}
// TODO: Add timeout, cluster-type, test-name?
#[derive(Debug)]
pub enum TestRequirementAttribute {
    MinSpu(u16),
    Topic(String),
    Timeout(u16),
    ClusterType(EnvironmentType),
    TestName(String),
}

impl TestRequirementAttribute {
    fn from_ast(meta: Meta) -> Result<Self> {
        match meta {
            Meta::NameValue(name_value) => {
                let keys: String = TestRequirementAttribute::get_key(&name_value.path);

                if keys.as_str() == "min_spu" {
                    if let Lit::Int(min_spu) = name_value.lit {
                        Ok(Self::MinSpu(
                            u16::from_str_radix(min_spu.base10_digits(), 10).expect("Parse failed"),
                        ))
                    } else {
                        Err(SynError::new(name_value.span(), "Min spu must be LitInt"))
                    }
                } else if keys.as_str() == "topic" {
                    if let Lit::Str(str) = name_value.lit {
                        Ok(Self::Topic(str.value()))
                    } else {
                        Err(SynError::new(name_value.span(), "Topic must be a LitStr"))
                    }
                } else if keys.as_str() == "timeout" {
                    if let Lit::Int(timeout) = name_value.lit {
                        Ok(Self::Timeout(
                            u16::from_str_radix(timeout.base10_digits(), 10).expect("Parse failed"),
                        ))
                    } else {
                        Err(SynError::new(name_value.span(), "Timeout must be LitInt"))
                    }
                } else if keys.as_str() == "cluster_type" {
                    if let Lit::Str(str) = name_value.lit.clone() {
                        if str.value().to_lowercase() == "k8" {
                            Ok(Self::ClusterType(EnvironmentType::K8))
                        } else if str.value().to_lowercase() == "local" {
                            Ok(Self::ClusterType(EnvironmentType::Local))
                        } else {
                            Err(SynError::new(
                                name_value.span(),
                                "ClusterType values must be \"k8\" or \"local\". Don't define cluster_type if both.",
                            ))
                        }
                    } else {
                        Err(SynError::new(
                            name_value.span(),
                            "ClusterType must be a LitStr",
                        ))
                    }
                } else if keys.as_str() == "name" {
                    if let Lit::Str(str) = name_value.lit {
                        Ok(Self::TestName(str.value()))
                    } else {
                        Err(SynError::new(
                            name_value.span(),
                            "TestName must be a LitStr",
                        ))
                    }
                } else {
                    Err(SynError::new(name_value.span(), "Unsupported key"))
                }
            }
            _ => Err(SynError::new(meta.span(), "Unsupported attribute:")),
        }
    }

    fn get_key(p: &Path) -> String {
        let mut key: Vec<String> = p
            .segments
            .iter()
            .map(|args| args.ident.to_string())
            .collect();

        key.pop().expect("Key expected")
    }
}

// These are the arguments used by `#[fluvio_test()]
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TestRequirements {
    pub min_spu: Option<u16>,
    pub topic: Option<String>,
    pub timeout: Option<u16>,
    pub cluster_type: Option<EnvironmentType>,
    pub test_name: Option<String>,
}

impl TestRequirements {
    pub fn from_ast(args: AttributeArgs) -> Result<Self> {
        let mut attrs: Vec<TestRequirementAttribute> = Vec::new();

        for attr in args {
            match attr {
                NestedMeta::Meta(meta) => {
                    attrs.push(TestRequirementAttribute::from_ast(meta)?);
                }
                _ => return Err(SynError::new(attr.span(), "invalid syntax")),
            }
        }

        Ok(Self::from(attrs))
    }

    fn from(attrs: Vec<TestRequirementAttribute>) -> Self {
        let mut test_requirements = TestRequirements::default();

        for attr in attrs {
            if let TestRequirementAttribute::MinSpu(min_spu) = attr {
                test_requirements.min_spu = Some(min_spu)
            } else if let TestRequirementAttribute::Topic(topic) = attr {
                test_requirements.topic = Some(topic)
            } else if let TestRequirementAttribute::Timeout(timeout) = attr {
                test_requirements.timeout = Some(timeout)
            } else if let TestRequirementAttribute::ClusterType(cluster_type) = attr {
                test_requirements.cluster_type = Some(cluster_type)
            } else if let TestRequirementAttribute::TestName(name) = attr {
                test_requirements.test_name = Some(name)
            }
        }

        test_requirements
    }
}
