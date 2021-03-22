use std::fmt::Debug;

use structopt::StructOpt;
use structopt::clap::AppSettings;
use serde::{Serialize, Deserialize};
use syn::{AttributeArgs, Error as SynError, Lit, Meta, NestedMeta, Path, Result};
use syn::spanned::Spanned;
use std::any::Any;

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
}
// TODO: Add timeout, cluster-type?
#[derive(Debug)]
pub enum TestRequirementAttribute {
    MinSpu(u16),
    Topic(String),
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
            }
        }

        test_requirements
    }
}
