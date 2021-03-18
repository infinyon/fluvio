use std::error::Error;
use std::collections::HashMap;

use structopt::StructOpt;
use structopt::clap::AppSettings;
use serde::{Serialize, Deserialize};
use syn::{AttributeArgs, Error as SynError, Lit, Meta, NestedMeta, Path, Result};
use syn::spanned::Spanned;

#[derive(Debug, Clone, StructOpt, PartialEq)]
pub enum TestCli {
    #[structopt(external_subcommand)]
    CliCmd(Vec<String>),
}

impl Default for TestCli {
    fn default() -> Self {
        TestCli::CliCmd(Vec::new())
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(
    name = "fluvio-test-runner",
    about = "Test fluvio platform",
    global_settings = &[AppSettings::ColoredHelp])]
pub struct CliArgs {
    #[structopt(flatten)]
    pub environment: EnvironmentSetup,

    #[structopt(subcommand)]
    pub test_cmd: TestCli,
    //#[structopt(flatten)]
    //pub test_vars: RawTestVars,
}

#[derive(Debug, Clone)]
pub struct TestCase {
    pub environment: EnvironmentSetup,
    pub name: String,
    //pub vars: HashMap<String, String>,
}

// Structopt doesn't support parsing into collections, so we'll do this in 2 steps
//#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
//pub struct RawTestVars {
//    #[structopt(short="v", long="var", parse(try_from_str = parse_key_val), number_of_values = 1)]
//    pub test_var: Vec<(String, String)>,
//}

//impl RawTestVars {
//    pub fn into_hashmap(self) -> HashMap<String, String> {
//        self.test_var.into_iter().collect()
//    }
//}

//impl From<CliArgs> for TestCase {
//    fn from(args: CliArgs) -> Self {
//
//
//
//        TestCase {
//            environment: args.environment,
//            name: args.test_name,
//            vars: args.test_vars.into_hashmap(),
//        }
//    }
//}

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

///// Parse a single "key=value" pair from structopt
//#[allow(clippy::unnecessary_wraps)]
//fn parse_key_val<T, U>(s: &str) -> Result<(T, U)>
//where
//    T: std::str::FromStr,
//    T::Err: Error + 'static,
//    U: std::str::FromStr,
//    U::Err: Error + 'static,
//{
//    let pos = s
//        .find('=')
//        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))
//        .expect("");
//    Ok((s[..pos].parse().expect(""), s[pos + 1..].parse().expect("")))
//}

impl TestCase {
    /// before we start test run, remove cluster
    // don't create a topic
    pub fn remove_cluster_before(&self) -> bool {
        !self.environment.disable_install
    }

    // don't attempt to clean up and start new test cluster
    // don't create a topic
    pub fn skip_cluster_start(&self) -> bool {
        self.environment.disable_install
    }

    // don't attempt to delete test cluster
    pub fn skip_cluster_delete(&self) -> bool {
        self.environment.keep_cluster
    }

    // For k8 cluster. Use development helm chart
    pub fn develop_mode(&self) -> bool {
        self.environment.develop
    }

    pub fn topic_name(&self) -> String {
        self.environment.topic_name.clone()
    }
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
