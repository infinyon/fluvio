use structopt::StructOpt;
use serde::{Serialize, Deserialize};
use syn::{AttributeArgs, Error, Lit, Meta, NestedMeta, Path, Result};
use syn::spanned::Spanned;

#[derive(Debug, Clone, StructOpt)]
pub struct ProductOption {
    /// number of produce iteration for a single producer
    #[structopt(short, long, default_value = "1")]
    pub produce_iteration: u16,

    // record size
    #[structopt(long, default_value = "100")]
    pub record_size: usize,

    // number of parallel producer
    #[structopt(long, default_value = "1")]
    pub producer_count: u16,
}

impl Default for ProductOption {
    fn default() -> Self {
        ProductOption {
            produce_iteration: 1,
            record_size: 100,
            producer_count: 1,
        }
    }
}

/// cli options
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "fluvio-test-runner", about = "Test fluvio platform")]
pub struct TestOption {
    #[structopt(flatten)]
    pub produce: ProductOption,

    /// don't attempt to delete cluster or start a new cluster before test
    /// topic creation will be skipped
    #[structopt(short, long)]
    pub disable_install: bool,

    /// don't delete cluster after test
    #[structopt(short, long)]
    pub keep_cluster: bool,

    /// Smoke test specific
    /// don't produce message
    #[structopt(long)]
    pub disable_produce: bool,

    /// Smoke test specific
    /// don't test consumer
    #[structopt(long)]
    pub disable_consume: bool,

    #[structopt(short, long)]
    /// replication count, number of spu will be same as replication count, unless overridden
    pub replication: Option<u16>,

    /// topic name used
    #[structopt(short("t"), long, default_value = "topic")]
    pub topic_name: String,

    /// Smoke test specific
    /// if this is turn on, consumer waits for producer to finish before starts consumption
    /// if iterations are long then consumer may receive large number of batches
    #[structopt(long)]
    pub consumer_wait: bool,

    /// number of spu
    #[structopt(short, long, default_value = "1")]
    pub spu: u16,

    /// enable tls
    #[structopt(long)]
    pub tls: bool,

    /// tls user, only used if tls is used
    #[structopt(long, default_value = "root")]
    pub tls_user: String,

    /// run local environment
    #[structopt(long)]
    pub local: bool,

    /// run develop image, this is for k8
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

    // We want to collect the tests and offer them here as choices
    #[structopt(possible_values = &["smoke", "concurrent", "many_producers"])]
    pub test_name: Option<String>,
}

impl Default for TestOption {
    fn default() -> Self {
        TestOption {
            produce: ProductOption::default(),
            disable_install: false,
            keep_cluster: false,
            disable_produce: false,
            disable_consume: false,
            replication: None,
            topic_name: String::from("default"),
            consumer_wait: false,
            spu: 1,
            tls: false,
            tls_user: String::from("root"),
            local: false,
            develop: false,
            client_log: Some(String::from("warn")),
            server_log: Some(String::from("fluvio=debug")),
            log_dir: None,
            authorization_config_map: None,
            skip_checks: false,
            test_name: None,
        }
    }
}

impl TestOption {
    /// return SC configuration or exist program.
    pub fn parse_cli_or_exit() -> Self {
        Self::from_args()
    }

    /// Smoke test specific
    pub fn test_consumer(&self) -> bool {
        !self.disable_consume
    }

    /// before we start test run, remove cluster
    // don't create a topic
    pub fn remove_cluster_before(&self) -> bool {
        !self.disable_install
    }

    // don't attempt to clean up and start new test cluster
    // don't create a topic
    pub fn skip_cluster_start(&self) -> bool {
        self.disable_install
    }

    // don't attempt to delete test cluster
    pub fn skip_cluster_delete(&self) -> bool {
        self.keep_cluster
    }

    pub fn tls(&self) -> bool {
        self.tls
    }

    pub fn replication(&self) -> u16 {
        self.replication.unwrap_or(self.spu)
    }

    /// Smoke test specific
    pub fn produce(&self) -> bool {
        !self.disable_produce
    }

    pub fn develop_mode(&self) -> bool {
        self.develop
    }

    pub fn topic_name(&self) -> String {
        self.topic_name.clone()
    }

    pub fn use_cli(&self) -> bool {
        self.produce.produce_iteration == 1
    }

    pub fn skip_checks(&self) -> bool {
        self.skip_checks
    }
}

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
                        Err(Error::new(name_value.span(), "Min spu must be LitInt"))
                    }
                } else if keys.as_str() == "topic" {
                    if let Lit::Str(str) = name_value.lit {
                        Ok(Self::Topic(str.value()))
                    } else {
                        Err(Error::new(name_value.span(), "Topic must be a LitStr"))
                    }
                } else {
                    Err(Error::new(name_value.span(), "Unsupported key"))
                }
            }
            _ => Err(Error::new(meta.span(), "Unsupported attribute:")),
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
                _ => return Err(Error::new(attr.span(), "invalid syntax")),
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

//impl ToTokens for TestRequirements {
//    fn to_tokens(&self, tokens: &mut TokenStream) {
//
//        let min_spu = self.min_spu.clone();
//        let topic = self.topic.clone();
//
//        let s = quote! { fluvio_test_util::test_meta::TestRequirements {
//            min_spu: #min_spu,
//            topic: #topic,
//        }};
//
//        tokens.append_all(s.into());
//    }
//}
