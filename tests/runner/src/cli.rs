use structopt::StructOpt;

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

/// cli options
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "fluvio-test-runner", about = "Test fluvio platform")]
pub struct TestOption {
    #[structopt(flatten)]
    pub produce: ProductOption,

    /// don't install and uninstall
    #[structopt(short, long)]
    disable_install: bool,

    /// don't produce message
    #[structopt(long)]
    disable_produce: bool,

    /// don't test consumer
    #[structopt(long)]
    disable_consume: bool,

    #[structopt(short, long)]
    /// replication count, number of spu will be same as replication count, unless overridden
    replication: Option<u16>,

    /// base topic name used.  if replication > 1, then topic name will be named: topic<replication>
    #[structopt(short("t"), long, default_value = "topic")]
    pub topic_name: String,

    /// if this is turn on, consumer waits for producer to finish before starts consumption
    /// if iterations are long then consumer may receive large number of batches
    #[structopt(long)]
    pub consumer_wait: bool,
    /// number of spu
    #[structopt(short, long, default_value = "1")]
    pub spu: u16,

    /// enable tls
    #[structopt(long)]
    tls: bool,

    /// tls user, only used if tls is used
    #[structopt(long, default_value = "root")]
    pub tls_user: String,

    /// run local environment
    #[structopt(long)]
    local: bool,

    /// run develop image, this is for k8
    #[structopt(long)]
    develop: bool,

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
    skip_checks: bool,
}

impl TestOption {
    /// return SC configuration or exist program.
    pub fn parse_cli_or_exit() -> Self {
        Self::from_args()
    }

    pub fn test_consumer(&self) -> bool {
        !self.disable_consume
    }

    /// before we start test run, remove cluster
    pub fn remove_cluster_before(&self) -> bool {
        !self.disable_install
    }

    // do the setup (without cleanup)
    pub fn setup(&self) -> bool {
        !self.disable_install
    }

    pub fn init_topic(&self) -> bool {
        !self.disable_install
    }

    pub fn terminate_after_consumer_test(&self) -> bool {
        !self.disable_install
    }

    pub fn tls(&self) -> bool {
        self.tls
    }

    pub fn replication(&self) -> u16 {
        self.replication.unwrap_or(self.spu)
    }

    pub fn produce(&self) -> bool {
        !self.disable_produce
    }

    /// use k8 env driver
    pub fn use_k8_driver(&self) -> bool {
        !self.local
    }

    pub fn develop_mode(&self) -> bool {
        self.develop
    }

    /// topic name based on index
    pub fn topic_name(&self, index: u16) -> String {
        format!("{}{}", self.topic_name, index)
    }

    pub fn use_cli(&self) -> bool {
        self.produce.produce_iteration == 1
    }

    pub fn skip_checks(&self) -> bool {
        self.skip_checks
    }
}
