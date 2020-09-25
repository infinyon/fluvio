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

    /// topic name used for testing
    #[structopt(short("t"), long, default_value = "topic")]
    pub topic_name: String,

    /// number of spu
    #[structopt(short, long, default_value = "1")]
    pub spu: u16,

    /// enable tls
    #[structopt(long)]
    tls: bool,

    /// run local environment
    #[structopt(long)]
    local_driver: bool,

    /// run develop image, this is for k8
    #[structopt(long)]
    develop: bool,

    // rust log
    #[structopt(long)]
    pub rust_log: Option<String>,

    // log dir
    #[structopt(long)]
    pub log_dir: Option<String>,
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
        self.replication.unwrap_or_else(|| self.spu)
    }

    pub fn produce(&self) -> bool {
        !self.disable_produce
    }

    /// use k8 env driver
    pub fn use_k8_driver(&self) -> bool {
        !self.local_driver
    }

    pub fn develop_mode(&self) -> bool {
        self.develop
    }

    /// topic name based on index
    pub fn topic_name(&self, index: u16) -> String {
        format!("{}{}", self.topic_name, index)
    }
}
