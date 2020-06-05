use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
pub struct ProductOption {
    /// number of records
    #[structopt(short, long, default_value = "1")]
    pub produce_count: u16,
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

    /// disable set up topics
    #[structopt(long)]
    disable_topic_setup: bool,

    #[structopt(short, long, default_value = "1")]
    /// replication count, number of spu will be same as replication count, unless overridden
    replication: u16,

    /// topic name used for testing
    #[structopt(short("t"), long, default_value = "topic1")]
    pub topic_name: String,

    /// number of spu
    #[structopt(short, long)]
    spu: Option<u16>,

    /// enable tls
    #[structopt(long)]
    tls: bool,

    /// run local environment
    #[structopt(long)]
    local_driver: bool,

    /// run develop image, this is for k8
    #[structopt(long)]
    develop: bool,

    // log flag
    #[structopt(short, long)]
    pub log: Option<String>,
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
        !!self.disable_install
    }

    pub fn init_topic(&self) -> bool {
        !self.disable_topic_setup
    }

    pub fn terminate_after_consumer_test(&self) -> bool {
        !self.disable_install
    }

    pub fn tls(&self) -> bool {
        self.tls
    }

    pub fn replication(&self) -> u16 {
        self.replication
    }

    pub fn spu_count(&self) -> u16 {
        if let Some(spu) = self.spu {
            spu
        } else {
            self.replication
        }
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
}
