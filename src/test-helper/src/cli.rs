use structopt::StructOpt;


/// cli options
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "fluvio-test-runner", about = "Test fluvio platform")]
pub struct TestOption {

    /// disable produce and consumer test, only run server
    #[structopt(short("t"),long)]
    disable_test: bool,

    /// don't produce message
    #[structopt(short("d"),long)]
    disable_produce: bool,

    /// don't test consumer
    #[structopt(short("c"),long)]
    disable_consume: bool,

    // disable initial cleanup
    #[structopt(long)]
    disable_clean: bool,

    #[structopt(short("s"),long)]
    disable_setup: bool,

    // disable shutdown
    #[structopt(long)]
    disable_shutdown: bool,

    #[structopt(short,long,default_value = "1")]
    /// replication count, number of spu will be same as replication count, unless overridden 
    replication: u16,

    /// disable init
    #[structopt(short("i"),long)]
    disable_init: bool,

    /// number of spu
    #[structopt(short("p"),long)]
    spu: Option<u16>,

    #[structopt(long)]
    tls: bool
}

impl TestOption  {

        /// return SC configuration or exist program.
    pub fn parse_cli_or_exit() -> Self {
        
        Self::from_args()
    }

    pub fn test_consumer(&self) -> bool {
        !self.disable_test && !self.disable_consume
    }

    pub fn cleanup(&self) -> bool {
        self.setup() && !self.disable_clean
    }

    // do the setup (without cleanup)
    pub fn setup(&self) -> bool {
        !self.disable_setup
    }

    pub fn init_topic(&self) -> bool {
        !self.disable_setup
    }

    pub fn terminate_after_consumer_test(&self) -> bool {
        !self.disable_shutdown
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
        !self.disable_test && !self.disable_produce
    }

}


