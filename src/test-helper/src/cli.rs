use structopt::StructOpt;


/// cli options
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "fluvio-test-runner", about = "Test fluvio platform")]
pub struct TestOption {

    /// disable test, only run server
    #[structopt(short,long)]
    pub disable_test: bool,

    // clean and terminate servers
    #[structopt(short,long)]
    pub terminate: bool,

    #[structopt(short,long,default_value = "1")]
    /// Address for external communication
    pub replication: u16,
}

impl TestOption  {

        /// return SC configuration or exist program.
    pub fn parse_cli_or_exit() -> Self {
        
        Self::from_args()
    }

    pub fn test_consumer(&self) -> bool {
        !self.disable_test
    }

}


