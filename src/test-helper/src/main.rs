mod runner;
mod cli;
mod tests;
mod bin;
mod cmd;
mod environment;
mod target;
mod tls;

use cli::TestOption;
use bin::*;
use cmd::*;
use target::*;
use tls::*;

fn main() {

    use runner::TestRunner;
    use flv_future_aio::task::run_block_on;

    flv_future_aio::util::init_logger();

    let option = TestOption::parse_cli_or_exit();

    
    let mut runner = TestRunner::new(option);

    if let Err(err) = run_block_on(runner.test()) {
        assert!(false,"error: {:?}",err);
    }  

}