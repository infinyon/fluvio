mod launcher;
mod runner;
mod cli;
mod setup;
mod tests;
mod bin;

use cli::TestOption;
use bin::*;
fn main() {

    use runner::TestRunner;
    use flv_future_aio::task::run_block_on;

    flv_future_aio::util::init_logger();

    let option = TestOption::parse_cli_or_exit();
    let runner = TestRunner::new(option);

    if let Err(err) = run_block_on(runner.basic_test()) {
        assert!(false,"error: {:?}",err);
    }  

}