mod setup;
mod cli;
mod tests;
mod util;
mod environment;
mod tls;
mod test_runner;

pub use cli::*;
pub use tls::*;

fn main() {
    use flv_future_aio::task::run_block_on;
    use setup::Setup;
    use test_runner::TestRunner;

    flv_future_aio::util::init_logger();

    let option = TestOption::parse_cli_or_exit();

    let mut setup = Setup::new(option.clone());
    let test_runner = TestRunner::new(option);

    run_block_on(async {
        setup.setup().await;

        test_runner.run_test().await;
    });
}
