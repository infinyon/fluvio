#![type_length_limit = "6685544"]
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
    use fluvio_future::task::run_block_on;
    use setup::Setup;
    use test_runner::TestRunner;

    fluvio_future::subscriber::init_logger();

    let option = TestOption::parse_cli_or_exit();

    let mut setup = Setup::new(option.clone());
    let test_runner = TestRunner::new(option);

    run_block_on(async {
        setup.setup().await;

        test_runner.run_test().await;
    });
}
