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

    let test_runner = TestRunner::new(option.clone());

    run_block_on(async move {
        if option.setup() {
            let mut setup = Setup::new(option);
            setup.setup().await;
        } else {
            println!("no setup");
        }

        test_runner.run_test().await;
    });
}
