mod setup;
mod cli;
mod tests;
mod environment;
mod tls;

pub use cli::*;
pub use tls::load_tls;
use crate::tests::{SmokeTestRunner, TestDriver};

const VERSION: &str = include_str!("../../../VERSION");

fn main() {
    use fluvio_future::task::run_block_on;
    use setup::Setup;

    println!("Start running fluvio test runner");
    fluvio_future::subscriber::init_logger();

    let option = TestOption::parse_cli_or_exit();

    // catch panic in the spawn
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("panic {}", panic_info);
        std::process::exit(-1);
    }));

    run_block_on(async move {
        if option.setup() {
            let mut setup = Setup::new(option.clone());
            setup.setup().await;
        } else {
            println!("no setup");
        }

        let runner = SmokeTestRunner::new(option);
        runner.run().await;
    });
}
