
use fluvio::run_cli;

use types::print_cli_err;

fn main() {
    utils::init_logger();

    if let Err(err) = run_cli() {
        print_cli_err!(err);
    }
}
