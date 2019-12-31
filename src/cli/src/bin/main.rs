
use fluvio::run_cli;

use types::print_cli_err;

fn main() {
    utils::init_logger();

    match run_cli() {
        Ok(output) => {
            if output.len() > 0 {
                println!("{}",output)
            }
        },
        Err(err) =>  print_cli_err!(err)
    }
}
