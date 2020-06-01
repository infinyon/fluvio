
use fluvio::run_cli;

use flv_types::print_cli_err;

fn main() {
    
    flv_util::init_logger();

    match run_cli() {
        Ok(output) => {
            if output.len() > 0 {
                println!("{}",output)
            }
        },
        Err(err) =>  {
            print_cli_err!(format!("error: {}",err));
            std::process::exit(-1);
        }
    }
}
