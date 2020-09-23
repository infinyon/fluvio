use fluvio_cli::run_cli;
use fluvio_types::print_cli_err;

fn main() {
    fluvio_future::subscriber::init_tracer(None);

    match run_cli() {
        Ok(output) => {
            if !output.is_empty() {
                println!("{}", output)
            }
        }
        Err(err) => {
            print_cli_err!(format!("error: {}", err));
            std::process::exit(-1);
        }
    }
}
