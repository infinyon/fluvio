use structopt::StructOpt;

use fluvio_sc::cli::ScOpt;
use fluvio_sc::k8::main_k8_loop as main_loop;

fn main() {
    fluvio_future::subscriber::init_tracer(None);

    let opt = ScOpt::from_args();
    main_loop(opt);
}
