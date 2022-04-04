use clap::Parser;

use fluvio_sc::cli::ScOpt;
use fluvio_sc::k8::main_k8_loop as main_loop;

fn main() {
    fluvio_future::subscriber::init_tracer(None);

    let opt = ScOpt::parse();
    main_loop(opt);
}
