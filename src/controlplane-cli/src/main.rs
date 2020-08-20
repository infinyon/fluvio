use structopt::StructOpt;

fn main() {
    flv_util::init_tracer(None);

    let opt = fluvio_controlplane_cli::ScOpt::from_args();
    fluvio_controlplane_cli::main_loop(opt);
}
