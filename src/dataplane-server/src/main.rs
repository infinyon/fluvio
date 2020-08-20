use structopt::StructOpt;

fn main() {
    flv_util::init_tracer(None);

    let opt = fluvio_dataplane::SpuOpt::from_args();
    fluvio_dataplane::main_loop(opt);
}
