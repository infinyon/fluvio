use structopt::StructOpt;

fn main() {
    flv_util::init_tracer(None);

    let opt = fluvio_spu::SpuOpt::from_args();
    fluvio_spu::main_loop(opt);
}
