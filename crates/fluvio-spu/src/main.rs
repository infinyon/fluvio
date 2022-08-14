


fn main() {
    use clap::Parser;


    fluvio_future::subscriber::init_tracer(None);

    let opt = fluvio_spu::SpuOpt::parse();
    fluvio_spu::main_loop(opt);
}
