#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::Parser;

fn main() {
    fluvio_future::subscriber::init_tracer(None);

    let opt = fluvio_spu::SpuOpt::parse();
    fluvio_spu::main_loop(opt);
}
