
use structopt::StructOpt;

fn main() {
    flv_util::init_logger();

    let opt = flv_spu::SpuOpt::from_args();
    flv_spu::main_loop(opt);
}
