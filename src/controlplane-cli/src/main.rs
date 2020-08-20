use structopt::StructOpt;

fn main() {
    flv_util::init_tracer(None);

    let opt = flv_sc_k8::ScOpt::from_args();
    flv_sc_k8::main_loop(opt);
}
