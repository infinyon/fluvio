use structopt::StructOpt;
use fluvio_future::task::run_block_on;

fn main() {
    fluvio_future::subscriber::init_tracer(None);

    let opt = fluvio_runner_local::run::RunnerCmd::from_args();
    run_block_on(async {
        opt.process().await.expect("process should run");
    });
}
