use structopt::StructOpt;
use fluvio_future::task::run_block_on;
use fluvio_run::RunCmd;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    fluvio_future::subscriber::init_tracer(None);
    let cmd = RunCmd::from_args();
    run_block_on(cmd.process())?;
    Ok(())
}
