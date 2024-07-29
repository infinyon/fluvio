use clap::Parser;
use fluvio_run::RunCmd;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd: RunCmd = RunCmd::parse();

    fluvio_future::subscriber::init_tracer(None);

    cmd.process()?;
    Ok(())
}
