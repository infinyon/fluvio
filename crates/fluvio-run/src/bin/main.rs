use clap::Parser;
use fluvio_run::RunCmd;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd: RunCmd = RunCmd::parse();

    cmd.process()?;
    Ok(())
}
