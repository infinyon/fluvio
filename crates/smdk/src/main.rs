mod build;
mod cmd;
mod test;

use clap::Parser;
use anyhow::Result;

use cmd::SmdkCommand;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let root: SmdkCommand = SmdkCommand::parse();
    root.process()?;

    Ok(())
}
