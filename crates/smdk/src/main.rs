mod cmd;
mod generate;

use clap::Parser;
use cmd::SmdkCommand;
use color_eyre::eyre::Result;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let root: SmdkCommand = SmdkCommand::parse();
    root.process();

    Ok(())
}
