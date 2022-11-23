mod build;
mod cmd;
mod generate;
mod test;
mod load;
mod publish;
mod hub;
mod set_public;

use std::path::{PathBuf};
use clap::Parser;
use anyhow::Result;

use cmd::SmdkCommand;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let root: SmdkCommand = SmdkCommand::parse();
    root.process()?;

    Ok(())
}

pub(crate) fn read_bytes_from_path(path: &PathBuf) -> Result<Vec<u8>> {
    println!("loading module at: {}", path.display());
    std::fs::read(path).map_err(|err| anyhow::anyhow!("error reading wasm file: {}", err))
}
