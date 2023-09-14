//! Fluvio Version Manager (FVM) CLI

mod command;

use clap::Parser;
use color_eyre::eyre::Result;

fn main() -> Result<()> {
    color_eyre::install()?;
    Cli::exec()?;

    Ok(())
}

#[derive(Debug, Parser)]
#[command(next_line_help = true)]
#[command(name = "fvm", author, version, about, long_about = Some("Fluvio Version Manager (FVM)"))]
pub enum Cli {
    /// Initialize a new FVM instance. This is run after downloading FVM.
    Init,
}

impl Cli {
    fn exec() -> Result<()> {
        let command = Cli::parse();

        match command {
            Cli::Init => command::init::exec(),
        }
    }
}
