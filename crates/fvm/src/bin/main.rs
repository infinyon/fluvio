//! Fluvio Version Manager (FVM) CLI

mod command;

use clap::Parser;
use color_eyre::eyre::Result;

use self::command::init::InitOpt;

fn main() -> Result<()> {
    color_eyre::install()?;

    let args = Cli::parse();

    args.exec()?;
    Ok(())
}

#[derive(Debug, Parser)]
pub struct Cli {
    /// Initialize a new FVM instance. This is run after downloading FVM.
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
#[command(about = "Fluvio Version Manager (FVM)", name = "fvm")]
pub enum Command {
    #[command(name = "init", about = "Initialize a new FVM instance")]
    Init(InitOpt),
}

impl Cli {
    fn exec(&self) -> Result<()> {
        let args = Cli::parse();
        let command = args.command;

        match command {
            Command::Init(cmd) => cmd.process(),
        }
    }
}
