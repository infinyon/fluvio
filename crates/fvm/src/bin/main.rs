mod command;

use clap::{Args, Parser, Subcommand};
use color_eyre::eyre::Result;

use self::command::init::InitOpt;

fn main() -> Result<()> {
    color_eyre::install()?;

    let args = Cli::parse();

    args.process()?;
    Ok(())
}

#[derive(Debug, Args, Clone, Default)]
pub struct GlobalOptions {
    /// Suppress stdout notifications
    #[clap(short = 'q', long, global = true, default_value_t = false)]
    quiet: bool,
}

#[derive(Debug, Parser)]
#[command(next_line_help = true)]
#[command(name = "fvm", version, about, arg_required_else_help = true)]
pub struct Cli {
    #[clap(flatten)]
    global_opts: GlobalOptions,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize a new FVM instance
    #[command(name = "init")]
    Init(InitOpt),
}

impl Cli {
    fn process(&self) -> Result<()> {
        let args = Cli::parse();
        let command = args.command;

        match command {
            Command::Init(cmd) => cmd.process(),
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::Command;

    use super::Cli;

    fn parse(command: &str) -> Result<Cli, clap::error::Error> {
        Cli::try_parse_from(command.split_whitespace())
    }

    #[test]
    fn recognizes_quiet_top_level_arg() {
        let args = parse("fvm -q init").expect("Should parse command as valid");

        assert!(args.global_opts.quiet);
        assert!(matches!(args.command, Command::Init(_)));
    }
}
