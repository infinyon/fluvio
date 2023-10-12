mod command;
mod common;

use anyhow::Result;
use clap::{Args, Parser};

use self::command::itself::SelfOpt;

#[tokio::main]
async fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let args = Cli::parse();

    args.process().await?;
    Ok(())
}

#[derive(Debug, Args, Clone, Default)]
pub struct GlobalOptions {
    /// Suppress stdout notifications
    #[clap(short = 'q', long, global = true, default_value_t = false)]
    quiet: bool,
}

#[derive(Debug, Parser)]
#[command(
    about = "Fluvio Version Manager (FVM)",
    name = "fvm",
    max_term_width = 100,
    disable_version_flag = true
)]
pub struct Cli {
    #[clap(flatten)]
    global_opts: GlobalOptions,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
pub enum Command {
    /// Manage FVM
    #[command(name = "self")]
    Itself(SelfOpt),
}

impl Cli {
    async fn process(&self) -> Result<()> {
        let args = Cli::parse();
        let command = args.command;

        match command {
            Command::Itself(cmd) => cmd.process().await,
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
        let args = parse("fvm -q self install").expect("Should parse command as valid");

        assert!(args.global_opts.quiet);
        assert!(matches!(args.command, Command::Itself(_)));
    }
}
