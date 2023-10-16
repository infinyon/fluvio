mod command;
mod common;

use anyhow::Result;
use clap::Parser;
use command::current::CurrentOpt;
use command::show::ShowOpt;

use self::command::install::InstallOpt;
use self::command::itself::SelfOpt;
use self::command::switch::SwitchOpt;
use self::common::notify::Notify;

#[async_std::main]
async fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let args = Cli::parse();

    args.process().await?;
    Ok(())
}

#[derive(Debug, Parser)]
#[command(
    about = "Fluvio Version Manager (FVM)",
    name = "fvm",
    max_term_width = 100,
    version = env!("CARGO_PKG_VERSION")
)]
pub struct Cli {
    #[clap(long, short = 'q', help = "Suppress all output")]
    quiet: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
pub enum Command {
    /// Print the current active Fluvio Version
    #[command(name = "current")]
    Current(CurrentOpt),
    /// Manage FVM
    #[command(name = "self")]
    Itself(SelfOpt),
    /// Install a Fluvio Version
    #[command(name = "install")]
    Install(InstallOpt),
    /// List installed Fluvio Versions
    #[command(name = "show")]
    Show(ShowOpt),
    /// Set a installed Fluvio Version as active
    #[command(name = "switch")]
    Switch(SwitchOpt),
}

impl Cli {
    async fn process(&self) -> Result<()> {
        let args = Cli::parse();
        let command = args.command;
        let notify = Notify::new(self.quiet);

        match command {
            Command::Current(cmd) => cmd.process(notify).await,
            Command::Itself(cmd) => cmd.process(notify).await,
            Command::Install(cmd) => cmd.process(notify).await,
            Command::Show(cmd) => cmd.process(notify).await,
            Command::Switch(cmd) => cmd.process(notify).await,
        }
    }
}
