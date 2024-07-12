mod command;
mod common;

use anyhow::Result;
use clap::Parser;
use command::uninstall::UninstallOpt;

use self::command::current::CurrentOpt;
use self::command::install::InstallOpt;
use self::command::itself::SelfOpt;
use self::command::list::ListOpt;
use self::command::switch::SwitchOpt;
use self::command::update::UpdateOpt;
use self::command::version::VersionOpt;
use self::common::notify::Notify;

/// Binary name is read from `Cargo.toml` `[[bin]]` section
pub const BINARY_NAME: &str = env!("CARGO_BIN_NAME");

/// Binary version is read from `VERSION` file, which is the same as Fluvio version
pub const VERSION: &str = include_str!("../../../VERSION");

#[fluvio_future::main_async]
async fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let args = Cli::parse();

    args.process().await?;
    Ok(())
}

#[derive(Debug, Parser)]
#[command(
    name = BINARY_NAME,
    about = "Fluvio Version Manager (FVM)",
    max_term_width = 100,
    arg_required_else_help = true,
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
    #[command(name = "list")]
    List(ListOpt),
    /// Set a installed Fluvio Version as active
    #[command(name = "switch")]
    Switch(SwitchOpt),
    /// Uninstalls a Fluvio Version
    #[command(name = "uninstall")]
    Uninstall(UninstallOpt),
    /// Updates the current channel version to the most recent
    #[command(name = "update")]
    Update(UpdateOpt),
    /// Prints version information
    Version(VersionOpt),
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
            Command::List(cmd) => cmd.process(notify).await,
            Command::Switch(cmd) => cmd.process(notify).await,
            Command::Uninstall(cmd) => cmd.process(notify).await,
            Command::Update(cmd) => cmd.process(notify).await,
            Command::Version(cmd) => cmd.process(),
        }
    }
}
