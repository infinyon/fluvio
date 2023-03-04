mod changelog;
use changelog::UpdateChangelogOpt;

mod repo_version;
use repo_version::UpdateVersionOpt;

use anyhow::{Result};
use clap::Parser;

use tracing_subscriber::filter::{EnvFilter, LevelFilter};
#[derive(Debug, Parser)]
struct FluvioDevOpsOpt {
    #[clap(subcommand)]
    command: FluvioDevOpsCmd,
}

#[derive(Debug, Parser)]
enum FluvioDevOpsCmd {
    /// Generates the most recent changelog for the repo using `git cliff`.
    ///
    /// By default, running this
    UpdateChangelog(UpdateChangelogOpt),
    /// Modify the version
    UpdateVersion(UpdateVersionOpt),
}

fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(LevelFilter::INFO.into()))
        .try_init();

    let opt = FluvioDevOpsOpt::parse();

    opt.command.execute()?;

    Ok(())
}

impl FluvioDevOpsCmd {
    pub fn execute(&self) -> Result<()> {
        match self {
            Self::UpdateChangelog(update_changelog_opt) => update_changelog_opt.execute(),
            Self::UpdateVersion(update_version_opt) => update_version_opt.execute(),
        }
    }
}
