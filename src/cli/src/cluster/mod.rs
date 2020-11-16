mod install;
mod uninstall;
mod util;
mod check;
mod releases;

pub use process::process_cluster;

use structopt::StructOpt;

pub use install::InstallCommand;
use uninstall::UninstallCommand;
use check::CheckCommand;
use releases::ReleasesCommand;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, StructOpt)]
#[structopt(about = "Available Commands")]
pub enum ClusterCommands {
    /// Install a Fluvio cluster, locally or on Minikube
    #[structopt(name = "install")]
    Install(InstallCommand),

    /// Uninstall a Fluvio cluster from the local machine or Minkube
    #[structopt(name = "uninstall")]
    Uninstall(UninstallCommand),

    /// Check that all requirements for installation are met
    #[structopt(name = "check")]
    Check(CheckCommand),

    /// Prints information about various Fluvio releases
    #[structopt(name = "releases")]
    Releases(ReleasesCommand),
}

mod process {

    use crate::CliError;
    use crate::Terminal;

    use super::*;

    use install::process_install;
    use uninstall::process_uninstall;
    use check::run_checks;
    use releases::process_releases;

    pub async fn process_cluster<O>(
        out: std::sync::Arc<O>,
        cmd: ClusterCommands,
    ) -> Result<String, CliError>
    where
        O: Terminal,
    {
        match cmd {
            ClusterCommands::Install(install) => process_install(out, install).await,
            ClusterCommands::Uninstall(uninstall) => process_uninstall(out, uninstall).await,
            ClusterCommands::Check(check) => run_checks(check).await,
            ClusterCommands::Releases(releases) => process_releases(releases),
        }
    }
}
