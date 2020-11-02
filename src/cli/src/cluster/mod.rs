mod install;
mod uninstall;
mod minikube;
mod util;
mod check;
mod releases;

pub use process::process_cluster;

use structopt::StructOpt;

use minikube::SetMinikubeContext;
pub use install::InstallCommand;
use uninstall::UninstallCommand;
use check::CheckCommand;
use releases::ReleasesCommand;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, StructOpt)]
#[structopt(about = "Available Commands")]
pub enum ClusterCommands {
    /// set my own context
    #[structopt(name = "set-minikube-context")]
    SetMinikubeContext(SetMinikubeContext),

    /// install cluster
    #[structopt(name = "install")]
    Install(InstallCommand),

    /// uninstall cluster
    #[structopt(name = "uninstall")]
    Uninstall(UninstallCommand),

    /// perform checks
    #[structopt(name = "check")]
    Check(CheckCommand),

    /// release information
    #[structopt(name = "releases")]
    Releases(ReleasesCommand),
}

mod process {

    use crate::CliError;
    use crate::Terminal;

    use super::*;

    use install::process_install;
    use uninstall::process_uninstall;
    use minikube::process_minikube_context;
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
            ClusterCommands::SetMinikubeContext(ctx) => process_minikube_context(ctx),
            ClusterCommands::Install(install) => process_install(out, install).await,
            ClusterCommands::Uninstall(uninstall) => process_uninstall(out, uninstall).await,
            ClusterCommands::Check(check) => run_checks(check).await,
            ClusterCommands::Releases(releases) => process_releases(releases),
        }
    }
}
