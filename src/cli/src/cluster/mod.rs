mod install;
mod uninstall;
mod minikube;
mod util;
mod check;
mod releases;

pub use process::process_cluster;

use structopt::StructOpt;

use util::*;

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
    ) -> anyhow::Result<String>
    where
        O: Terminal,
    {
        let output = match cmd {
            ClusterCommands::SetMinikubeContext(ctx) => process_minikube_context(ctx)?,
            ClusterCommands::Install(install) => process_install(out, install).await?,
            ClusterCommands::Uninstall(uninstall) => process_uninstall(out, uninstall).await?,
            ClusterCommands::Check(check) => run_checks(check).await?,
            ClusterCommands::Releases(releases) => process_releases(releases)?,
        };
        Ok(output)
    }
}

mod k8_util {

    use std::time::Duration;

    use k8_client::SharedK8Client;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata_client::MetadataClient;
    use k8_obj_metadata::Spec;
    use fluvio_future::timer::sleep;
    use k8_client::ClientError as K8ClientError;

    // wait for i8 objects appear
    pub async fn wait_for_delete<S: Spec>(client: SharedK8Client, input: &InputObjectMeta) {
        use k8_client::http::StatusCode;

        for i in 0..100u16 {
            println!("checking to see if {} is deleted, count: {}", S::label(), i);
            match client.retrieve_item::<S, _>(input).await {
                Ok(_) => {
                    println!("sc {} still exists, sleeping 10 second", S::label());
                    sleep(Duration::from_millis(10000)).await;
                }
                Err(err) => match err {
                    K8ClientError::Client(status) if status == StatusCode::NOT_FOUND => {
                        println!("no sc {} found, can proceed to setup ", S::label());
                        return;
                    }
                    _ => panic!("error: {}", err),
                },
            };
        }

        panic!("waiting too many times, failing")
    }
}
