mod install;
mod uninstall;
mod minikube;
mod util;
mod check;

pub use process::process_cluster;

use structopt::StructOpt;

use util::*;

use minikube::SetMinikubeContext;
pub use install::InstallCommand;
use uninstall::UninstallCommand;
use check::CheckCommand;

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
}

mod process {

    use crate::CliError;
    use crate::Terminal;

    use super::*;

    use install::process_install;
    use uninstall::process_uninstall;
    use minikube::process_minikube_context;
    use check::run_checks;

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
        }
    }
}

mod k8_util {

    use std::time::Duration;

    use k8_client::SharedK8Client;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata_client::MetadataClient;
    use k8_obj_metadata::Spec;
    use flv_future_aio::timer::sleep;
    use k8_client::ClientError as K8ClientError;

    // wait for i8 objects appear
    pub async fn wait_for_delete<S: Spec>(client: SharedK8Client, input: &InputObjectMeta) {
        for i in 0..100u16 {
            println!("checking to see if {} is deleted, count: {}", S::label(), i);
            match client.retrieve_item::<S, _>(input).await {
                Ok(_) => {
                    println!("sc {} still exists, sleeping 10 second", S::label());
                    sleep(Duration::from_millis(10000)).await;
                }
                Err(err) => match err {
                    K8ClientError::NotFound => {
                        println!("no sc {} found, can proceed to setup ", S::label());
                        return;
                    }
                    _ => assert!(false, format!("error: {}", err)),
                },
            };
        }

        assert!(false, "waiting too many times, failing");
    }
}
