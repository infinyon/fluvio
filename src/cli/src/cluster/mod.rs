mod install;
mod uninstall;
mod minikube;


use structopt::StructOpt;

pub use process::process_cluster;
pub use cmd_util::CommandUtil;
pub use k8_util::*;

use minikube::SetMinikubeContext;
use install::InstallCommand;
use uninstall::UninstallCommand;

#[derive(Debug,StructOpt)]
#[structopt(about = "Available Commands")]
pub enum ClusterCommands {


    /// set my own context
    #[structopt(name = "set-minikube-context")]
    SetMinikubeContext(SetMinikubeContext),

    
    #[structopt(name = "install")] 
    Install(InstallCommand),

    #[structopt(name = "uninstall")]
    Uninstall(UninstallCommand),

}


mod process {

    use crate::CliError;
    use crate::Terminal;

    use super::*;

    use install::process_install;
    use uninstall::process_uninstall;
    use minikube::process_minikube_context;

    pub async fn process_cluster<O>(out: std::sync::Arc<O>,cmd: ClusterCommands) -> Result<String,CliError>
        where O: Terminal
    {

        match cmd {
            ClusterCommands::SetMinikubeContext(ctx) =>  process_minikube_context(ctx),
            ClusterCommands::Install(install) => process_install(out, install).await,
            ClusterCommands::Uninstall(uninstall) => process_uninstall(out, uninstall).await
        }

    
    }

}




mod cmd_util {

    pub trait CommandUtil {

        // wait and check
        fn wait(&mut self);

        fn wait_check(&mut self);
    }

    use std::process::Command;

    impl CommandUtil for Command {

        /// execute and wait, ignore error
        fn wait(&mut self) {

            use std::io;
            use std::io::Write;

            let output = self.output().expect("execution failed");

            io::stdout().write_all(&output.stdout).unwrap();
            io::stderr().write_all(&output.stderr).unwrap();

        }

        /// execute and wait, ignore error
        fn wait_check(&mut self) {

            use std::io;
            use std::io::Write;

            let output = self.output().expect("execution failed");

            io::stdout().write_all(&output.stdout).unwrap();
            io::stderr().write_all(&output.stderr).unwrap();

            assert!(output.status.success());


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
    pub async fn wait_for_delete<S:Spec>(client: SharedK8Client,input: &InputObjectMeta)  {

        for i in 0..100u16 {
            println!("checking to see if {} is deleted, count: {}",S::label(),i);
            match client.retrieve_item::<S,_>(input).await {
                Ok(_) => {
                    println!("sc {} still exists, sleeping 10 second",S::label());
                    sleep(Duration::from_millis(10000)).await;
                },
                Err(err) => match err {
                    K8ClientError::NotFound => {
                        println!("no sc {} found, can proceed to setup ",S::label());
                        return;
                    },
                    _ => assert!(false,format!("error: {}",err))
                }
            };
        }

        assert!(false,"waiting too many times, failing");

    }



}