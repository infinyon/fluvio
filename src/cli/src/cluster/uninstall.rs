
use crate::Terminal;

use crate::CliError;

use structopt::StructOpt;


#[derive(Debug,StructOpt)]
pub struct UninstallCommand {

    #[structopt(long,default_value="default")]
    namespace: String,

    #[structopt(long,default_value="fluvio")]
    name: String,

    /// don't wait for clean up
    #[structopt(long)]
    no_wait: bool

}



pub async fn process_uninstall<O>(_out: std::sync::Arc<O>,command: UninstallCommand) -> Result<String,CliError>
    where O: Terminal
 {

    use std::process::Command;

    use k8_client::load_and_share;
    use k8_obj_metadata::InputObjectMeta;
    use k8_obj_core::pod::PodSpec;

    use super::CommandUtil;
    use super::k8_util::wait_for_delete;

    println!("removing fluvio installation");
    Command::new("helm")
        .arg("uninstall")
        .arg(command.name)
        .wait();

    let client = load_and_share()
        .map_err(|err| CliError::Other(err.to_string()))?;

    let sc_pod = InputObjectMeta::named("flv-sc", &command.namespace);
    wait_for_delete::<PodSpec>(client,&sc_pod).await;


    println!("deleting existing spu groups");
    Command::new("kubectl")
        .arg("delete")
        .arg("spg")
        .arg("--all")
        .arg("--namespace")
        .arg(&command.namespace)
        .wait_check();

    println!("deleting existing spu");
     Command::new("kubectl")
        .arg("delete")
        .arg("spu")
        .arg("--all")
        .arg("--namespace")
        .arg(&command.namespace)
        .wait_check();

    
    println!("deleting existing topics");    
    Command::new("kubectl")
        .arg("delete")
        .arg("topic")
        .arg("--all")
        .arg("--namespace")
        .arg(&command.namespace)
        .wait_check();

    println!("deleting existing pvc");  
    Command::new("kubectl")
        .arg("delete")
        .arg("persistentvolumeclaims")
        .arg("--all")
        .arg("--namespace")
        .arg(&command.namespace)
        .wait_check();

    
    Ok("".to_owned())
}

