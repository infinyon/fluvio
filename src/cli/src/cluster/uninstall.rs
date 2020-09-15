use crate::Terminal;

use crate::CliError;
use structopt::StructOpt;
use tracing::debug;

#[derive(Debug, StructOpt)]
pub struct UninstallCommand {
    #[structopt(long, default_value = "default")]
    namespace: String,

    #[structopt(long, default_value = "fluvio")]
    name: String,

    /// don't wait for clean up
    #[structopt(long)]
    no_wait: bool,

    /// uninstall local spu/sc(custom)
    #[structopt(long)]
    local: bool,

    #[structopt(long)]
    /// removing sys chart
    sys: bool,
}

use std::process::Command;
use super::CommandUtil;

pub async fn process_uninstall<O>(
    _out: std::sync::Arc<O>,
    command: UninstallCommand,
) -> Result<String, CliError>
where
    O: Terminal,
{
    println!("removing fluvio installation");
    if command.sys {
        remove_sys();
    } else if command.local {
        remove_local_cluster();
    } else {
        remove_k8_cluster(&command).await?;
    }

    let ns = &command.namespace;

    remove_objects("spugroups", ns, None);
    remove_objects("spus", ns, None);
    remove_objects("topics", ns, None);
    remove_objects("persistentvolumeclaims", ns, Some("fluvio-spu"));
    remove_objects("persistentvolumes", ns, Some("fluvio-spu"));
    remove_objects("storageclasses", ns, Some("fluvio-spu"));

    // delete secrets
    Command::new("kubectl")
        .arg("delete")
        .arg("secret")
        .arg("fluvio-ca")
        .arg("--ignore-not-found=true")
        .inherit();

    Command::new("kubectl")
        .arg("delete")
        .arg("secret")
        .arg("fluvio-tls")
        .arg("--ignore-not-found=true")
        .inherit();

    Ok("".to_owned())
}

fn remove_objects(object_type: &str, namespace: &str, selector: Option<&str>) {
    let mut cmd = Command::new("kubectl");

    cmd.arg("delete");
    cmd.arg(object_type);
    cmd.arg("--namespace");
    cmd.arg(namespace);

    if let Some(label) = selector {
        println!(
            "deleting label '{}' object {} in: {}",
            label, object_type, namespace
        );
        cmd.arg("--selector").arg(label);
    } else {
        println!("deleting all {} in: {}", object_type, namespace);
        cmd.arg("--all");
    }

    cmd.inherit();
}

fn remove_local_cluster() {
    use std::fs::remove_dir_all;

    use tracing::warn;

    println!("removing local cluster ");
    Command::new("pkill")
        .arg("-f")
        .arg("fluvio run")
        .print()
        .output()
        .expect("failed to execute process");

    // delete fluvio file
    debug!("remove fluvio directory");
    if let Err(err) = remove_dir_all("/tmp/fluvio") {
        warn!("fluvio dir can't be removed: {}", err);
    }
}

async fn remove_k8_cluster(command: &UninstallCommand) -> Result<(), CliError> {
    use k8_client::load_and_share;
    use k8_obj_metadata::InputObjectMeta;
    use k8_obj_core::pod::PodSpec;

    use super::k8_util::wait_for_delete;

    println!("removing kubernetes cluster");
    Command::new("helm")
        .arg("uninstall")
        .arg(&command.name)
        .wait();

    let client = load_and_share().map_err(|err| CliError::Other(err.to_string()))?;

    let sc_pod = InputObjectMeta::named("flv-sc", &command.namespace);
    wait_for_delete::<PodSpec>(client, &sc_pod).await;

    Ok(())
}

fn remove_sys() {
    println!("removing fluvio sys chart");

    Command::new("helm")
        .arg("uninstall")
        .arg("fluvio-sys")
        .inherit();

    println!("fluvio sys chart has been uninstalled");
}
