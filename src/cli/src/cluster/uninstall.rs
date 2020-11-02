use crate::Terminal;

use crate::CliError;
use structopt::StructOpt;
use fluvio_cluster::ClusterUninstaller;

#[derive(Debug, StructOpt)]
pub struct UninstallCommand {
    #[structopt(long, default_value = "default")]
    namespace: String,

    #[structopt(long, default_value = "fluvio")]
    name: String,

    /// don't wait for clean up
    #[structopt(long)]
    no_wait: bool,

    /// Remove local spu/sc(custom) fluvio installation
    #[structopt(long)]
    local: bool,

    #[structopt(long)]
    /// Remove fluvio system chart
    sys: bool,
}

pub async fn process_uninstall<O>(
    _out: std::sync::Arc<O>,
    command: UninstallCommand,
) -> Result<String, CliError>
where
    O: Terminal,
{
    let uninstaller = ClusterUninstaller::new()
        .with_namespace(&command.namespace)
        .with_name(&command.name)
        .build()?;
    if command.sys {
        uninstaller.uninstall_sys()?;
    } else if command.local {
        uninstaller.uninstall_local()?;
    } else {
        uninstaller.uninstall().await?;
    }

    Ok("".to_owned())
}
