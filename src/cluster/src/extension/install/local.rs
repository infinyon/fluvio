use std::convert::TryInto;

use fluvio::config::TlsPolicy;

use crate::extension::ClusterCmdError;
use crate::LocalClusterInstaller;

use super::InstallOpt;

pub async fn install_local(opt: InstallOpt) -> Result<(), ClusterCmdError> {
    let mut builder = LocalClusterInstaller::new()
        .with_log_dir(opt.log_dir.to_string())
        .with_spu_replicas(opt.spu);

    if let Some(rust_log) = opt.rust_log {
        builder = builder.with_rust_log(rust_log);
    }

    if opt.tls.tls {
        let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;
        builder = builder.with_tls(client, server);
    }
    if opt.skip_checks {
        builder = builder.with_skip_checks(true);
    }

    let installer = builder.build()?;
    installer.install().await?;
    Ok(())
}

pub async fn run_local_setup(_opt: InstallOpt) -> Result<(), ClusterCmdError> {
    let installer = LocalClusterInstaller::new().build()?;
    installer.setup().await?;
    println!("Setup successful, all the steps neccessary for cluster installation have been performed successfully");
    Ok(())
}
