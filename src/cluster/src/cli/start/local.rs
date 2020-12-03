use std::convert::TryInto;

use fluvio::config::TlsPolicy;

use crate::cli::ClusterCliError;
use crate::LocalClusterInstaller;

use super::StartOpt;

pub async fn install_local(opt: StartOpt) -> Result<(), ClusterCliError> {
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

pub async fn run_local_setup(_opt: StartOpt) -> Result<(), ClusterCliError> {
    let installer = LocalClusterInstaller::new().build()?;
    let _results = installer.setup().await;
    println!("Setup successful, all the steps necessary for cluster startup have been performed successfully");
    Ok(())
}
