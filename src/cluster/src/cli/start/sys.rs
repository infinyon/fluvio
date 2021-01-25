use crate::cli::start::StartOpt;
use crate::cli::ClusterCliError;
use crate::sys::{SysConfig, SysInstaller};
use crate::ClusterError;
use crate::error::SysInstallError;

pub fn install_sys(opt: StartOpt, upgrade: bool) -> Result<(), ClusterCliError> {
    install_sys_impl(opt, upgrade).map_err(ClusterError::InstallSys)?;
    Ok(())
}

fn install_sys_impl(opt: StartOpt, upgrade: bool) -> Result<(), SysInstallError> {
    let mut builder = SysConfig::builder();
    builder.with_namespace(opt.k8_config.namespace);

    match opt.k8_config.chart_location {
        // If a chart location is given, use it
        Some(chart_location) => {
            builder.with_local_chart(chart_location);
        }
        // If we're in develop mode (but no explicit chart location), use local path
        None if opt.develop => {
            builder.with_local_chart("./k8-util/helm");
        }
        _ => (),
    }

    let config = builder.build()?;
    let installer = SysInstaller::with_config(config)?;

    if upgrade {
        installer.install()?;
        println!("Fluvio system chart has been installed");
    } else {
        installer.upgrade()?;
        println!("Fluvio system chart has been upgraded");
    }

    Ok(())
}
