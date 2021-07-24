use semver::Version;
use crate::cli::start::StartOpt;
use crate::cli::ClusterCliError;
use crate::charts::{SysConfig, SysInstaller};
use crate::ClusterError;
use crate::error::SysInstallError;

pub fn process_sys(
    opt: StartOpt,
    default_chart_version: Version,
    upgrade: bool,
) -> Result<(), ClusterCliError> {
    install_sys_impl(opt, default_chart_version, upgrade).map_err(ClusterError::InstallSys)?;
    Ok(())
}

fn install_sys_impl(
    opt: StartOpt,
    default_chart_version: Version,
    upgrade: bool,
) -> Result<(), SysInstallError> {
    let chart_version = opt
        .k8_config
        .chart_version
        .clone()
        .unwrap_or(default_chart_version);

    let config = SysConfig::builder(chart_version)
        .namespace(&opt.k8_config.namespace)
        .with(|builder| match &opt.k8_config.chart_location {
            // If a chart location is given, use it
            Some(chart_location) => builder.local_chart(chart_location),
            // If we're in develop mode (but no explicit chart location), use local path
            None if opt.develop => builder.local_chart("./k8-util/helm"),
            _ => builder,
        })
        .build()?;
    let installer = SysInstaller::from_config(config)?;

    if upgrade {
        installer.upgrade()?;
        println!("Fluvio system chart has been upgraded");
    } else {
        installer.install()?;
        println!("Fluvio system chart has been installed");
    }

    Ok(())
}
