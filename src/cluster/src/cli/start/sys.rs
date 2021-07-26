use semver::Version;
use crate::cli::start::StartOpt;
use crate::cli::ClusterCliError;
use crate::charts::{ChartConfig, ChartInstallError,ChartInstaller};
use crate::ClusterError;


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
) -> Result<(), ChartInstallError> {
    let chart_version = opt
        .k8_config
        .chart_version
        .clone()
        .unwrap_or(default_chart_version);

    let config = ChartConfig::sys_builder(chart_version)
        .namespace(&opt.k8_config.namespace)
        .with(|builder| match &opt.k8_config.chart_location {
            // If a chart location is given, use it
            Some(chart_location) => builder.local(chart_location),
            _ => builder,
        })
        .build()?;
    let installer = ChartInstaller::from_config(config)?;

    if upgrade {
        installer.upgrade()?;
        println!("Fluvio system chart has been upgraded");
    } else {
        installer.install()?;
        println!("Fluvio system chart has been installed");
    }

    Ok(())
}
