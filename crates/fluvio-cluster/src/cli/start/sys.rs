use crate::cli::start::StartOpt;
use crate::cli::ClusterCliError;
use crate::charts::{ChartConfig, ChartInstallError, ChartInstaller};
use crate::ClusterError;

pub fn process_sys(opt: &StartOpt, upgrade: bool) -> Result<(), ClusterCliError> {
    install_sys_impl(opt, upgrade).map_err(ClusterError::InstallSys)?;
    Ok(())
}

fn install_sys_impl(opt: &StartOpt, upgrade: bool) -> Result<(), ChartInstallError> {
    println!("installing sys chart, upgrade: {upgrade}");

    let config = ChartConfig::sys_builder()
        .namespace(opt.k8_config.namespace.clone())
        .version(opt.k8_config.chart_version.clone())
        .with(|builder| match &opt.k8_config.chart_location {
            // If a chart location is given, use it
            Some(chart_location) => builder.local(chart_location),
            _ => builder,
        })
        .build()?;
    let installer = ChartInstaller::from_config(config)?;
    installer.process(upgrade)?;

    Ok(())
}
