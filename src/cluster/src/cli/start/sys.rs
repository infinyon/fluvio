use semver::Version;

use crate::cli::start::StartOpt;
use crate::cli::ClusterCliError;
use crate::charts::{ChartConfig, ChartInstallError, ChartInstaller};
use crate::ClusterError;
use crate::sys::{SysConfig,SysInstallError};

pub fn process_sys(opt: StartOpt,platform_version: Version) -> Result<(), SysInstallError> {
    
    let config = SysConfig::builder(platform_version)
        .namespace(&opt.k8_config.namespace)
        .build()?;
    
    /* 
    if upgrade {
        println!("upgrading sys chart");
    } else {
        println!("installing sys chart");
    }

    let config = ChartConfig::sys_builder()
        .namespace(&opt.k8_config.namespace)
        .version(opt.k8_config.chart_version.clone())
        .with(|builder| match &opt.k8_config.chart_location {
            // If a chart location is given, use it
            Some(chart_location) => builder.local(chart_location),
            _ => builder,
        })
        .build()?;
    let installer = ChartInstaller::from_config(config)?;
    installer.process(upgrade)?;
    */

    Ok(())
}
