use std::{convert::TryInto, path::PathBuf};
use std::ops::Deref;
use anyhow::{Context, Result};
use semver::Version;

use fluvio::config::TlsPolicy;

use crate::{LocalInstaller, LocalConfig};
use fluvio_types::config_file::SaveLoadConfig;
use crate::{cli::{get_installation_type, shutdown::ShutdownOpt, ClusterCliError}, start::local::LOCAL_CONFIG_PATH};

use super::StartOpt;

/// Attempts to either start a local Fluvio cluster or check (and fix) the preliminery preflight checks.
/// Pass opt.setup = false, to only run the checks.
pub async fn process_local(opt: StartOpt, platform_version: Version, upgrade: bool) -> Result<()> {
    let mut builder = LocalConfig::builder(platform_version);

    if let Some(data_dir) = opt.data_dir {
        builder.data_dir(data_dir);
    }

    builder
        .log_dir(opt.log_dir.deref())
        .spu_replicas(opt.spu)
        .hide_spinner(false);

    if let Some(chart_location) = opt.k8_config.chart_location {
        builder.local_chart(chart_location);
    }

    if let Some(rust_log) = opt.rust_log {
        builder.rust_log(rust_log);
    }

    if opt.tls.tls {
        let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;
        builder.tls(client, server);
    }

    if opt.skip_checks {
        builder.skip_checks(true);
    }

    builder.installation_type(opt.installation_type.get_or_default());

    builder.read_only_config(opt.installation_type.read_only);

    builder.save_profile(!opt.skip_profile_creation);

    if let Some(pub_addr) = opt.sc_pub_addr {
        builder.sc_pub_addr(pub_addr);
    }

    if let Some(priv_addr) = opt.sc_priv_addr {
        builder.sc_priv_addr(priv_addr);
    }

    // SPIKING ONLY: THIS IS DUPLICATED CODE NOT MEANT TO BE CHECKED IN
    let config = 
    if upgrade {
        let local_conf = match LOCAL_CONFIG_PATH.as_ref() {
            None => {
                return Err(ClusterCliError::Other(
                    "Configuration file for local cluster not found from previous run".to_string(),
                )
                .into())
            }
            Some(local_config_path) => LocalConfig::load_from(local_config_path),
        }
        .with_context(|| "Unable to load configuration file for local cluster")?;
        builder.build()?.overwrite_with(local_conf)
    } else {
        builder.build()?
    };

    let installer = LocalInstaller::from_config(config, upgrade);
    if opt.setup {
        preflight_check(&installer).await?;
    } else {
        install_local(&installer).await?;
    }

    Ok(())
}

async fn install_local(installer: &LocalInstaller) -> Result<()> {
    installer.install().await?;
    Ok(())
}

async fn preflight_check(installer: &LocalInstaller) -> Result<()> {
    installer.preflight_check(false).await?;

    Ok(())
}
