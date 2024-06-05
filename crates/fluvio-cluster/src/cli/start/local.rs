use std::ops::Deref;
use anyhow::Result;
use semver::Version;

use fluvio::config::TlsPolicy;

use crate::{
    cli::options::{ClusterConnectionOpts, K8Install},
    LocalConfig, LocalConfigBuilder, LocalInstaller,
};

use super::{IntallationTypeOpt, StartOpt};

/// Attempts to either start a local Fluvio cluster or check (and fix) the preliminery preflight checks.
/// Pass opt.setup = false, to only run the checks.
pub async fn process_local(
    opt: StartOpt,
    platform_version: Version, /* , upgrade: bool*/
) -> Result<()> {
    let mut builder = LocalConfig::builder(platform_version);

    if let Some(data_dir) = opt.data_dir {
        builder.data_dir(data_dir);
    }

    if let Some(rust_log) = opt.rust_log {
        builder.rust_log(rust_log);
    }

    if opt.skip_checks {
        builder.skip_checks(true);
    }

    builder
        .log_dir(opt.log_dir.deref())
        .spu_replicas(opt.spu)
        .hide_spinner(false)
        .save_profile(!opt.skip_profile_creation)
        .append_installation_type(opt.installation_type)
        .append_k8s_config(opt.k8_config)
        .append_connection_options(opt.connection_config)?
        .build_and_start(opt.setup, false)
        .await
}

impl LocalConfigBuilder {
    pub fn append_connection_options(
        &mut self,
        connection_config: ClusterConnectionOpts,
    ) -> Result<&mut Self> {
        if connection_config.tls.tls {
            let (client, server): (TlsPolicy, TlsPolicy) = connection_config.tls.try_into()?;
            self.tls(client, server);
        }

        if let Some(pub_addr) = connection_config.sc_pub_addr {
            self.sc_pub_addr(pub_addr);
        }

        if let Some(priv_addr) = connection_config.sc_priv_addr {
            self.sc_priv_addr(priv_addr);
        }

        Ok(self)
    }

    pub fn append_installation_type(&mut self, installation_type: IntallationTypeOpt) -> &mut Self {
        self.installation_type(installation_type.get_or_default())
            .read_only_config(installation_type.read_only)
    }

    pub fn append_k8s_config(&mut self, k8_config: K8Install) -> &mut Self {
        if let Some(chart_location) = k8_config.chart_location {
            self.local_chart(chart_location);
        }

        self
    }

    pub async fn build_and_start(&self, setup: bool, upgrade: bool) -> Result<()> {
        let config = self.build()?;

        let installer = LocalInstaller::from_config(config, upgrade);
        if setup {
            preflight_check(&installer).await
        } else {
            install_local(&installer).await
        }
    }
}

async fn install_local(installer: &LocalInstaller) -> Result<()> {
    installer.install().await?;
    Ok(())
}

async fn preflight_check(installer: &LocalInstaller) -> Result<()> {
    installer.preflight_check(false).await?;

    Ok(())
}
