use anyhow::Result;
use fluvio::config::TlsPolicy;
use semver::Version;
use tracing::debug;

use crate::cli::options::ClusterConnectionOpts;
use crate::{ClusterConfig, ClusterConfigBuilder, ClusterInstaller};
use crate::cli::start::StartOpt;

use super::K8Install;

pub async fn process_k8(opt: StartOpt, platform_version: Version) -> Result<()> {
    let mut builder = ClusterConfig::builder(platform_version);
    if opt.develop {
        builder.development()?;
    }

    builder
        .append_connection_options(opt.connection_config)?
        .append_k8s_options(opt.k8_config)
        .append_spu_config(opt.spu, opt.spu_config)
        .append_start_opt(
            !opt.skip_profile_creation,
            opt.skip_checks,
            opt.rust_log,
            opt.service_type,
        )
        .build_and_start(opt.setup, false)
        .await
}

impl ClusterConfigBuilder {
    fn append_start_opt(
        &mut self,
        save_profile: bool,
        skip_checks: bool,
        rust_log: Option<String>,
        service_type: Option<String>,
    ) -> &mut Self {
        self.save_profile(save_profile)
            .hide_spinner(false)
            .with_if(skip_checks, |b| b.skip_checks(true));

        if let Some(rust_log) = rust_log {
            self.rust_log(rust_log);
        }

        if let Some(service_type) = service_type {
            self.service_type(service_type);
        }

        self
    }

    pub(crate) fn append_spu_config(
        &mut self,
        spu_replicas: u16,
        spu_config: super::SpuCliConfig,
    ) -> &mut Self {
        self.spu_replicas(spu_replicas)
            .spu_config(spu_config.as_spu_config())
    }

    pub(crate) fn append_k8s_options(&mut self, k8_config: K8Install) -> &mut Self {
        if let Some(chart_location) = k8_config.chart_location {
            self.local_chart(chart_location);
        }

        if let Some(registry) = k8_config.registry {
            self.image_registry(registry);
        }

        if let Some(image_tag) = k8_config.image_version {
            self.image_tag(image_tag.trim());
        }

        self.namespace(k8_config.namespace)
            .chart_version(k8_config.chart_version)
            .group_name(k8_config.group_name)
            .tls_client_secret_name(k8_config.tls_client_secret_name)
            .tls_server_secret_name(k8_config.tls_server_secret_name)
            .chart_values(k8_config.chart_values)
            .use_k8_port_forwarding(k8_config.use_k8_port_forwarding)
            .use_cluster_ip(k8_config.use_cluster_ip)
    }

    pub(crate) fn append_connection_options(
        &mut self,
        connection_config: ClusterConnectionOpts,
    ) -> Result<&mut Self> {
        let (client, server): (TlsPolicy, TlsPolicy) = connection_config.tls.try_into()?;
        self.tls(client, server);
        if cfg!(target_os = "macos") {
            self.proxy_addr(
                connection_config
                    .proxy_addr
                    .unwrap_or_else(|| String::from("localhost")),
            );
        } else {
            self.proxy_addr(connection_config.proxy_addr);
        }

        if let Some(map) = connection_config.authorization_config_map {
            self.authorization_config_map(map);
        }

        Ok(self)
    }

    pub(crate) async fn build_and_start(&self, setup: bool, upgrade: bool) -> Result<()> {
        let config = self.build()?;

        debug!("cluster config: {:#?}", config);
        let installer = ClusterInstaller::from_config(config, upgrade)?;
        if setup {
            setup_k8(&installer).await
        } else {
            start_k8(&installer).await
        }
    }
}

async fn start_k8(installer: &ClusterInstaller) -> Result<()> {
    installer.install_fluvio().await?;
    Ok(())
}

async fn setup_k8(installer: &ClusterInstaller) -> Result<()> {
    installer.preflight_check(false).await?;
    Ok(())
}
