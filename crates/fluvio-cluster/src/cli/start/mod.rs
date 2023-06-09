use std::{fmt, str::FromStr};
use std::path::PathBuf;

use clap::Parser;
use semver::Version;
use anyhow::Result;

use fluvio_controlplane_metadata::spg::{SpuConfig, StorageConfig};
use fluvio_types::defaults::{TLS_SERVER_SECRET_NAME, TLS_CLIENT_SECRET_NAME};

mod local;
mod k8;
mod sys;
mod tls;

use tls::TlsOpt;

#[cfg(target_os = "macos")]
pub fn get_log_directory() -> &'static str {
    "/usr/local/var/log/fluvio"
}

#[cfg(not(target_os = "macos"))]
pub fn get_log_directory() -> &'static str {
    "/tmp"
}

#[derive(Debug, Clone)]
pub struct DefaultLogDirectory(String);

impl Default for DefaultLogDirectory {
    fn default() -> Self {
        Self(get_log_directory().to_string())
    }
}

impl fmt::Display for DefaultLogDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DefaultLogDirectory {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

#[derive(Debug, Parser)]
pub struct SpuCliConfig {
    /// set spu storage size
    #[arg(long, default_value = "10")]
    pub spu_storage_size: u16,
}

impl SpuCliConfig {
    pub fn as_spu_config(&self) -> SpuConfig {
        SpuConfig {
            storage: Some(StorageConfig {
                size: Some(format!("{}Gi", self.spu_storage_size)),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Debug, Parser)]
pub struct K8Install {
    /// k8: use specific chart version
    #[arg(long)]
    pub chart_version: Option<semver::Version>,

    /// k8: use specific image version
    #[arg(long)]
    pub image_version: Option<String>,

    /// k8: use custom docker registry
    #[arg(long)]
    pub registry: Option<String>,

    /// k8 namespace
    #[arg(long, default_value = "default")]
    pub namespace: String,

    /// k8
    #[arg(long, default_value = "main")]
    pub group_name: String,

    /// helm chart installation name
    #[arg(long, default_value = "fluvio")]
    pub install_name: String,

    /// Local path to a helm chart to install
    #[arg(long)]
    pub chart_location: Option<String>,

    /// chart values
    #[arg(long)]
    pub chart_values: Vec<PathBuf>,

    /// Uses port forwarding for connecting to SC during install
    #[arg(long)]
    use_k8_port_forwarding: bool,

    /// TLS: Client secret name while adding to Kubernetes
    #[arg(long, default_value = TLS_CLIENT_SECRET_NAME)]
    tls_client_secret_name: String,

    /// TLS: Server secret name while adding to Kubernetes
    #[arg(long, default_value = TLS_SERVER_SECRET_NAME)]
    tls_server_secret_name: String,
}

#[derive(Debug, Parser)]
pub struct StartOpt {
    /// use local image
    #[arg(long)]
    pub develop: bool,

    #[clap(flatten)]
    pub k8_config: K8Install,

    #[clap(flatten)]
    pub spu_config: SpuCliConfig,

    #[arg(long)]
    pub skip_profile_creation: bool,

    /// number of SPU
    #[arg(long, default_value = "1")]
    pub spu: u16,

    /// RUST_LOG options
    #[arg(long)]
    pub rust_log: Option<String>,

    /// log dir
    #[arg(long, default_value_t)]
    pub log_dir: DefaultLogDirectory,

    #[arg(long)]
    /// installing/upgrade sys only
    sys_only: bool,

    /// install local spu/sc(custom)
    #[arg(long)]
    local: bool,

    #[clap(flatten)]
    pub tls: TlsOpt,

    #[arg(long)]
    pub authorization_config_map: Option<String>,

    /// Whether to skip pre-install checks, defaults to false
    #[arg(long)]
    pub skip_checks: bool,
    /// Tries to setup necessary environment for cluster startup
    #[arg(long)]
    pub setup: bool,

    /// Proxy address
    #[arg(long)]
    pub proxy_addr: Option<String>,

    /// Service Type
    #[arg(long)]
    pub service_type: Option<String>,
}

impl StartOpt {
    pub async fn process(self, platform_version: Version, upgrade: bool) -> Result<()> {
        use crate::cli::start::local::process_local;
        use crate::cli::start::sys::process_sys;
        use crate::cli::start::k8::process_k8;

        if self.sys_only {
            process_sys(&self, upgrade)?;
        } else if self.local {
            process_local(self, platform_version).await?;
        } else {
            process_k8(self, platform_version, upgrade).await?;
        }

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct UpgradeOpt {
    #[clap(flatten)]
    pub start: StartOpt,
}

impl UpgradeOpt {
    pub async fn process(self, platform_version: Version) -> Result<()> {
        self.start.process(platform_version, true).await?;
        Ok(())
    }
}
