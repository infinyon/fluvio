use std::{fmt, str::FromStr};
use std::path::PathBuf;
use fluvio_controlplane_metadata::spg::{SpuConfig, StorageConfig};
use structopt::StructOpt;
use semver::Version;

mod local;
mod k8;
mod sys;
mod tls;

use crate::cli::ClusterCliError;
use tls::TlsOpt;

#[cfg(target_os = "macos")]
pub fn get_log_directory() -> &'static str {
    "/usr/local/var/log/fluvio"
}

#[cfg(not(target_os = "macos"))]
pub fn get_log_directory() -> &'static str {
    "/tmp"
}

#[derive(Debug)]
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

#[derive(Debug, StructOpt)]
pub struct SpuCliConfig {
    /// set spu storage size
    #[structopt(long, default_value = "10")]
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

#[derive(Debug, StructOpt)]
pub struct K8Install {
    /// k8: use specific chart version
    #[structopt(long)]
    pub chart_version: Option<semver::Version>,

    /// k8: use specific image version
    #[structopt(long)]
    pub image_version: Option<String>,

    /// k8: use custom docker registry
    #[structopt(long)]
    pub registry: Option<String>,

    /// k8
    #[structopt(long, default_value = "default")]
    pub namespace: String,

    /// k8
    #[structopt(long, default_value = "main")]
    pub group_name: String,

    /// helm chart installation name
    #[structopt(long, default_value = "fluvio")]
    pub install_name: String,

    /// Local path to a helm chart to install
    #[structopt(long)]
    pub chart_location: Option<String>,

    /// chart values
    #[structopt(long, parse(from_os_str))]
    pub chart_values: Vec<PathBuf>,
}

#[derive(Debug, StructOpt)]
pub struct StartOpt {
    /// use local image
    #[structopt(long)]
    pub develop: bool,

    #[structopt(flatten)]
    pub k8_config: K8Install,

    #[structopt(flatten)]
    pub spu_config: SpuCliConfig,

    #[structopt(long)]
    pub skip_profile_creation: bool,

    /// number of SPU
    #[structopt(long, default_value = "1")]
    pub spu: u16,

    /// RUST_LOG options
    #[structopt(long)]
    pub rust_log: Option<String>,

    /// log dir
    #[structopt(long, default_value)]
    pub log_dir: DefaultLogDirectory,

    #[structopt(long)]
    /// installing sys
    sys: bool,

    /// install local spu/sc(custom)
    #[structopt(long)]
    local: bool,

    #[structopt(flatten)]
    pub tls: TlsOpt,

    #[structopt(long)]
    pub authorization_config_map: Option<String>,

    /// Whether to skip pre-install checks, defaults to false
    #[structopt(long)]
    pub skip_checks: bool,
    /// Tries to setup necessary environment for cluster startup
    #[structopt(long)]
    pub setup: bool,

    /// Used to hide spinner animation for progress updates
    #[structopt(long)]
    pub hide_spinner: bool,

    /// Proxy address
    #[structopt(long)]
    pub proxy_addr: Option<String>,

    /// Service Type
    #[structopt(long)]
    pub service_type: Option<String>,

    /// Connector Prefix
    #[structopt(long, name = "connector_prefix")]
    pub connector_prefix: Vec<String>,
}

impl StartOpt {
    pub async fn process(
        self,
        platform_version: Version,
        upgrade: bool,
        skip_sys: bool, // only applies to upgrade
    ) -> Result<(), ClusterCliError> {
        use crate::cli::start::local::process_local;
        use crate::cli::start::sys::process_sys;
        use crate::cli::start::k8::process_k8;

        if self.sys {
            process_sys(&self, upgrade)?;
        } else if self.local {
            process_local(self, platform_version).await?;
        } else {
            // if upgrade and not skip sys, invoke sys
            if upgrade && !skip_sys {
                process_sys(&self, upgrade)?;
            }
            process_k8(self, platform_version, upgrade, skip_sys).await?;
        }

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct UpgradeOpt {
    #[structopt(flatten)]
    start: StartOpt,
    /// Whether to skip upgrading the sys chart
    #[structopt(long)]
    skip_sys: bool,
}

impl UpgradeOpt {
    pub async fn process(self, platform_version: Version) -> Result<(), ClusterCliError> {
        self.start
            .process(platform_version, true, self.skip_sys)
            .await?;
        Ok(())
    }
}
