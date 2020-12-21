use std::{fmt, str::FromStr};
use structopt::StructOpt;

mod local;
mod k8;
mod tls;

use crate::cli::ClusterCliError;
use tls::TlsOpt;

#[cfg(target_os = "macos")]
fn get_log_directory() -> &'static str {
    "/usr/local/var/log/fluvio"
}

#[cfg(not(target_os = "macos"))]
fn get_log_directory() -> &'static str {
    "/tmp"
}

#[derive(Debug)]
pub struct DefaultVersion(String);

impl Default for DefaultVersion {
    fn default() -> Self {
        Self(crate::VERSION.trim().to_string())
    }
}

impl fmt::Display for DefaultVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DefaultVersion {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
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
pub struct K8Install {
    /// k8: use specific chart version
    #[structopt(long, default_value)]
    pub chart_version: DefaultVersion,

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

    /// k8
    #[structopt(long, default_value = "minikube")]
    pub cloud: String,
}

#[derive(Debug, StructOpt)]
pub struct StartOpt {
    /// use local image
    #[structopt(long)]
    pub develop: bool,

    #[structopt(flatten)]
    pub k8_config: K8Install,

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
}

impl StartOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        use k8::install_sys;
        use k8::install_core;
        use k8::run_setup;

        use local::{install_local, run_local_setup};

        if self.sys {
            install_sys(self)?;
        } else if self.local {
            if self.setup {
                run_local_setup(self).await?;
            } else {
                install_local(self).await?;
            }
        } else if self.setup {
            run_setup(self).await?;
        } else {
            install_core(self).await?;
        }

        Ok(())
    }
}
