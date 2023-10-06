//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use std::{str::FromStr, io::ErrorKind, fmt::Display};

use serde::{Serialize, Deserialize};

use crate::{config::TlsPolicy, FluvioError};

use super::ConfigFile;

/// Fluvio Cluster Target Configuration
/// This is part of profile
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FluvioConfig {
    /// The address to connect to the Fluvio cluster
    // TODO use a validated address type.
    // We don't want to have a "" address.
    #[serde(alias = "addr")]
    pub endpoint: String,

    #[serde(default)]
    pub use_spu_local_address: bool,

    /// The TLS policy to use when connecting to the cluster
    // If no TLS field is present in config file,
    // use the default of NoTls
    #[serde(default)]
    pub tls: TlsPolicy,

    /// This is not part of profile and doesn't persist.
    /// It is purely to override client id when creating ClientConfig
    #[serde(skip)]
    pub client_id: Option<String>,

    #[serde(default)]
    pub kind: ClusterKind,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterKind {
    Local,
    #[default]
    K8s,
}

impl FromStr for ClusterKind {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "k8s" => Ok(Self::K8s),
            "local" => Ok(Self::Local),
            _ => Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "unable to parse cluster kind",
            )),
        }
    }
}

impl Display for ClusterKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterKind::Local => write!(f, "local"),
            ClusterKind::K8s => write!(f, "k8s"),
        }
    }
}

impl FluvioConfig {
    /// get current cluster config from default profile
    pub fn load() -> Result<Self, FluvioError> {
        let config_file = ConfigFile::load_default_or_new()?;
        let cluster_config = config_file.config().current_cluster()?;
        Ok(cluster_config.to_owned())
    }

    /// Create a new cluster configuration with no TLS.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            endpoint: addr.into(),
            use_spu_local_address: false,
            tls: TlsPolicy::Disabled,
            client_id: None,
            kind: ClusterKind::default(),
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls(mut self, tls: impl Into<TlsPolicy>) -> Self {
        self.tls = tls.into();
        self
    }
}

impl TryFrom<FluvioConfig> for fluvio_socket::ClientConfig {
    type Error = std::io::Error;
    fn try_from(config: FluvioConfig) -> Result<Self, Self::Error> {
        let connector = fluvio_future::net::DomainConnector::try_from(config.tls.clone())?;
        Ok(Self::new(
            &config.endpoint,
            connector,
            config.use_spu_local_address,
        ))
    }
}
