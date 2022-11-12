//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
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
}

impl FluvioConfig {
    /// get current cluster config from default profile
    pub fn load() -> Result<Self, FluvioError> {
        let config_file = ConfigFile::load_default_or_new()?;
        let cluster_config = config_file.config().current_cluster()?;
        Ok(cluster_config.to_owned())
    }

    /// Create a new cluster configuration with no TLS.
    pub fn new<S: Into<String>>(addr: S) -> Self {
        Self {
            endpoint: addr.into(),
            use_spu_local_address: false,
            tls: TlsPolicy::Disabled,
            client_id: None,
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls<T: Into<TlsPolicy>>(mut self, tls: T) -> Self {
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
