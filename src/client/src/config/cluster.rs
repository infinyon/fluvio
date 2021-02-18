//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use serde::{Serialize, Deserialize};
use fluvio_types::Endpoint;

use crate::config::TlsPolicy;

/// Public configuration for Fluvio.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FluvioConfig {
    /// The address to connect to the Fluvio cluster
    pub endpoint: Endpoint,
    /// The TLS policy to use when connecting to the cluster
    // If no TLS field is present in config file,
    // use the default of NoTls
    #[serde(default)]
    pub tls: TlsPolicy,
}

impl FluvioConfig {
    /// Create a new cluster configuration with no TLS.
    pub fn new<S: Into<String>>(host: S, port: u16) -> Self {
        Self {
            endpoint: Endpoint {
                host: host.into(),
                port,
            },
            tls: TlsPolicy::Disabled,
        }
    }

    /// Create a new cluster configuration with no TLS.
    pub fn new_from_endpoint(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            tls: TlsPolicy::Disabled,
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls<T: Into<TlsPolicy>>(mut self, tls: T) -> Self {
        self.tls = tls.into();
        self
    }
}
