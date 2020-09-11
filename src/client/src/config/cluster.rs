//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use serde::{Serialize, Deserialize};

use crate::config::TlsPolicy;

/// Public configuration for the cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ClusterConfig {
    /// The address to connect to the cluster
    // TODO use a validated address type.
    // We don't want to have a "" address.
    pub addr: String,
    /// The TLS policy to use when connecting to the cluster
    // If no TLS field is present in config file,
    // use the default of NoTls
    #[serde(default)]
    pub tls: TlsPolicy,
}

impl ClusterConfig {
    /// Create a new cluster configuration with no TLS.
    pub fn new<S: Into<String>>(addr: S) -> Self {
        Self {
            addr: addr.into(),
            tls: TlsPolicy::Disabled,
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls<T: Into<TlsPolicy>>(mut self, tls: T) -> Self {
        self.tls = tls.into();
        self
    }
}
