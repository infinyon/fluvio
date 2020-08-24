//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use serde::{Serialize, Deserialize};

use super::tls::TlsConfig;

/// Public configuration for the cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ClusterConfig {
    // TODO use a validated address type.
    // We don't want to have a "" address.
    pub addr: String,
    pub tls: Option<TlsConfig>,
}

impl ClusterConfig {
    /// Create a new cluster configuration with no TLS.
    pub fn new<S: Into<String>>(addr: S) -> Self {
        Self {
            addr: addr.into(),
            tls: None,
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}
