//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use serde::{Serialize, Deserialize};

use crate::config::TlsPolicy;

/// Public configuration for Fluvio.
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
}

impl FluvioConfig {
    /// Create a new cluster configuration with no TLS.
    pub fn new<S: Into<String>>(addr: S) -> Self {
        Self {
            endpoint: addr.into(),
            use_spu_local_address: false,
            tls: TlsPolicy::Disabled,
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls<T: Into<TlsPolicy>>(mut self, tls: T) -> Self {
        self.tls = tls.into();
        self
    }
}
