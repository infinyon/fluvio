//!
//! # Streaming Controller Configurations
//!
//! Stores configuration parameter used by Streaming Controller module.
//!
use std::collections::HashSet;
use std::{io::Error as IoError, path::PathBuf};

use fluvio_types::defaults::SC_PUBLIC_PORT;
use fluvio_types::defaults::SC_PRIVATE_PORT;

pub const DEFAULT_NAMESPACE: &str = "default";

// -----------------------------------
// Traits
// -----------------------------------

pub trait ScConfigBuilder {
    #[allow(clippy::wrong_self_convention)]
    fn to_sc_config(self) -> Result<ScConfig, IoError>;
}

/// streaming controller configuration file
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScConfig {
    pub read_only_metadata: bool,
    pub public_endpoint: String,
    pub private_endpoint: String,
    pub namespace: String,
    pub x509_auth_scopes: Option<PathBuf>,
    pub white_list: HashSet<String>,
}

impl ::std::default::Default for ScConfig {
    fn default() -> Self {
        Self {
            read_only_metadata: false,
            public_endpoint: format!("0.0.0.0:{SC_PUBLIC_PORT}"),
            private_endpoint: format!("0.0.0.0:{SC_PRIVATE_PORT}"),
            namespace: DEFAULT_NAMESPACE.to_owned(),
            x509_auth_scopes: None,
            white_list: HashSet::new(),
        }
    }
}

impl ScConfig {
    /// check if white list is enabled for controller name
    /// if white list is empty then everything is enabled,
    /// otherwise only included
    pub fn enabled(&self, name: &str) -> bool {
        if self.white_list.is_empty() {
            true
        } else {
            self.white_list.contains(name)
        }
    }
}
