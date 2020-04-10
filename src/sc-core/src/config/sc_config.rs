//!
//! # Streaming Controller Configurations
//!
//! Stores configuration parameter used by Streaming Controller module.
//!
use std::io::Error as IoError;

use types::defaults::SC_PUBLIC_PORT;
use types::defaults::SC_PRIVATE_PORT;


// -----------------------------------
// Traits
// -----------------------------------

pub trait ScConfigBuilder {
    fn to_sc_config(self) -> Result<ScConfig, IoError>;
}


/// streaming controller configuration file
#[derive(Debug, Clone, PartialEq)]
pub struct ScConfig {
    pub public_endpoint: String,
    pub private_endpoint: String,
    pub run_k8_dispatchers: bool,
    pub namespace: String,
}


impl ::std::default::Default for ScConfig {
    fn default() -> Self {

        ScConfig {
            public_endpoint: format!("0.0.0.0:{}",SC_PUBLIC_PORT),
            private_endpoint: format!("0.0.0.0:{}",SC_PRIVATE_PORT),
            run_k8_dispatchers: true,
            namespace: "default".to_owned(),

        }
    }
}
