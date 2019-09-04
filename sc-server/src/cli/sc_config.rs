//!
//! # Streaming Controller Configurations
//!
//! Stores configuration parameter used by Streaming Controller module.
//!
use std::io::Error as IoError;
use std::path::Path;

use types::defaults::SC_CONFIG_FILE;
use types::defaults::{SC_DEFAULT_ID, SC_PUBLIC_PORT};
use types::defaults::SC_PRIVATE_PORT;
use utils::config_helper::build_server_config_file_path;
use types::socket_helpers::EndPoint;


use super::ScConfigFile;

// -----------------------------------
// Data Structures
// -----------------------------------

/// streaming controller configuration file
#[derive(Debug, Clone, PartialEq)]
pub struct ScConfig {
    pub id: i32,
    pub public_endpoint: EndPoint,
    pub private_endpoint: EndPoint,
    pub run_k8_dispatchers: bool,
    pub namespace: String
}


// -----------------------------------
// Traits
// -----------------------------------

pub trait ScConfigBuilder {
    fn to_sc_config(&self) -> Result<ScConfig, IoError>;
}

// -----------------------------------
// Implementation
// -----------------------------------

/// initialize with default parameters
impl ::std::default::Default for ScConfig {
    fn default() -> Self {

        ScConfig {
            id: SC_DEFAULT_ID,
            public_endpoint: EndPoint::all_end_point(SC_PUBLIC_PORT),
            private_endpoint: EndPoint::all_end_point(SC_PRIVATE_PORT),
            run_k8_dispatchers: true,
            namespace: "default".to_owned()

        }
    }
}

impl ScConfig {
    /// generate sc configuration based on configuration file provided
    pub fn new(config_file: Option<String>) -> Result<Self, IoError> {
        
        let config = if let Some(file) = config_file {
            ScConfig::read_sc_config_from_custom_file(file)?
        } else {
            ScConfig::read_sc_config_from_default_file()?
        };
        Ok(config)
    }

    /// look-up custom configuration and overwrite defautl configuration parameters
    fn read_sc_config_from_custom_file(config_file: String) -> Result<Self, IoError> {
        let sc_config_file = ScConfigFile::from_file(config_file)?;
        sc_config_file.to_sc_config()
    }

    /// look-up default configuration, if it doesn't exist, return defautls
    fn read_sc_config_from_default_file() -> Result<Self, IoError> {
        let sc_file_path = build_server_config_file_path(SC_CONFIG_FILE);

        if Path::new(&sc_file_path).exists() {
            // if file exists, it must readable and correct
            let sc_config_file = ScConfigFile::from_file(sc_file_path)?;
            sc_config_file.to_sc_config()
        } else {
            // no config file, return default parameters
            Ok(ScConfig::default())
        }
    }
}
