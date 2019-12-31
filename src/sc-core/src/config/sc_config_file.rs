//!
//! # Config file for Streaming Controller
//!
//! Given a configuration file, load and return sc parameters
//!

use serde::Deserialize;
use std::fs::read_to_string;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::Path;

use super::{ScConfig, ScConfigBuilder};

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Debug, PartialEq, Deserialize)]
pub struct ScConfigFile {
    pub version: String,
    sc: Option<ScGroup>,
    bind_public: Option<BindGroup>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ScGroup {
    pub id: i32,
}

#[derive(Debug, PartialEq, Deserialize)]
struct BindGroup {
    pub host: String,
    pub port: u16,
}

// ---------------------------------------
// Implementation
// ---------------------------------------

impl ScConfigBuilder for ScConfigFile {
    fn to_sc_config(&self) -> Result<ScConfig, IoError> {
        let mut sc_config = ScConfig::default();

        // update id (if configured)
        if let Some(ref sc) = &self.sc {
            sc_config.id = sc.id
        }

        // update bind_addr (if configured)
        if let Some(ref bind_public) = &self.bind_public {
            let host_port_str = format!("{}:{}", bind_public.host, bind_public.port);

            // parse address and error if failed
            let bind_addr = host_port_str
                .parse::<SocketAddr>()
                .map_err(|err| IoError::new(ErrorKind::InvalidInput, format!("{}", err)))?;
            sc_config.public_endpoint = bind_addr.into();
        }

        Ok(sc_config)
    }
}

impl ScConfigFile {
    // read and parse the .toml file
    pub fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, IoError> {
        let file_str = read_to_string(path)?;
        toml::from_str(&file_str)
            .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))
    }
}

// ---------------------------------------
// Unit Tests
// ---------------------------------------

#[cfg(test)]
pub mod test {
    use super::*;
    use std::path::PathBuf;
    use types::defaults::{CONFIG_FILE_EXTENTION, SC_CONFIG_FILE};

    #[test]
    fn test_default_sc_config_ok() {
        let mut sc_config_path = PathBuf::new();
        sc_config_path.push("./test-data/config");
        sc_config_path.push(SC_CONFIG_FILE);
        sc_config_path.set_extension(CONFIG_FILE_EXTENTION);

        // test file generator
        assert_eq!(
            sc_config_path.clone().to_str().unwrap(),
            "./test-data/config/sc_server.toml"
        );

        // test read & parse
        let result = ScConfigFile::from_file(sc_config_path);
        assert!(result.is_ok());

        // compare with expected result
        let expected = ScConfigFile {
            version: "1.0".to_owned(),
            sc: Some(ScGroup { id: 500 }),
            bind_public: Some(BindGroup {
                host: "127.0.0.1".to_owned(),
                port: 9999,
            }),
        };
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_default_sc_config_not_found() {
        let mut sc_config_path = PathBuf::new();
        sc_config_path.push("./test-data/config/unknown.toml");
        let result = ScConfigFile::from_file(sc_config_path);

        // expecting error
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "No such file or directory (os error 2)"
        );
    }

    #[test]
    fn test_invalid_sc_config_file() {
        let mut sc_config_path = PathBuf::new();
        sc_config_path.push("./test-data/config/sc_invalid.toml");
        let result = ScConfigFile::from_file(sc_config_path);

        // expecting error
        assert!(result.is_err());
        assert!(
            format!("{}", result.unwrap_err()).contains(
            "missing field `version`")
        );
    }
}
