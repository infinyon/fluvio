//!
//! # Config file for Streaming Processing Unit
//!
//! Given a configuration file, load and return spu parameters
//!

use std::fs::read_to_string;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use types::socket_helpers::ServerAddress;

use types::defaults::SPU_CONFIG_FILE;
use utils::config_helper::build_server_config_file_path;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Debug, PartialEq, Deserialize)]
pub struct SpuConfigFile {
    version: String,
    spu: Option<SpuGroup>,
    servers: Option<ServersGroup>,
    controller: Option<ControllerGroup>,
    configurations: Option<ConfigurationsGroup>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct SpuGroup {
    pub id: Option<i32>,
    pub rack: Option<String>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ServersGroup {
    pub public: Option<ServerGroup>,
    pub private: Option<ServerGroup>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ServerGroup {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ControllerGroup {
    pub host: String,
    pub port: u16,
    pub retry_timeout_ms: Option<u16>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ConfigurationsGroup {
    pub replication: Option<ReplicationGroup>,
    pub log: Option<LogGroup>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ReplicationGroup {
    pub min_in_sync_replicas: Option<u16>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct LogGroup {
    pub base_dir: Option<PathBuf>,
    pub size: Option<String>,
    pub index_max_bytes: Option<u32>,
    pub index_max_interval_bytes: Option<u32>,
    pub segment_max_bytes: Option<u32>,
}

// ---------------------------------------
// Implementation
// ---------------------------------------

impl SpuConfigFile {
    // read and parse the .toml file
    pub fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, IoError> {
        let filename = { format!("{}", path.as_ref().display()) };
        let file_str = read_to_string(path)?;
        toml::from_str(&file_str)
            .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}: {}", filename, err)))
    }

    // parse the default file if exists, otherwise None
    pub fn from_default_file() -> Result<Option<Self>, IoError> {
        let default_cfg_file_path = build_server_config_file_path(SPU_CONFIG_FILE);
        if Path::new(&default_cfg_file_path).exists() {
            Ok(Some(SpuConfigFile::from_file(default_cfg_file_path)?))
        } else {
            Ok(None)
        }
    }

   

    /// Retrieve rack or none
    pub fn rack(&self) -> Option<String> {
        if let Some(ref spu_group) = &self.spu {
            if let Some(ref rack) = spu_group.rack {
                return Some(rack.clone());
            }
        }
        None
    }

    /// Retrieve public endpoint or none
    pub fn public_endpoint(&self) -> Option<ServerAddress> {
        if let Some(ref servers_group) = &self.servers {
            if let Some(ref server) = servers_group.public {
                return Some(ServerAddress {
                    host: server.host.clone(),
                    port: server.port,
                });
            }
        }
        None
    }

    /// Retrieve private endpoint or none
    pub fn private_endpoint(&self) -> Option<ServerAddress> {
        if let Some(ref servers_group) = &self.servers {
            if let Some(ref server) = servers_group.private {
                return Some(ServerAddress {
                    host: server.host.clone(),
                    port: server.port,
                });
            }
        }
        None
    }

    /// Retrieve controller private endpoint or none
    pub fn controller_endpoint(&self) -> Option<ServerAddress> {
        if let Some(ref controller) = &self.controller {
            return Some(ServerAddress {
                host: controller.host.clone(),
                port: controller.port,
            });
        }
        None
    }
    /// Retrieve controller connection retry in miliseconds or none
    pub fn sc_retry_ms(&self) -> Option<u16> {
        if let Some(ref controller) = &self.controller {
            return controller.retry_timeout_ms.clone();
        }
        None
    }

    /// Retrieve minim in sync replicas or none
    pub fn min_in_sync_replicas(&self) -> Option<u16> {
        if let Some(ref config_group) = &self.configurations {
            if let Some(ref replication_group) = &config_group.replication {
                return replication_group.min_in_sync_replicas.clone();
            }
        }
        None
    }

    /// Retrieve log base directory or none
    pub fn log_base_dir(&self) -> Option<PathBuf> {
        if let Some(ref config_group) = &self.configurations {
            if let Some(ref log_group) = &config_group.log {
                return log_group.base_dir.clone();
            }
        }
        None
    }

    /// Retrieve log size or none
    pub fn log_size(&self) -> Option<String> {
        if let Some(ref config_group) = &self.configurations {
            if let Some(ref log_group) = &config_group.log {
                return log_group.size.clone();
            }
        }
        None
    }

    /// Retrieve log index max bytes or none
    pub fn log_index_max_bytes(&self) -> Option<u32> {
        if let Some(ref config_group) = &self.configurations {
            if let Some(ref log_group) = &config_group.log {
                return log_group.index_max_bytes.clone();
            }
        }
        None
    }

    /// Retrieve log index max interval bytes or none
    pub fn log_index_max_interval_bytes(&self) -> Option<u32> {
        if let Some(ref config_group) = &self.configurations {
            if let Some(ref log_group) = &config_group.log {
                return log_group.index_max_interval_bytes.clone();
            }
        }
        None
    }

    /// Retrieve segment max bytes or none
    pub fn log_segment_max_bytes(&self) -> Option<u32> {
        if let Some(ref config_group) = &self.configurations {
            if let Some(ref log_group) = &config_group.log {
                return log_group.segment_max_bytes.clone();
            }
        }
        None
    }
}

// ---------------------------------------
// Unit Tests
//      >> utils::init_logger();
//      >> RUST_LOG=spu_server=trace cargo test <test-name>
// ---------------------------------------

#[cfg(test)]
pub mod test {
    use types::defaults::CONFIG_FILE_EXTENTION;
    use super::*;

    /// Use a base path and defaults to stitch together th spu configuration file
    pub fn config_file(path: &String) -> PathBuf {
        let mut file_path = PathBuf::from(path);
        file_path.push(SPU_CONFIG_FILE);
        file_path.set_extension(CONFIG_FILE_EXTENTION);
        file_path
    }

    #[test]
    fn test_default_spu_config_ok() {
        let spu_config_path = config_file(&"./test-data/config".to_owned());

        // test file generator
        assert_eq!(
            spu_config_path.clone().to_str().unwrap(),
            "./test-data/config/spu_server.toml"
        );

        // test read & parse
        let result = SpuConfigFile::from_file(spu_config_path);
        assert!(result.is_ok());

        // compare with expected result
        let expected = SpuConfigFile {
            version: "1.0".to_owned(),
            spu: Some(SpuGroup {
                id: Some(5050),
                rack: Some("rack-1".to_owned()),
            }),
            servers: Some(ServersGroup {
                public: Some(ServerGroup {
                    host: "127.0.0.1".to_owned(),
                    port: 5555,
                }),
                private: Some(ServerGroup {
                    host: "127.0.0.1".to_owned(),
                    port: 5556,
                }),
            }),
            controller: Some(ControllerGroup {
                host: "127.0.0.1".to_owned(),
                port: 5554,
                retry_timeout_ms: Some(2000),
            }),
            configurations: Some(ConfigurationsGroup {
                replication: Some(ReplicationGroup {
                    min_in_sync_replicas: Some(3),
                }),
                log: Some(LogGroup {
                    base_dir: Some(PathBuf::from("/tmp/data_streams")),
                    size: Some("2Gi".to_owned()),
                    index_max_bytes: Some(888888),
                    index_max_interval_bytes: Some(2222),
                    segment_max_bytes: Some(9999999),
                }),
            }),
        };
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_default_spu_config_min() {
        let mut spu_config_path = PathBuf::new();
        spu_config_path.push("./test-data/config/spu_server_small.toml");

        // test read & parse
        let result = SpuConfigFile::from_file(spu_config_path);
        assert!(result.is_ok());

        // compare with expected result
        let expected = SpuConfigFile {
            version: "1.0".to_owned(),
            spu: Some(SpuGroup {
                id: Some(12),
                rack: None,
            }),
            servers: None,
            controller: Some(ControllerGroup {
                host: "1.1.1.1".to_owned(),
                port: 2323,
                retry_timeout_ms: None,
            }),
            configurations: None,
        };
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_default_spu_config_not_found() {
        let mut spu_config_path = PathBuf::new();
        spu_config_path.push("./test-data/config/unknown.toml");
        let result = SpuConfigFile::from_file(spu_config_path);

        // expecting error
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "No such file or directory (os error 2)"
        );
    }

    #[test]
    fn test_invalid_spu_config_file() {
        let mut spu_config_path = PathBuf::new();
        spu_config_path.push("./test-data/config/spu_invalid.toml");
        let result = SpuConfigFile::from_file(spu_config_path);

        // expecting error
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "./test-data/config/spu_invalid.toml: missing field `version`"
        );
    }
}
