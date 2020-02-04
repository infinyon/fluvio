//!
//! # Streaming Processing Unit Configurations
//!
//! Stores configuration parameter used by Streaming Processing Unit module.
//! Parameters looked-up in following sequence (first value wins):
//!     1) cli parameters
//!     2) environment variables
//!     3) custom configuration or default configuration (from file)
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::env;
use std::path::PathBuf;

use log::debug;
use log::error;
use log::info;

// defaults values
use types::defaults::{SPU_PUBLIC_HOSTNAME, SPU_PUBLIC_PORT};
use types::defaults::{SPU_PRIVATE_HOSTNAME, SPU_PRIVATE_PORT};
use types::defaults::{SC_HOSTNAME, SC_PRIVATE_PORT};
use types::defaults::SPU_RETRY_SC_TIMEOUT_MS;
use types::defaults::SPU_MIN_IN_SYNC_REPLICAS;
use types::defaults::SPU_LOG_BASE_DIR;
use types::defaults::SPU_LOG_SIZE;
use types::defaults::SPU_LOG_INDEX_MAX_BYTES;
use types::defaults::SPU_LOG_INDEX_MAX_INTERVAL_BYTES;
use types::defaults::SPU_LOG_SEGMENT_MAX_BYTES;

// environment variables
use types::defaults::FLV_SPU_ID;
use types::defaults::FLV_SPU_TYPE;
use types::defaults::FLV_RACK;
use types::defaults::{FLV_SPU_PUBLIC_HOST, FLV_SPU_PUBLIC_PORT};
use types::defaults::{FLV_SPU_PRIVATE_HOST, FLV_SPU_PRIVATE_PORT};
use types::defaults::{FLV_SC_PRIVATE_HOST, FLV_SC_PRIVATE_PORT};
use types::defaults::FLV_SC_RETRY_TIMEOUT_MS;
use types::defaults::FLV_REPLICA_IN_SYNC_REPLICA_MIN;
use types::defaults::FLV_LOG_BASE_DIR;
use types::defaults::FLV_LOG_SIZE;
use types::defaults::FLV_LOG_INDEX_MAX_BYTES;
use types::defaults::FLV_LOG_INDEX_MAX_INTERVAL_BYTES;
use types::defaults::FLV_LOG_SEGMENT_MAX_BYTES;

use types::SpuId;
use types::socket_helpers::ServerAddress;
use types::socket_helpers::server_to_socket_addr;
use flv_storage::ConfigOption;

use super::{SpuOpt, SpuConfigFile};

#[derive(Debug, PartialEq, Clone)]
pub enum SpuType {
    Custom,
    Managed,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Endpoint {
    pub socket_addr: SocketAddr,
    pub server_addr: ServerAddress,
}


#[derive(Debug, PartialEq, Clone)]
pub struct Replication {
    pub min_in_sync_replicas: u16,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Log {
    pub base_dir: PathBuf,
    pub size: String,
    pub index_max_bytes: u32,
    pub index_max_interval_bytes: u32,
    pub segment_max_bytes: u32,
}

impl Log {

    /// create new storage config
    pub fn new_config(&self) -> ConfigOption {
        ConfigOption::new(
            self.base_dir.clone(),
            self.index_max_bytes, 
            self.index_max_interval_bytes, 
            self.segment_max_bytes
        )     
    }
}



/// streaming processing unit configuration file
#[derive(Debug, PartialEq, Clone)]
pub struct SpuConfig {
    pub id: SpuId,
    pub spu_type: SpuType,
    pub rack: Option<String>,

    // spu (local server) points
    pub public_endpoint: Endpoint,
    pub private_endpoint: Endpoint,

    // sc (remote server) endpoint
    pub sc_endpoint: ServerAddress,
    pub sc_retry_ms: u16,

    // parameters
    pub replication: Replication,
    pub log: Log
}



impl SpuConfig {
    /// Creates an SPU Config object by merging object in the following order of precedence:
    ///  * cli configuration
    ///  * environment variable
    ///  * config file
    pub fn new_from_all(
        cli_cfg: SpuOpt,
        file_cfg: Option<SpuConfigFile>,
    ) -> Result<SpuConfig, IoError> {
        let spu_id = SpuConfig::make_spu_id(&cli_cfg, &file_cfg)?;
        let spu_type = SpuConfig::make_spu_type()?;
        let rack = SpuConfig::make_rack(&file_cfg)?;
        let public_endpoint = SpuConfig::make_public_endpoint(&cli_cfg, &file_cfg)?;
        let private_endpoint = SpuConfig::make_private_endpoint(&cli_cfg, &file_cfg)?;
        let sc_endpoint = SpuConfig::make_sc_endpoint(&cli_cfg, &file_cfg)?;
        let sc_retry_ms = SpuConfig::make_sc_retry_ms(&file_cfg)?;
        let min_in_sync_replicas = SpuConfig::make_min_in_sync_replicas(&file_cfg)?;
        let log_base_dir = SpuConfig::make_log_base_dir(&file_cfg)?;
        let log_size = SpuConfig::make_log_size(&file_cfg)?;
        let log_index_max_bytes = SpuConfig::make_log_index_max_bytes(&file_cfg)?;
        let log_index_max_interval_bytes = SpuConfig::make_log_index_max_interval_bytes(&file_cfg)?;
        let log_segment_max_bytes = SpuConfig::make_log_segment_max_bytes(&file_cfg)?;

        Ok(SpuConfig {
            id: spu_id,
            spu_type: spu_type,
            rack: rack,
            public_endpoint: public_endpoint,
            private_endpoint: private_endpoint,
            sc_endpoint: sc_endpoint,
            sc_retry_ms: sc_retry_ms,
            replication: Replication {
                min_in_sync_replicas: min_in_sync_replicas,
            },
            log: Log {
                base_dir: log_base_dir,
                size: log_size,
                index_max_bytes: log_index_max_bytes,
                index_max_interval_bytes: log_index_max_interval_bytes,
                segment_max_bytes: log_segment_max_bytes,
            }
        })
    }


    /// Generate spu-id by combining all config elements. Returns error on failure.
    fn make_spu_id(cli_cfg: &SpuOpt, _file_cfg: &Option<SpuConfigFile>) -> Result<SpuId, IoError> {
        // 1) check cli
        let spu_id = cli_cfg.id;

        // 2) environment variable (optional field, ignore errors)
        if let Some(some_id) = spu_id {
            debug!("spu id is supplied by config: {}",some_id);
            Ok(some_id)
        } else {
            debug!("no spu is supplied, looking up env");
            if let Ok(id_str) = env::var(FLV_SPU_ID) {
                debug!("found spu id from env: {}",id_str);
                let id = id_str.parse().map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("spu-id: {}", err))
                })?;
                Ok(id)
            } else {
                
                // try get special env SPU which has form of {}-{id} when in as in-cluster config
                if let Ok(spu_name) = env::var("SPU_INDEX") {
                    info!("extracting SPU from: {}",spu_name);
                    let spu_tokens: Vec<&str> = spu_name.split('-').collect();
                    if spu_tokens.len() < 2 {
                        error!("SPU is invalid format. bailing out");
                    } else {
                        let spu_token = spu_tokens[spu_tokens.len()-1];
                        let id: SpuId = match spu_token.parse() {
                            Ok(id) => id,
                            Err(err) => {
                                error!("invalid spu id: {}. terminating it",err);
                                std::process::exit(-1);
                            }
                        };
                        info!("found SPU INDEX ID: {}",id);

                        // now we get SPU_MIN which tells min
                        let spu_min_var = env::var("SPU_MIN").unwrap_or("0".to_owned());
                        debug!("found SPU MIN ID: {}",spu_min_var);
                        let base_id: SpuId = spu_min_var.parse().expect("spu min should be integer");
                        return Ok(id + base_id)
                    }
                } else {
                    error!("no spu founded from env. this is bad");
                }

                for (key, value) in env::vars() {
                    debug!("{}: {}", key, value);
                }   
                std::process::exit(0x0100);
            }
        }
    
    }

    

    /// Generate spu-type base on the presence of the environment variable. Returns error on failure.
    fn make_spu_type() -> Result<SpuType, IoError> {
        // look-up environment variable
        if let Ok(spu_type) = env::var(FLV_SPU_TYPE) {
            // match to appropriate values
            match spu_type.as_str() {
                "Custom" => Ok(SpuType::Custom),
                "Managed" => Ok(SpuType::Managed),
                _ => Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "spu-type: expected 'Managed' or 'Custom', found '{}'",
                        spu_type
                    ),
                )),
            }
        } else {
            // default to Custom, if environment variable is not present
            Ok(SpuType::Custom)
        }
    }

    /// Generate rack by combining all config elements. Returns error on failure.
    fn make_rack(file_cfg: &Option<SpuConfigFile>) -> Result<Option<String>, IoError> {
        let mut rack = None;

        // 1) environment variable (optional field, ignore errors)
        if rack.is_none() {
            if let Ok(rack_str) = env::var(FLV_RACK) {
                rack = Some(rack_str);
            }
        }

        // 2) config file
        if rack.is_none() && file_cfg.is_some() {
            rack = file_cfg.as_ref().unwrap().rack();
        }

        // return result
        Ok(rack)
    }

    /// Generate public_endpoint by combining all config elements. Returns error on failure.
    fn make_public_endpoint(
        cli_cfg: &SpuOpt,
        file_cfg: &Option<SpuConfigFile>,
    ) -> Result<Endpoint, IoError> {
        // 1) check cli and convert to server address
        let mut public_ep = server_str_to_server_addr(&cli_cfg.public_server)?;

        // 2) environment variable (optional field, ignore errors)
        if public_ep.is_none() {
            if let Ok(host) = env::var(FLV_SPU_PUBLIC_HOST) {
                if let Ok(port_str) = env::var(FLV_SPU_PUBLIC_PORT) {
                    let port: u16 = port_str.parse().map_err(|err| {
                        IoError::new(
                            ErrorKind::InvalidInput,
                            format!("invalid env port: {}", err),
                        )
                    })?;
                    public_ep = Some(ServerAddress { host, port });
                }
            }
        }

        // 3) config file
        if public_ep.is_none() && file_cfg.is_some() {
            public_ep = file_cfg.as_ref().unwrap().public_endpoint();
        }

        // 4) use default
        if public_ep.is_none() {
            let host = SPU_PUBLIC_HOSTNAME.to_owned();
            let port = SPU_PUBLIC_PORT;
            public_ep = Some(ServerAddress { host, port });
        }

        // 5) create endpoint
        let ep = Endpoint::new(&public_ep.unwrap())?;

        // return result
        Ok(ep)
    }

    /// Generate private_endpoint by combining all config elements. Returns error on failure.
    fn make_private_endpoint(
        cli_cfg: &SpuOpt,
        file_cfg: &Option<SpuConfigFile>,
    ) -> Result<Endpoint, IoError> {
        // 1) check cli and convert to server address
        let mut private_ep = server_str_to_server_addr(&cli_cfg.private_server)?;

        // 2) environment variable (optional field, ignore errors)
        if private_ep.is_none() {
            if let Ok(host) = env::var(FLV_SPU_PRIVATE_HOST) {
                if let Ok(port_str) = env::var(FLV_SPU_PRIVATE_PORT) {
                    let port: u16 = port_str.parse().map_err(|err| {
                        IoError::new(
                            ErrorKind::InvalidInput,
                            format!("invalid env port: {}", err),
                        )
                    })?;
                    private_ep = Some(ServerAddress { host, port });
                }
            }
        }

        // 3) config file
        if private_ep.is_none() && file_cfg.is_some() {
            private_ep = file_cfg.as_ref().unwrap().private_endpoint();
        }

        // 4) use default
        if private_ep.is_none() {
            let host = SPU_PRIVATE_HOSTNAME.to_owned();
            let port = SPU_PRIVATE_PORT;
            private_ep = Some(ServerAddress { host, port });
        }

        // 5) create endpoint
        let ep = Endpoint::new(&private_ep.unwrap())?;

        // return result
        Ok(ep)
    }

    /// Generate sc_endpoint by combining all config elements. Returns error on failure.
    fn make_sc_endpoint(
        cli_cfg: &SpuOpt,
        file_cfg: &Option<SpuConfigFile>,
    ) -> Result<ServerAddress, IoError> {
        // 1) check cli and convert to server address
        let mut sc_ep = server_str_to_server_addr(&cli_cfg.sc_server)?;

        // 2) environment variable (optional field, ignore errors)
        if sc_ep.is_none() {
            debug!("no sc endpoint is supplied. checking env");
            if let Ok(host) = env::var(FLV_SC_PRIVATE_HOST) {
                debug!("found sc addr from env var: {}",host);
                if let Ok(port_str) = env::var(FLV_SC_PRIVATE_PORT) {
                    let port: u16 = port_str.parse().map_err(|err| {
                        IoError::new(
                            ErrorKind::InvalidInput,
                            format!("invalid env port: {}", err),
                        )
                    })?;
                    return Ok(ServerAddress { host, port });
                } else {
                    debug!("no port supplied with env var, using default port: {}",SC_PRIVATE_PORT);
                    return Ok(ServerAddress { host, port: SC_PRIVATE_PORT})
                }
            } else {
                debug!("no sc endpoint from env var");
            }
        }

        // 3) config file
        if sc_ep.is_none() && file_cfg.is_some() {
            debug!("try reading sc from file");
            sc_ep = file_cfg.as_ref().unwrap().controller_endpoint();
        }

        // 4) use default
        match sc_ep {
            Some(addr) => Ok(addr),
            None => {
                debug!("no sc endpoint from any config source, default to localhost");
                let host = SC_HOSTNAME.to_owned();
                let port = SC_PRIVATE_PORT;
                Ok(ServerAddress { host, port })
            }
        }
    }

    /// Generate retry SC connection by combining all config elements. Returns error on failure.
    fn make_sc_retry_ms(file_cfg: &Option<SpuConfigFile>) -> Result<u16, IoError> {
        let mut sc_retry_ms = None;

        // 1) environment variable (optional field, ignore errors)
        if let Ok(retry_ms_str) = env::var(FLV_SC_RETRY_TIMEOUT_MS) {
            let retry_ms = retry_ms_str.parse().map_err(|err| {
                IoError::new(ErrorKind::InvalidInput, format!("sc-retry-ms: {}", err))
            })?;
            sc_retry_ms = Some(retry_ms);
        }

        // 2) config file
        if sc_retry_ms.is_none() && file_cfg.is_some() {
            sc_retry_ms = file_cfg.as_ref().unwrap().sc_retry_ms();
        }

        // 3) unwrap or use default
        Ok(sc_retry_ms.unwrap_or(SPU_RETRY_SC_TIMEOUT_MS))
    }

    /// Generate min in-sync replicas by combining all config elements. Returns error on failure.
    fn make_min_in_sync_replicas(file_cfg: &Option<SpuConfigFile>) -> Result<u16, IoError> {
        let mut make_min_in_sync_replicas = None;

        // 1) environment variable (optional field, ignore errors)
        if let Ok(in_sync_replica_str) = env::var(FLV_REPLICA_IN_SYNC_REPLICA_MIN) {
            let in_sync_replica = in_sync_replica_str.parse().map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidInput,
                    format!("min-in-sync-replica: {}", err),
                )
            })?;
            make_min_in_sync_replicas = Some(in_sync_replica);
        }

        // 2) config file
        if make_min_in_sync_replicas.is_none() && file_cfg.is_some() {
            make_min_in_sync_replicas = file_cfg.as_ref().unwrap().min_in_sync_replicas();
        }

        // 3) unwrap or use default
        Ok(make_min_in_sync_replicas.unwrap_or(SPU_MIN_IN_SYNC_REPLICAS))
    }

    /// Generate log base dir by combining all config elements. Returns error on failure.
    fn make_log_base_dir(file_cfg: &Option<SpuConfigFile>) -> Result<PathBuf, IoError> {
        let mut log_base_dir = None;

        // 1) environment variable (optional field, ignore errors)
        if let Ok(log_base_dir_str) = env::var(FLV_LOG_BASE_DIR) {
            log_base_dir = Some(PathBuf::from(log_base_dir_str));
        }

        // 2) config file
        if log_base_dir.is_none() && file_cfg.is_some() {
            log_base_dir = file_cfg.as_ref().unwrap().log_base_dir();
        }

        // 3) unwrap or use default
        Ok(log_base_dir.unwrap_or(PathBuf::from(SPU_LOG_BASE_DIR)))
    }

    /// Generate log size by combining all config elements. Returns error on failure.
    fn make_log_size(file_cfg: &Option<SpuConfigFile>) -> Result<String, IoError> {
        let mut log_size = None;

        // 1) environment variable (optional field, ignore errors)
        if let Ok(log_size_str) = env::var(FLV_LOG_SIZE) {
            log_size = Some(log_size_str);
        }

        // 2) config file
        if log_size.is_none() && file_cfg.is_some() {
            log_size = file_cfg.as_ref().unwrap().log_size();
        }

        // 3) unwrap or use default
        Ok(log_size.unwrap_or(SPU_LOG_SIZE.to_owned()))
    }

    /// Generate log index_max_bytes by combining all config elements. Returns error on failure.
    fn make_log_index_max_bytes(file_cfg: &Option<SpuConfigFile>) -> Result<u32, IoError> {
        let mut log_index_max_bytes = None;

        // 1) environment variable (optional field, ignore errors)
        if let Ok(log_index_max_bytes_str) = env::var(FLV_LOG_INDEX_MAX_BYTES) {
            let index_max_bytes: u32 = log_index_max_bytes_str.parse().map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidInput,
                    format!("log index-max-bytes: {}", err),
                )
            })?;
            log_index_max_bytes = Some(index_max_bytes);
        }

        // 2) config file
        if log_index_max_bytes.is_none() && file_cfg.is_some() {
            log_index_max_bytes = file_cfg.as_ref().unwrap().log_index_max_bytes();
        }

        // 3) unwrap or use default
        Ok(log_index_max_bytes.unwrap_or(SPU_LOG_INDEX_MAX_BYTES))
    }

    /// Generate log index_max_interval_bytes by combining all config elements. Returns error on failure.
    fn make_log_index_max_interval_bytes(file_cfg: &Option<SpuConfigFile>) -> Result<u32, IoError> {
        let mut log_index_max_interval_bytes = None;

        // 1) environment variable (optional field, ignore errors)
        if let Ok(log_index_max_interval_bytes_str) = env::var(FLV_LOG_INDEX_MAX_INTERVAL_BYTES) {
            let index_max_interval_bytes: u32 =
                log_index_max_interval_bytes_str.parse().map_err(|err| {
                    IoError::new(
                        ErrorKind::InvalidInput,
                        format!("log index-max-interval-bytes: {}", err),
                    )
                })?;
            log_index_max_interval_bytes = Some(index_max_interval_bytes);
        }

        // 2) config file
        if log_index_max_interval_bytes.is_none() && file_cfg.is_some() {
            log_index_max_interval_bytes =
                file_cfg.as_ref().unwrap().log_index_max_interval_bytes();
        }

        // 3) unwrap or use default
        Ok(log_index_max_interval_bytes.unwrap_or(SPU_LOG_INDEX_MAX_INTERVAL_BYTES))
    }

    /// Generate log segment_max_bytes by combining all config elements. Returns error on failure.
    fn make_log_segment_max_bytes(file_cfg: &Option<SpuConfigFile>) -> Result<u32, IoError> {
        let mut log_segment_max_bytes = None;

        // 1) environment variable (optional field, ignore errors)
        if let Ok(log_segment_max_bytes_str) = env::var(FLV_LOG_SEGMENT_MAX_BYTES) {
            let segment_max_bytes: u32 = log_segment_max_bytes_str.parse().map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidInput,
                    format!("log segment_max_bytes: {}", err),
                )
            })?;
            log_segment_max_bytes = Some(segment_max_bytes);
        }

        // 2) config file
        if log_segment_max_bytes.is_none() && file_cfg.is_some() {
            log_segment_max_bytes = file_cfg.as_ref().unwrap().log_segment_max_bytes();
        }

        // 3) unwrap or use default
        Ok(log_segment_max_bytes.unwrap_or(SPU_LOG_SEGMENT_MAX_BYTES))
    }

    pub fn id(&self) -> SpuId {
        self.id
    }

    pub fn rack(&self) -> &Option<String> {
        &self.rack
    }

    pub fn is_custom(&self) -> bool {
        match self.spu_type {
            SpuType::Custom => true,
            SpuType::Managed => false,
        }
    }

    pub fn type_label(&self) -> String {
        match self.spu_type {
            SpuType::Custom => "custom".to_string(),
            SpuType::Managed => "managed".to_string(),
        }
    }

    pub fn sc_endpoint(&self) -> &ServerAddress {
        &self.sc_endpoint
    }

    pub fn public_socket_addr(&self) -> &SocketAddr {
        &self.public_endpoint.socket_addr
    }

    pub fn public_server_addr(&self) -> &ServerAddress {
        &self.public_endpoint.server_addr
    }

    pub fn private_socket_addr(&self) -> &SocketAddr {
        &self.private_endpoint.socket_addr
    }

    pub fn private_server_addr(&self) -> &ServerAddress {
        &self.public_endpoint.server_addr
    }


    pub fn storage(&self) -> &Log {
        &self.log
    }


}

impl Endpoint {
    pub fn new(server_addr: &ServerAddress) -> Result<Self, IoError> {
        let socket_addr = server_to_socket_addr(server_addr)?;
        let server_addr = server_addr.clone();
        Ok(Endpoint {
            server_addr,
            socket_addr,
        })
    }
}

/// Convert Server string to Server Address
fn server_str_to_server_addr(
    server_str: &Option<String>,
) -> Result<Option<ServerAddress>, IoError> {
    if let Some(server) = server_str {
        // parse host and port
        let host_port: Vec<&str> = server.split(':').collect();
        if host_port.len() != 2 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Expected 'host:port' format, found '{}'", server),
            ));
        }

        let host = host_port[0].to_owned();
        let port: u16 = host_port[1].parse().map_err(|err| {
            IoError::new(ErrorKind::InvalidInput, format!("invalid port: {}", err))
        })?;

        Ok(Some(ServerAddress { host, port }))
    } else {
        Ok(None)
    }
}

// ---------------------------------------
// Unit Tests
// ---------------------------------------
#[cfg(test)]
pub mod test {
   
    use super::*;
    use types::defaults::SPU_DEFAULT_ID;

    #[test]
    fn test_get_spu_config_with_secret() {
        let spu_opt = SpuOpt::default();
        let spu_config_file = None;

        // test read & parse
        let result = SpuConfig::new_from_all(spu_opt, spu_config_file);
        assert!(result.is_ok());

        // setup endpoints
        let public_endpoint_res = Endpoint::new(&ServerAddress {
            host: SPU_PUBLIC_HOSTNAME.to_owned(),
            port: SPU_PUBLIC_PORT,
        });
        assert!(public_endpoint_res.is_ok());

        let private_endpoint_res = Endpoint::new(&ServerAddress {
            host: SPU_PRIVATE_HOSTNAME.to_owned(),
            port: SPU_PRIVATE_PORT,
        });
        assert!(private_endpoint_res.is_ok());

        let sc_endpoint_res = ServerAddress {
            host: SC_HOSTNAME.to_owned(),
            port: SC_PRIVATE_PORT,
        };

        // compare with expected result
        let expected = SpuConfig {
            id: SPU_DEFAULT_ID,
            spu_type: SpuType::Custom,
            rack: None,
            public_endpoint: public_endpoint_res.unwrap(),
            private_endpoint: private_endpoint_res.unwrap(),
            sc_endpoint: sc_endpoint_res,
            sc_retry_ms: SPU_RETRY_SC_TIMEOUT_MS,
            replication: Replication {
                min_in_sync_replicas: SPU_MIN_IN_SYNC_REPLICAS,
            },
            log: Log {
                base_dir: PathBuf::from(SPU_LOG_BASE_DIR),
                size: SPU_LOG_SIZE.to_owned(),
                index_max_bytes: SPU_LOG_INDEX_MAX_BYTES,
                index_max_interval_bytes: SPU_LOG_INDEX_MAX_INTERVAL_BYTES,
                segment_max_bytes: SPU_LOG_SEGMENT_MAX_BYTES,
            }
        };

        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_get_spu_config_from_named_config_file() {
        let spu_opt = SpuOpt::default();
       
        let file = PathBuf::from("./test-data/config/spu_server.toml");
        let spu_config_file = SpuConfigFile::from_file(&file);
        assert!(spu_config_file.is_ok());

        // test read & parse
        let result = SpuConfig::new_from_all(spu_opt, Some(spu_config_file.unwrap()));
        assert!(result.is_ok());

        // setup endpoints
        let public_endpoint_res = Endpoint::new(&ServerAddress {
            host: "127.0.0.1".to_owned(),
            port: 5555,
        });
        assert!(public_endpoint_res.is_ok());

        let private_endpoint_res = Endpoint::new(&ServerAddress {
            host: "127.0.0.1".to_owned(),
            port: 5556,
        });
        assert!(private_endpoint_res.is_ok());

        let sc_endpoint_res = ServerAddress {
            host: "127.0.0.1".to_owned(),
            port: 5554,
        };

        // compare with expected result
        let expected = SpuConfig {
            id: 5050,
            spu_type: SpuType::Custom,
            rack: Some("rack-1".to_owned()),
            public_endpoint: public_endpoint_res.unwrap(),
            private_endpoint: private_endpoint_res.unwrap(),
            sc_endpoint: sc_endpoint_res,
            sc_retry_ms: 2000,
            replication: Replication {
                min_in_sync_replicas: 3,
            },
            log: Log {
                base_dir: PathBuf::from("/tmp/data_streams"),
                size: "2Gi".to_owned(),
                index_max_bytes: 888888,
                index_max_interval_bytes: 2222,
                segment_max_bytes: 9999999,
            }
        };

        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_get_spu_config_overwite_config_file() {
        flv_util::init_logger();

        let spu_opt = SpuOpt {
            id: Some(9898),
            public_server: Some("1.1.1.1:8888".to_owned()),
            private_server: Some("2.2.2.2:9999".to_owned()),
            sc_server: Some("3.3.3.3:5555".to_owned()),
            config_file: None,
        };

        let file = PathBuf::from("./test-data/config/spu_server.toml");
        let spu_config_file = SpuConfigFile::from_file(&file);
        assert!(spu_config_file.is_ok());

        // test read & parse
        let result = SpuConfig::new_from_all(spu_opt, Some(spu_config_file.unwrap()));
        assert!(result.is_ok());

        // setup endpoints
        let public_endpoint_res = Endpoint::new(&ServerAddress {
            host: "1.1.1.1".to_owned(),
            port: 8888,
        });
        assert!(public_endpoint_res.is_ok());

        let private_endpoint_res = Endpoint::new(&ServerAddress {
            host: "2.2.2.2".to_owned(),
            port: 9999,
        });
        assert!(private_endpoint_res.is_ok());

        let sc_endpoint_res = ServerAddress {
            host: "3.3.3.3".to_owned(),
            port: 5555,
        };
     
        // compare with expected result
        let expected = SpuConfig {
            id: 9898,
            spu_type: SpuType::Custom,
            rack: Some("rack-1".to_owned()),
            public_endpoint: public_endpoint_res.unwrap(),
            private_endpoint: private_endpoint_res.unwrap(),
            sc_endpoint: sc_endpoint_res,
            sc_retry_ms: 2000,
            replication: Replication {
                min_in_sync_replicas: 3,
            },
            log: Log {
                base_dir: PathBuf::from("/tmp/data_streams"),
                size: "2Gi".to_owned(),
                index_max_bytes: 888888,
                index_max_interval_bytes: 2222,
                segment_max_bytes: 9999999,
            }
        };

        assert_eq!(result.unwrap(), expected);
    }
}
