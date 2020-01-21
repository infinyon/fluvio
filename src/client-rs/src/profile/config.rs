//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;
use std::convert::TryInto;

use types::socket_helpers::ServerAddress;

use crate::ClientConfig;
use crate::ScClient;
use crate::KfClient;
use crate::SpuController;
use crate::SpuLeader;
use crate::LeaderConfig;
use crate::KfLeader;
use crate::ClientError;


use super::profile_file::build_cli_profile_file_path;
use super::profile_file::ProfileFile;

pub type CliClientConfig = ClientConfig<String>;

const CLIENT_ID: &'static str = "fluvio_cli";

fn addr_client_config(addr: ServerAddress) -> CliClientConfig {
    ClientConfig::new(addr.to_string()).client_id(CLIENT_ID)
}

pub enum ReplicaLeaderTarget {
    Spu(SpuLeader),
    Kf(KfLeader),
}

#[derive(Debug)]
pub enum ReplicaLeaderConfig {
    Sc(ServerAddress),
    Spu(ServerAddress),
    Kf(ServerAddress),
}

impl ReplicaLeaderConfig {
    pub fn new(
        sc_host_port: Option<String>,
        spu_host_port: Option<String>,
        kf_host_port: Option<String>,
        profile_name: Option<String>,
    ) -> Result<Self, ClientError> {
        let profile =
            ProfileConfig::new_with_spu(sc_host_port, spu_host_port, kf_host_port, profile_name)?;

        if let Some(sc_server) = profile.sc_addr {
            Ok(Self::Sc(sc_server))
        } else if let Some(spu_server) = profile.spu_addr {
            Ok(Self::Spu(spu_server))
        } else if let Some(kf_server) = profile.kf_addr {
            Ok(Self::Kf(kf_server))
        } else {
            Err(ClientError::IoError(IoError::new(
                ErrorKind::Other,
                "replica server configuration missing",
            )))
        }
    }

    pub async fn connect(
        self,
        topic: &str,
        partition: i32,
    ) -> Result<ReplicaLeaderTarget, ClientError> {
        match self {
            Self::Kf(addr) => {
                let mut kf_client = KfClient::connect(addr_client_config(addr)).await?;
                kf_client
                    .find_leader_for_topic_partition(topic, partition)
                    .await
                    .map(|leader| ReplicaLeaderTarget::Kf(leader))
            }
            Self::Sc(addr) => {
                let mut sc_client = ScClient::connect(addr_client_config(addr)).await?;
                sc_client
                    .find_leader_for_topic_partition(topic, partition)
                    .await
                    .map(|leader| ReplicaLeaderTarget::Spu(leader))
            }
            Self::Spu(addr) => {
                let leader_config =
                    LeaderConfig::new(addr, topic.to_owned(), partition).client_id(CLIENT_ID);
                SpuLeader::connect(leader_config)
                    .await
                    .map(|leader| ReplicaLeaderTarget::Spu(leader))
            }
        }
    }
}

pub enum SpuControllerTarget {
    Sc(ScClient<String>),
    Kf(KfClient<String>),
}

#[derive(Debug)]
pub enum SpuControllerConfig {
    Sc(ServerAddress),
    Kf(ServerAddress),
}

impl SpuControllerConfig {
    pub fn new(
        sc_host_port: Option<String>,
        kf_host_port: Option<String>,
        profile_name: Option<String>,
    ) -> Result<Self, ClientError> {
        let profile = ProfileConfig::new(sc_host_port, kf_host_port, profile_name)?;

        if let Some(sc_server) = profile.sc_addr {
            Ok(Self::Sc(sc_server))
        } else if let Some(kf_server) = profile.kf_addr {
            Ok(Self::Kf(kf_server))
        } else {
            Err(ClientError::IoError(IoError::new(
                ErrorKind::Other,
                "controller server configuration missing",
            )))
        }
    }

    pub async fn connect(self) -> Result<SpuControllerTarget, ClientError> {
        match self {
            Self::Kf(addr) => KfClient::connect(addr_client_config(addr))
                .await
                .map(|leader| SpuControllerTarget::Kf(leader)),
            Self::Sc(addr) => ScClient::connect(addr_client_config(addr))
                .await
                .map(|leader| SpuControllerTarget::Sc(leader)),
        }
    }
}

/// Configure Sc server using either manual config or profile
pub struct ScConfig(ServerAddress);

impl ScConfig {
    pub fn new(host_port: Option<String>, profile_name: Option<String>) -> Result<Self, ClientError> {
        let profile = ProfileConfig::new(host_port, None, profile_name)?;

        if let Some(sc_addr) = profile.sc_addr {
            Ok(Self(sc_addr))
        } else {
            Err(ClientError::IoError(IoError::new(
                ErrorKind::Other,
                "Sc server configuration missing",
            )))
        }
    }

    pub async fn connect(self) -> Result<ScClient<String>, ClientError> {
        ScClient::connect(addr_client_config(self.0)).await
    }
}

/// Configure Kafka using either manual address or profile
pub struct KfConfig(ServerAddress);

impl KfConfig {
    pub fn new(host_port: Option<String>, profile_name: Option<String>) -> Result<Self, ClientError> {
        let profile = ProfileConfig::new(None, host_port, profile_name)?;

        if let Some(kf_addr) = profile.kf_addr {
            Ok(Self(kf_addr))
        } else {
            Err(ClientError::IoError(IoError::new(
                ErrorKind::Other,
                "Kf server configuration missing",
            )))
        }
    }

    pub async fn connect(self) -> Result<KfClient<String>, ClientError> {
        KfClient::connect(addr_client_config(self.0)).await
    }
}

/// Profile parameters
#[derive(Default, Debug, PartialEq)]
pub struct ProfileConfig {
    pub sc_addr: Option<ServerAddress>,
    pub spu_addr: Option<ServerAddress>,
    pub kf_addr: Option<ServerAddress>,
}

// -----------------------------------
// Implementation
// -----------------------------------

impl ProfileConfig {
    /// generate config from cli parameter where we could have one of the config
    pub fn from_cli(
        sc_host_port: Option<String>,
        spu_host_port: Option<String>,
        kf_host_port: Option<String>,
    ) -> Result<Self, ClientError> {
        let mut config = ProfileConfig::default();

        if let Some(host_port) = sc_host_port {
            let address: ServerAddress = host_port.try_into()?;
            config.sc_addr = Some(address);
        }

        if let Some(host_port) = spu_host_port {
            let address: ServerAddress = host_port.try_into()?;
            config.spu_addr = Some(address);
        }

        if let Some(host_port) = kf_host_port {
            let address: ServerAddress = host_port.try_into()?;
            config.kf_addr = Some(address);
        }

        config.valid_servers_or_error()?;

        Ok(config)
    }

    /// generate profile configuration based on a default or custom profile file
    pub fn new(
        sc_host_port: Option<String>,
        kf_host_port: Option<String>,
        profile_name: Option<String>,
    ) -> Result<Self, ClientError> {
        ProfileConfig::new_with_spu(sc_host_port, None, kf_host_port, profile_name)
    }

    /// generate profile configuration with spu based on a default or custom profile file
    pub fn new_with_spu(
        sc_host_port: Option<String>,
        spu_host_port: Option<String>,
        kf_host_port: Option<String>,
        profile_name: Option<String>,
    ) -> Result<Self, ClientError> {
        // build profile config from cli parameters
        let cli_config = Self::from_cli(sc_host_port, spu_host_port, kf_host_port)?;

        // if server is configured from cli, do not load profile (as it impacts precedence)
        let profile_config = if cli_config.valid_servers_or_error().is_ok() {
            cli_config
        } else {
            // build profile config from profile file
            let mut file_config = match profile_name {
                Some(profile) => ProfileConfig::config_from_custom_profile(profile)?,
                None => ProfileConfig::config_from_default_profile()?,
            };

            // merge the profiles (cli takes precedence)
            file_config.merge_with(&cli_config);
            file_config
        };

        profile_config.valid_servers_or_error()?;

        Ok(profile_config)
    }

    /// convert my self into target server

    /// ensure there is at least one server.
    fn valid_servers_or_error(&self) -> Result<(), ClientError> {
        if self.sc_addr.is_some() || self.spu_addr.is_some() || self.kf_addr.is_some() {
            Ok(())
        } else {
            Err(ClientError::IoError(IoError::new(
                ErrorKind::Other,
                "no sc address or spu address is provided",
            )))
        }
    }

    /// merge local profile with the other profile
    ///  - values are augmented but not cleared by other
    fn merge_with(&mut self, other: &ProfileConfig) {
        if other.sc_addr.is_some() {
            self.sc_addr = other.sc_addr.clone();
        }
        if other.spu_addr.is_some() {
            self.spu_addr = other.spu_addr.clone();
        }
        if other.kf_addr.is_some() {
            self.kf_addr = other.kf_addr.clone();
        }
    }

    /// read profile config from a user-defined (custom) profile
    fn config_from_custom_profile(profile: String) -> Result<Self, IoError> {
        let custom_profile_path = build_cli_profile_file_path(Some(&profile))?;
        Ok((ProfileFile::from_file(custom_profile_path)?).into())
    }

    /// read profile config from the default profile
    fn config_from_default_profile() -> Result<Self, IoError> {
        let default_path = build_cli_profile_file_path(None)?;
        if Path::new(&default_path).exists() {
            Ok((ProfileFile::from_file(default_path)?).into())
        } else {
            Ok(ProfileConfig::default())
        }
    }
}
