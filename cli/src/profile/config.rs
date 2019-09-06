//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::Path;

use types::socket_helpers::string_to_socket_addr;

use crate::CliError;

use super::profile_file::build_cli_profile_file_path;
use super::profile_file::ProfileFile;

// -----------------------------------
// Data Structures
// -----------------------------------

/// Profile parameters
#[derive(Default, Debug, PartialEq)]
pub struct ProfileConfig {
    pub sc_addr: Option<SocketAddr>,
    pub spu_addr: Option<SocketAddr>,
    pub kf_addr: Option<SocketAddr>,
}

/// Target Server
#[derive(Debug)]
pub enum TargetServer {
    Sc(SocketAddr),
    Spu(SocketAddr),
    Kf(SocketAddr),
}

// -----------------------------------
// Implementation
// -----------------------------------

impl ProfileConfig {
    /// generate profile configuration based on a default or custom profile file
    pub fn new(
        sc_host_port: &Option<String>,
        kf_host_port: &Option<String>,
        profile_name: &Option<String>,
    ) -> Result<Self, CliError> {
        ProfileConfig::new_with_spu(sc_host_port, &None, kf_host_port, profile_name)
    }

    /// generate profile configuration with spu based on a default or custom profile file
    pub fn new_with_spu(
        sc_host_port: &Option<String>,
        spu_host_port: &Option<String>,
        kf_host_port: &Option<String>,
        profile_name: &Option<String>,
    ) -> Result<Self, CliError> {
        // build profile config from cli parameters
        let cli_config =
            ProfileConfig::config_from_cli_params(&sc_host_port, &spu_host_port, &kf_host_port)?;

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

        Ok(profile_config)
    }

    /// retrieve target server
    pub fn target_server(&self) -> Result<TargetServer, CliError> {

        if let Some(sc_server) = self.sc_addr {
            Ok(TargetServer::Sc(sc_server.clone()))
        } else if let Some(spu_server) = self.spu_addr {
            Ok(TargetServer::Spu(spu_server.clone()))
        } else if let Some(kf_server) = self.kf_addr {
            Ok(TargetServer::Kf(kf_server.clone()))
        } else {
            Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                "target server configuration missing",
            )))
        }
    }

    /// ensure there is at least one server.
    pub fn valid_servers_or_error(&self) -> Result<(), CliError> {
        if self.sc_addr.is_some() || self.spu_addr.is_some() || self.kf_addr.is_some() {
            Ok(())
        } else {
            Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                "no sc address or spu address is provided",
            )))
        }
    }

    /// merge local profile with the other profile
    ///  - values are augmented but not cleared by other
    fn merge_with(&mut self, other: &ProfileConfig) {
        if other.sc_addr.is_some() {
            self.sc_addr = other.sc_addr;
        }
        if other.spu_addr.is_some() {
            self.spu_addr = other.spu_addr;
        }
        if other.kf_addr.is_some() {
            self.kf_addr = other.kf_addr;
        }
    }

    /// read profile config from a user-defined (custom) profile
    fn config_from_custom_profile(profile: &String) -> Result<Self, IoError> {
        let custom_profile_path = build_cli_profile_file_path(Some(&profile))?;
        (ProfileFile::from_file(custom_profile_path)?).to_config()
    }

    /// read profile config from the default profile
    fn config_from_default_profile() -> Result<Self, IoError> {
        let default_path = build_cli_profile_file_path(None)?;
        if Path::new(&default_path).exists() {
            (ProfileFile::from_file(default_path)?).to_config()
        } else {
            Ok(ProfileConfig::default())
        }
    }

    /// generate fluvio configuration from cli parameters
    fn config_from_cli_params(
        sc_host_port: &Option<String>,
        spu_host_port: &Option<String>,
        kf_host_port: &Option<String>,
    ) -> Result<Self, IoError> {
        let mut profile_config = ProfileConfig::default();

        if let Some(host_port) = sc_host_port {
            profile_config.sc_addr = Some(ProfileConfig::host_port_to_socket_addr(&host_port)?);
        }

        if let Some(host_port) = spu_host_port {
            profile_config.spu_addr = Some(ProfileConfig::host_port_to_socket_addr(&host_port)?);
        }

        if let Some(host_port) = kf_host_port {
            profile_config.kf_addr = Some(ProfileConfig::host_port_to_socket_addr(&host_port)?);
        }

        Ok(profile_config)
    }

    /// parse host/prot to socket address
    pub fn host_port_to_socket_addr(host_port: &String) -> Result<SocketAddr, IoError> {
        string_to_socket_addr(host_port)
    }
}
