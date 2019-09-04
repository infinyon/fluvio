//!
//! # Profiles File Data Structure
//!
//! Profile file retrieves configurations from profile file into memory.
//!
use std::env;
use std::fs::read_to_string;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use dirs::home_dir;
use serde::Deserialize;

use types::defaults::{CLI_CONFIG_PATH, CLI_DEFAULT_PROFILE, CLI_PROFILES_DIR};
use types::defaults::{CONFIG_FILE_EXTENTION, FLV_FLUVIO_HOME};

use super::config::ProfileConfig;

#[derive(Debug, PartialEq, Deserialize)]
pub struct ProfileFile {
    pub version: String,
    sc: Option<TargetScGroup>,
    spu: Option<TargetSpuGroup>,
    kf: Option<TargetKfGroup>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct TargetScGroup {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, PartialEq, Deserialize)]
struct TargetSpuGroup {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, PartialEq, Deserialize)]
struct TargetKfGroup {
    pub host: String,
    pub port: u16,
}

// ---------------------------------------
// Implementation
// ---------------------------------------

impl ProfileFile {
    // read and parse the .toml file
    pub fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, IoError> {
        let file_str: String = read_to_string(path)?;
        toml::from_str(&file_str)
            .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))
    }

    // converts profile file into a profile configuation
    pub fn to_config(&self) -> Result<ProfileConfig, IoError> {
        let sc_addr = if let Some(ref sc) = &self.sc {
            Some(
                format!("{}:{}", sc.host, sc.port)
                    .parse::<SocketAddr>()
                    .map_err(|err| {
                        IoError::new(
                            ErrorKind::InvalidInput,
                            format!("invalid streaming controller {}", err),
                        )
                    })?,
            )
        } else {
            None
        };

        let spu_addr = if let Some(ref spu) = &self.spu {
            Some(
                format!("{}:{}", spu.host, spu.port)
                    .parse::<SocketAddr>()
                    .map_err(|err| {
                        IoError::new(
                            ErrorKind::InvalidInput,
                            format!("invalid target_spu {}", err),
                        )
                    })?,
            )
        } else {
            None
        };

        let kf_addr = if let Some(ref kf) = &self.kf {
            Some(
                format!("{}:{}", kf.host, kf.port)
                    .parse::<SocketAddr>()
                    .map_err(|err| {
                        IoError::new(
                            ErrorKind::InvalidInput,
                            format!("invalid target_kf {}", err),
                        )
                    })?,
            )
        } else {
            None
        };

        Ok(ProfileConfig {
            sc_addr,
            spu_addr,
            kf_addr,
        })
    }
}

/// Look-up profile file for Fluvio CLI based on profile name
///  
/// * Step1:
/// set base path:
///     1) use $FLUVIO_HOME environment variable if set
///     2) else ~/.fluvio
///
/// * Step 2:
/// get configuration based on profile (default for none):
///     default     => <base-path>/profiles/default.toml
///     profile1    => <base-path>/profiles/profile1.toml
///
pub fn build_cli_profile_file_path(profile_name: Option<&String>) -> Result<PathBuf, IoError> {
    // set base path
    let base_path = match env::var(FLV_FLUVIO_HOME) {
        Ok(val) => {
            // FLUVIO_HOME env variable is set
            let mut user_dir = PathBuf::new();
            user_dir.push(val);
            user_dir
        }
        Err(_) => {
            // use HOME directory
            if let Some(mut home_dir) = home_dir() {
                home_dir.push(CLI_CONFIG_PATH);
                home_dir
            } else {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    "can't get home directory",
                ));
            }
        }
    };

    // augment profiles path
    let mut file_path = base_path.join(CLI_PROFILES_DIR);

    // augment profile name
    if profile_name.is_some() {
        file_path.push(profile_name.unwrap());
    } else {
        file_path.push(CLI_DEFAULT_PROFILE);
    }

    // augment extension
    file_path.set_extension(CONFIG_FILE_EXTENTION);

    Ok(file_path)
}

// ---------------------------------------
// Unit Tests
// ---------------------------------------

#[cfg(test)]
pub mod test {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_default_profile_ok() {
        let mut profile_path = PathBuf::new();
        profile_path.push("./test-data/profiles/default.toml");

        // test read & parse
        let result = ProfileFile::from_file(profile_path);
        assert!(result.is_ok());

        // compare with expected result
        let expected = ProfileFile {
            version: "1.0".to_owned(),
            sc: Some(TargetScGroup {
                host: "127.0.0.1".to_owned(),
                port: 9033,
            }),
            spu: Some(TargetSpuGroup {
                host: "127.0.0.1".to_owned(),
                port: 9034,
            }),
            kf: Some(TargetKfGroup {
                host: "127.0.0.1".to_owned(),
                port: 9093,
            }),
        };

        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_default_profile_not_found() {
        let mut profile_path = PathBuf::new();
        profile_path.push("./test-data/profiles/notfound.toml");

        // run test
        let result = ProfileFile::from_file(profile_path);

        // expecting error
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "No such file or directory (os error 2)"
        );
    }

    #[test]
    fn test_invalid_profile_file() {
        let mut profile_path = PathBuf::new();
        profile_path.push("./test-data/profiles/invalid.toml");

        // run test
        let result = ProfileFile::from_file(profile_path);

        // expecting error
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "missing field `port` for key `sc`"
        );
    }

    #[test]
    fn test_build_default_profile_file_path() {
        let file_path = build_cli_profile_file_path(None);
        assert_eq!(file_path.is_ok(), true);

        let mut expected_file_path = PathBuf::new();
        expected_file_path.push(home_dir().unwrap());
        expected_file_path.push(".fluvio/profiles/default.toml");

        assert_eq!(file_path.unwrap(), expected_file_path);
    }

    #[test]
    fn test_build_custom_cli_profile_file_path() {
        let file_path = build_cli_profile_file_path(Some(&"custom".to_owned()));
        assert_eq!(file_path.is_ok(), true);

        let mut expected_file_path = PathBuf::new();
        expected_file_path.push(home_dir().unwrap());
        expected_file_path.push(".fluvio/profiles/custom.toml");

        assert_eq!(file_path.unwrap(), expected_file_path);
    }

}
