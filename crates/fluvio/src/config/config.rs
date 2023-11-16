//!
//! # Main Configuration file
//!
//! Contains contexts, profiles
//!
use std::env;
use std::fs::read_to_string;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs::File;
use std::fs::create_dir_all;

use thiserror::Error;

use tracing::debug;
#[cfg(not(target_arch = "wasm32"))]
use dirs::home_dir;

#[cfg(target_arch = "wasm32")]
fn home_dir() -> Option<PathBuf> {
    None
}

use serde::Deserialize;
use serde::Serialize;

use fluvio_types::defaults::CLI_CONFIG_PATH;
use crate::{FluvioConfig, FluvioError};

use super::TlsPolicy;

fn config_file_error(msg: &str, source: IoError) -> ConfigError {
    ConfigError::ConfigFileError {
        msg: msg.to_owned(),
        source,
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("config file {msg}")]
    ConfigFileError { msg: String, source: IoError },
    #[error("Failed to deserialize Fluvio config {msg}")]
    TomlError {
        msg: String,
        source: toml::de::Error,
    },
    #[error("Config has no active profile")]
    NoActiveProfile,
    #[error("No cluster config for profile {profile}")]
    NoClusterForProfile { profile: String },
}

pub struct ConfigFile {
    path: PathBuf,
    config: Config,
}

impl ConfigFile {
    fn new(path: PathBuf, config: Config) -> Self {
        Self { path, config }
    }

    /// create default profile
    pub fn default_config() -> Result<Self, IoError> {
        let path = Self::default_file_path()?;
        Ok(Self {
            path,
            config: Config::new(),
        })
    }

    /// load from default location if not found, create new one
    pub fn load_default_or_new() -> Result<Self, IoError> {
        match Self::load(None) {
            Ok(config_file) => Ok(config_file),
            Err(err) => {
                // if doesn't exist, we create new profile
                debug!("profile can't be loaded, creating new one: {}", err);
                ConfigFile::default_config()
            }
        }
    }

    /// try to load from default locations
    pub fn load(optional_path: Option<String>) -> Result<Self, FluvioError> {
        Self::from_file(match optional_path {
            Some(p) => PathBuf::from(p),
            None => Self::default_file_path().map_err(|e| config_file_error("default path", e))?,
        })
    }

    /// read from file
    fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, FluvioError> {
        let path_ref = path.as_ref();
        debug!(?path_ref, "loading from");
        let file_str: String = read_to_string(path_ref)
            .map_err(|e| config_file_error(&format!("{:?}", path_ref.as_os_str()), e))?;
        let config = toml::from_str(&file_str).map_err(|e| ConfigError::TomlError {
            msg: path_ref.display().to_string(),
            source: e,
        })?;
        Ok(Self::new(path_ref.to_owned(), config))
    }

    /// find default path where config is stored.  precedent is:
    /// 1) supplied path
    /// 2) environment variable in FLV_PROFILE_PATH
    /// 3) home directory ~/.fluvio/config
    fn default_file_path() -> Result<PathBuf, IoError> {
        env::var("FLV_PROFILE_PATH")
            .map(|p| Ok(PathBuf::from(p)))
            .unwrap_or_else(|_| {
                if let Some(mut profile_path) = home_dir() {
                    profile_path.push(CLI_CONFIG_PATH);
                    profile_path.push("config");
                    Ok(profile_path)
                } else {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "can't get profile directory",
                    ))
                }
            })
    }

    /// Return a reference to the internal Config
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Return a mutable reference to the internal Config
    pub fn mut_config(&mut self) -> &mut Config {
        &mut self.config
    }

    /// Save to file
    pub fn save(&self) -> Result<(), FluvioError> {
        create_dir_all(self.path.parent().unwrap())
            .map_err(|e| config_file_error(&format!("parent {:?}", self.path), e))?;
        self.config
            .save_to(&self.path)
            .map_err(|e| config_file_error(&format!("{:?}", &self.path), e))?;
        Ok(())
    }

    /// add or update profile with a simple cluster address
    /// this will create or replace cluster config if it doesn't exists
    pub fn add_or_replace_profile(
        &mut self,
        profile_name: &str,
        cluster_addr: &str,
        tls_policy: &TlsPolicy,
    ) -> Result<(), FluvioError> {
        let config = self.mut_config();

        // if cluster exists, just update extern addr
        // if not create new config
        match config.cluster_mut(profile_name) {
            Some(cluster) => {
                cluster.endpoint = cluster_addr.to_string();
                cluster.tls = tls_policy.clone();
            }
            None => {
                let mut new_cluster = FluvioConfig::new(cluster_addr);
                new_cluster.tls = tls_policy.clone();
                config.add_cluster(new_cluster, profile_name.to_string());
            }
        }

        // add profile or exist
        match config.profile_mut(profile_name) {
            Some(profile) => {
                profile.set_cluster(profile_name.to_string());
            }
            None => {
                let profile = Profile::new(profile_name.to_string());
                config.add_profile(profile, profile_name.to_string());
            }
        };

        config.set_current_profile(profile_name);
        self.save()?;
        Ok(())
    }
}

pub const LOCAL_PROFILE: &str = "local";
const CONFIG_VERSION: &str = "2.0";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    version: String,
    current_profile: Option<String>,
    pub profile: HashMap<String, Profile>,
    pub cluster: HashMap<String, FluvioConfig>,
    client_id: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        Self {
            version: CONFIG_VERSION.to_owned(),
            ..Default::default()
        }
    }

    /// create new config with a single local cluster
    pub fn new_with_local_cluster(domain: String) -> Self {
        let cluster = FluvioConfig::new(domain);
        let mut config = Self::new();

        config.cluster.insert(LOCAL_PROFILE.to_owned(), cluster);

        let profile_name = LOCAL_PROFILE.to_owned();
        let local_profile = Profile::new(profile_name.clone());
        config.profile.insert(profile_name.clone(), local_profile);
        config.set_current_profile(&profile_name);
        config
    }

    /// add new cluster
    pub fn add_cluster(&mut self, cluster: FluvioConfig, name: String) {
        self.cluster.insert(name, cluster);
    }

    pub fn add_profile(&mut self, profile: Profile, name: String) {
        self.profile.insert(name, profile);
    }

    // save to file
    fn save_to<T: AsRef<Path>>(&self, path: T) -> Result<(), IoError> {
        let path_ref = path.as_ref();
        debug!("saving config: {:#?} to: {:#?}", self, path_ref);
        let toml = toml::to_string(self)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("{err}")))?;

        let mut file = File::create(path_ref)?;
        file.write_all(toml.as_bytes())?;
        // On windows flush() is noop, but sync_all() calls FlushFileBuffers.
        file.sync_all()
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    /// current profile
    pub fn current_profile_name(&self) -> Option<&str> {
        self.current_profile.as_deref()
    }

    /// set current profile, if profile doesn't exists return false
    pub fn set_current_profile(&mut self, profile_name: &str) -> bool {
        if self.profile.contains_key(profile_name) {
            self.current_profile = Some(profile_name.to_owned());
            true
        } else {
            false
        }
    }

    pub fn rename_profile(&mut self, from: &str, to: String) -> bool {
        // Remove the profile from its old name, or return if it didn't exist
        let profile = match self.profile.remove(from) {
            Some(profile) => profile,
            None => return false,
        };

        // Re-add the profile under its new name
        self.add_profile(profile, to.clone());

        // If the renamed profile was current, we need to update the current name
        let update_current = self
            .current_profile_name()
            .map(|it| it == from)
            .unwrap_or(false);
        if update_current {
            self.current_profile = Some(to);
        }

        true
    }

    /// delete profile
    pub fn delete_profile(&mut self, profile_name: &str) -> bool {
        if self.profile.remove(profile_name).is_some() {
            if let Some(old_profile) = &self.current_profile {
                // check if it same as current profile, then remove it
                if profile_name == old_profile {
                    self.current_profile = None;
                }
            }

            true
        } else {
            false
        }
    }

    /// Deletes the named cluster, whether it is being used or not.
    ///
    /// You may want to check if the named cluster is active or not using
    /// `delete_cluster_check`. Otherwise, you may remove a cluster that
    /// is being used by the active profile.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::FluvioConfig;
    /// # use fluvio::config::{Config, Profile};
    /// let mut config = Config::new();
    /// let cluster = FluvioConfig::new("https://cloud.fluvio.io".to_string());
    /// config.add_cluster(cluster, "fluvio-cloud".to_string());
    /// let profile = Profile::new("fluvio-cloud".to_string());
    /// config.add_profile(profile, "fluvio-cloud".to_string());
    ///
    /// config.delete_cluster("fluvio-cloud").unwrap();
    /// assert!(config.cluster("fluvio-cloud").is_none());
    /// ```
    pub fn delete_cluster(&mut self, cluster_name: &str) -> Option<FluvioConfig> {
        self.cluster.remove(cluster_name)
    }

    /// Checks whether it's safe to delete the named cluster
    ///
    /// If there are any profiles that reference the named cluster,
    /// they are considered conflicts and the cluster is unsafe to delete.
    /// When conflicts exist, the conflicting profile names are returned
    /// in the `Err()` return value.
    ///
    /// If there are no profile conflicts, this returns with `Ok(())`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::FluvioConfig;
    /// # use fluvio::config::{Config, Profile};
    /// let mut config = Config::new();
    /// let cluster = FluvioConfig::new("https://cloud.fluvio.io".to_string());
    /// config.add_cluster(cluster, "fluvio-cloud".to_string());
    /// let profile = Profile::new("fluvio-cloud".to_string());
    /// config.add_profile(profile, "fluvio-cloud".to_string());
    ///
    /// let conflicts = config.delete_cluster_check("fluvio-cloud").unwrap_err();
    /// assert_eq!(conflicts, vec!["fluvio-cloud"]);
    /// ```
    pub fn delete_cluster_check(&mut self, cluster_name: &str) -> Result<(), Vec<&str>> {
        // Find all profiles that reference the named cluster
        let conflicts: Vec<_> = self
            .profile
            .iter()
            .filter(|(_, profile)| &*profile.cluster == cluster_name)
            .map(|(name, _)| &**name)
            .collect();

        if !conflicts.is_empty() {
            return Err(conflicts);
        }

        Ok(())
    }

    /// Returns a reference to the current Profile if there is one.
    pub fn current_profile(&self) -> Result<&Profile, FluvioError> {
        let profile = self
            .current_profile
            .as_ref()
            .and_then(|p| self.profile.get(p))
            .ok_or(ConfigError::NoActiveProfile)?;
        Ok(profile)
    }

    /// Returns a reference to the current Profile if there is one.
    pub fn profile(&self, profile_name: &str) -> Option<&Profile> {
        self.profile.get(profile_name)
    }

    /// Returns a mutable reference to the current Profile if there is one.
    pub fn profile_mut(&mut self, profile_name: &str) -> Option<&mut Profile> {
        self.profile.get_mut(profile_name)
    }

    /// Returns the FluvioConfig belonging to the current profile.
    pub fn current_cluster(&self) -> Result<&FluvioConfig, FluvioError> {
        let profile = self.current_profile()?;
        let maybe_cluster = self.cluster.get(&profile.cluster);
        let cluster = maybe_cluster.ok_or_else(|| {
            let profile = profile.cluster.clone();
            ConfigError::NoClusterForProfile { profile }
        })?;
        Ok(cluster)
    }

    /// Returns the FluvioConfig belonging to the named profile.
    pub fn cluster_with_profile(&self, profile_name: &str) -> Option<&FluvioConfig> {
        self.profile
            .get(profile_name)
            .and_then(|profile| self.cluster.get(&profile.cluster))
    }

    /// Returns a reference to the named FluvioConfig.
    pub fn cluster(&self, cluster_name: &str) -> Option<&FluvioConfig> {
        self.cluster.get(cluster_name)
    }

    /// Returns a mutable reference to the named FluvioConfig.
    pub fn cluster_mut(&mut self, cluster_name: &str) -> Option<&mut FluvioConfig> {
        self.cluster.get_mut(cluster_name)
    }

    /// look up replica config
    /// this will iterate and find all configuration that can resolve config
    /// 1) match all config that matches criteria including asterisk
    /// 2) apply in terms of precedent
    pub fn resolve_replica_config(&self, _topic_name: &str, _partition: i32) -> Replica {
        /*
        for (key, val) in self.topic.iter() {
            println!("key: {:#?}, value: {:#?}",key,val);
        }
        */

        Replica::default()
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Topic {
    replica: HashMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    pub cluster: String,
    pub topic: Option<String>,
    pub partition: Option<i32>,
}

impl Profile {
    pub fn new(cluster: String) -> Self {
        Self {
            cluster,
            ..Default::default()
        }
    }

    pub fn set_cluster(&mut self, cluster: String) {
        self.cluster = cluster;
    }
}

#[derive(Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Replica {
    pub max_bytes: Option<i32>,
    pub isolation: Option<String>,
}

#[cfg(test)]
pub mod test {
    use super::*;
    use std::path::PathBuf;
    use std::env::temp_dir;
    use crate::config::{TlsPolicy, TlsConfig, TlsCerts};

    //#[test]
    #[allow(unused)]
    fn test_default_path_env() {
        env::set_var("FLV_PROFILE_PATH", "/user2/config");
        assert_eq!(
            ConfigFile::default_file_path().expect("file"),
            PathBuf::from("/user2/config")
        );
        env::remove_var("FLV_PROFILE_PATH");
    }

    #[test]
    fn test_default_path_home() {
        let mut path = home_dir().expect("home dir must exist");
        path.push(CLI_CONFIG_PATH);
        path.push("config");
        assert_eq!(ConfigFile::default_file_path().expect("file"), path);
    }

    /// test basic reading
    #[test]
    fn test_config() {
        // test read & parse
        let mut conf_file = ConfigFile::load(Some("test-data/profiles/config.toml".to_owned()))
            .expect("failed to parse file");
        let config = conf_file.mut_config();

        assert_eq!(config.version(), "1.0");
        assert_eq!(config.current_profile_name().unwrap(), "local");
        let profile = config.current_profile().expect("profile should exists");
        assert_eq!(profile.cluster, "local");

        assert!(!config.set_current_profile(""));
        assert!(config.set_current_profile("local2"));
        assert_eq!(config.current_profile_name().unwrap(), "local2");

        let cluster = config.current_cluster().expect("cluster should exist");
        assert_eq!(cluster.endpoint, "127.0.0.1:9003");
    }

    #[test]
    fn test_rename_profile() {
        let mut conf_file = ConfigFile::load(Some("test-data/profiles/config.toml".to_owned()))
            .expect("parse failed");

        let config = conf_file.mut_config();
        assert_eq!(config.current_profile_name(), Some("local"));
        config.rename_profile("local", "remote".to_string());
        assert_eq!(config.current_profile_name(), Some("remote"));
        assert!(!config.profile.contains_key("local"));
        assert!(config.profile.contains_key("remote"));
    }

    /// test TOML save generation
    #[test]
    fn test_tls_save() {
        let mut config = Config::new_with_local_cluster("localhost:9003".to_owned());
        let inline_tls_config = TlsConfig::Inline(TlsCerts {
            key: "ABCDEFF".to_owned(),
            cert: "JJJJ".to_owned(),
            ca_cert: "XXXXX".to_owned(),
            domain: "my_domain".to_owned(),
        });

        println!("temp: {:#?}", temp_dir());
        config.cluster_mut(LOCAL_PROFILE).unwrap().tls = inline_tls_config.into();
        config
            .save_to(temp_dir().join("inline.toml"))
            .expect("save should succeed");

        config.cluster_mut(LOCAL_PROFILE).unwrap().tls = TlsPolicy::Disabled;
        config
            .save_to(temp_dir().join("noverf.toml"))
            .expect("save should succeed");
    }

    #[test]
    fn test_set_tls() {
        let mut conf_file = ConfigFile::load(Some("test-data/profiles/config.toml".to_owned()))
            .expect("parse failed");
        let config = conf_file.mut_config();
        let cfg_path = temp_dir().join("test_config.toml");
        config.set_current_profile("local3");
        config
            .save_to(cfg_path.clone())
            .expect("save should succeed");
        let update_conf_file =
            ConfigFile::load(Some(cfg_path.to_string_lossy().to_string())).expect("parse failed");
        assert_eq!(
            update_conf_file.config().current_profile_name().unwrap(),
            "local3"
        );
    }

    #[test]
    fn test_local_cluster() {
        let config = Config::new_with_local_cluster("localhost:9003".to_owned());

        assert_eq!(config.current_profile_name().unwrap(), "local");
        let cluster = config.current_cluster().expect("cluster should exists");
        assert_eq!(cluster.endpoint, "localhost:9003");
    }
}
