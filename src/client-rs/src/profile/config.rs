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

use log::debug;
use dirs::home_dir;
use serde::Deserialize;
use serde::Serialize;

use flv_types::defaults::{CLI_CONFIG_PATH};
use flv_future_aio::net::tls::AllDomainConnector;

use crate::client::*;
use crate::ClientError;
use super::TlsConfig;

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
        let path = Self::default_file_path(None)?;
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
    pub fn load(optional_path: Option<String>) -> Result<Self, ClientError> {
        Self::from_file(Self::default_file_path(optional_path)?)
    }

    /// read from file
    fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, ClientError> {
        let path_ref = path.as_ref();
        let file_str: String = read_to_string(path_ref).map_err(|err| {
            debug!("failed to read profile on path: {:#?}, {} ", path_ref, err);
            ClientError::UnableToReadProfile
        })?;
        let config = toml::from_str(&file_str)
            .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?;
        Ok(Self::new(path_ref.to_owned(), config))
    }

    /// find default path where config is stored.  precedent is:
    /// 1) supplied path
    /// 2) environment variable in FLV_PROFILE_PATH
    /// 3) home directory ~/.fluvio/config
    fn default_file_path(path: Option<String>) -> Result<PathBuf, IoError> {
        path.map(|p| Ok(PathBuf::from(p))).unwrap_or_else(|| {
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
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn mut_config(&mut self) -> &mut Config {
        &mut self.config
    }

    // save to file
    pub fn save(&self) -> Result<(), IoError> {
        create_dir_all(self.path.parent().unwrap())?;
        self.config.save_to(&self.path)
    }
}

pub const LOCAL_PROFILE: &'static str = "local";
const CONFIG_VERSION: &'static str = "2.0";

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Config {
    version: String,
    current_profile: Option<String>,
    profile: HashMap<String, Profile>,
    cluster: HashMap<String, Cluster>,
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
        let cluster = Cluster::new(domain);
        let mut config = Self::new();

        config.cluster.insert(LOCAL_PROFILE.to_owned(), cluster);

        let profile_name = LOCAL_PROFILE.to_owned();
        let local_profile = Profile::new(profile_name.clone());
        config.profile.insert(profile_name.clone(), local_profile);
        config.set_current_profile(&profile_name);
        config
    }

    /// add new cluster
    pub fn add_cluster(&mut self, cluster: Cluster, name: String) {
        self.cluster.insert(name, cluster);
    }

    pub fn add_profile(&mut self, profile: Profile, name: String) {
        self.profile.insert(name, profile);
    }

    // save to file
    fn save_to<T: AsRef<Path>>(&self, path: T) -> Result<(), IoError> {
        let path_ref = path.as_ref();
        debug!("saving config: {:#?} to: {:#?}", self, path_ref);
        let toml =
            toml::to_vec(self).map_err(|err| IoError::new(ErrorKind::Other, format!("{}", err)))?;

        let mut file = File::create(path_ref)?;
        file.write_all(&toml)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    /// current profile
    pub fn current_profile_name(&self) -> Option<&str> {
        self.current_profile.as_ref().map(|c| c.as_ref())
    }

    pub fn current_profile(&self) -> Option<&Profile> {
        self.current_profile
            .as_ref()
            .and_then(|p| self.profile.get(p))
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

    pub fn mut_profile(&mut self, profile_name: &str) -> Option<&mut Profile> {
        self.profile.get_mut(profile_name)
    }

    /// find cluster specified in the profile or current cluster
    pub fn current_cluster_or_with_profile(&self, profile_name: Option<&str>) -> Option<&Cluster> {
        if let Some(profile) = profile_name {
            if let Some(profile_info) = self.profile.get(profile) {
                self.cluster.get(&profile_info.cluster)
            } else {
                None
            }
        } else {
            self.current_cluster()
        }
    }

    /// find current cluster
    pub fn current_cluster(&self) -> Option<&Cluster> {
        self.current_profile()
            .and_then(|profile| self.cluster.get(&profile.cluster))
    }

    pub fn cluster(&self, cluster_name: &str) -> Option<&Cluster> {
        self.cluster.get(cluster_name)
    }

    pub fn mut_cluster(&mut self, cluster_name: &str) -> Option<&mut Cluster> {
        self.cluster.get_mut(cluster_name)
    }

    /// look up replica config
    /// this will iterate and find all configuration that can resolve config
    /// 1) match all config that matches criteria including asterik
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Topic {
    replica: HashMap<String, String>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Replica {
    pub max_bytes: Option<i32>,
    pub isolation: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    pub addr: String,
    pub tls: Option<TlsConfig>,
}

impl From<String> for Cluster {
    fn from(addr: String) -> Self {
        Self {
            addr,
            ..Default::default()
        }
    }
}

impl Cluster {
    /// default non tls local cluster
    pub fn new(addr: String) -> Cluster {
        Self {
            addr,
            ..Default::default()
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn set_addr(&mut self, addr: String) {
        self.addr = addr;
    }
}

impl From<Cluster> for ClientConfig {
    fn from(cluster: Cluster) -> Self {
        ClientConfig::new(cluster.addr, AllDomainConnector::default_tcp())
    }
}

#[cfg(test)]
pub mod test {

    use std::path::PathBuf;
    use std::env::temp_dir;

    use super::*;
    use super::super::TlsClientConfig;

    #[test]
    fn test_default_path_arg() {
        assert_eq!(
            ConfigFile::default_file_path(Some("/user1/test".to_string())).expect("file"),
            PathBuf::from("/user1/test")
        );
    }

    //#[test]
    #[allow(unused)]
    fn test_default_path_env() {
        env::set_var("FLV_PROFILE_PATH", "/user2/config");
        assert_eq!(
            ConfigFile::default_file_path(None).expect("file"),
            PathBuf::from("/user2/config")
        );
        env::remove_var("FLV_PROFILE_PATH");
    }

    #[test]
    fn test_default_path_home() {
        let mut path = home_dir().expect("home dir must exist");
        path.push(CLI_CONFIG_PATH);
        path.push("config");
        assert_eq!(ConfigFile::default_file_path(None).expect("file"), path);
    }

    /// test basic reading
    #[test]
    fn test_config() {
        // test read & parse
        let mut conf_file = ConfigFile::load(Some("test-data/profiles/config.toml".to_owned()))
            .expect("parse failed");
        let config = conf_file.mut_config();

        assert_eq!(config.version(), "1.0");
        assert_eq!(config.current_profile_name().unwrap(), "local");
        let profile = config.current_profile().expect("profile should exists");
        assert_eq!(profile.cluster, "local");

        assert!(!config.set_current_profile("dummy"));
        assert!(config.set_current_profile("local2"));
        assert_eq!(config.current_profile_name().unwrap(), "local2");

        let cluster = config.current_cluster().expect("cluster should exist");
        assert_eq!(cluster.addr, "127.0.0.1:9003");
    }

    /// test TOML save generation
    #[test]
    fn test_tls_save() {
        let mut config = Config::new_with_local_cluster("localhost:9003".to_owned());
        let inline_tls_config = TlsClientConfig {
            client_key: "ABCDEFF".to_owned(),
            client_cert: "JJJJ".to_owned(),
            ca_cert: "XXXXX".to_owned(),
            domain: "my_domain".to_owned(),
        };

        println!("temp: {:#?}", temp_dir());
        config.mut_cluster(LOCAL_PROFILE).unwrap().tls = Some(TlsConfig::Inline(inline_tls_config));
        config
            .save_to(temp_dir().join("inline.toml"))
            .expect("save should succeed");

        config.mut_cluster(LOCAL_PROFILE).unwrap().tls = Some(TlsConfig::NoVerification);
        config
            .save_to(temp_dir().join("noverf.toml"))
            .expect("save should succeed");

        let file_tls_config = TlsClientConfig {
            client_key: "/tmp/client.key".to_owned(),
            client_cert: "/tmp/client.cert".to_owned(),
            ca_cert: "/tmp/ca.cert".to_owned(),
            domain: "my_domain".to_owned(),
        };

        config.mut_cluster(LOCAL_PROFILE).unwrap().tls = Some(TlsConfig::File(file_tls_config));
        config
            .save_to(temp_dir().join("file.toml"))
            .expect("save should succeed");
    }

    #[test]
    fn test_set_tls() {
        let mut conf_file = ConfigFile::load(Some("test-data/profiles/config.toml".to_owned()))
            .expect("parse failed");
        let config = conf_file.mut_config();
        config.set_current_profile("local3");
        config
            .save_to("/tmp/test_config.toml")
            .expect("save should succeed");
        let update_conf_file =
            ConfigFile::load(Some("/tmp/test_config.toml".to_owned())).expect("parse failed");
        assert_eq!(
            update_conf_file.config().current_profile_name().unwrap(),
            "local3"
        );
    }

    /*
    #[test]
    fn test_topic_config() {
        let conf_file = ConfigFile::load(Some("test-data/profiles/config.toml".to_owned())).expect("parse failed");
        let config = conf_file.config().resolve_replica_config("test3",0);
    }
    */

    #[test]
    fn test_local_cluster() {
        let config = Config::new_with_local_cluster("localhost:9003".to_owned());

        assert_eq!(config.current_profile_name().unwrap(), "local");
        let cluster = config.current_cluster().expect("cluster should exists");
        assert_eq!(cluster.addr, "localhost:9003");
    }
}
