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

use dirs::home_dir;
use serde::Deserialize;
use serde::Serialize;
use toml::value::Table;

use types::defaults::{CLI_CONFIG_PATH};
use flv_future_aio::net::tls::AllDomainConnector;


use crate::ClientConfig;

pub struct ConfigFile {
    path: PathBuf,
    config: Config
}

impl ConfigFile {
    fn new(path: PathBuf,config: Config) -> Self {
        Self {
            path,
            config
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn mut_config(&mut self) -> &mut Config {
        &mut self.config
    }

    // save to file
    pub fn save(&self) -> Result<(),IoError>  {

        self.config.save_to(&self.path)
    }
}

#[derive(Debug, PartialEq, Serialize,Deserialize)]
pub struct Config {
    version: String,
    current_profile: String,
    profile: HashMap<String,Profile>,
    topic: Table,
    cluster: HashMap<String,Cluster>,
    client_id: Option<String>
}


impl Config {

    /// try to load from default locations
    pub fn load(optional_path: Option<String>) -> Result<ConfigFile,IoError> {

        Self::from_file(Self::default_file_path(optional_path)?)
    }
    
    /// read from file
    fn from_file<T: AsRef<Path>>(path: T) -> Result<ConfigFile, IoError> {
        let path_ref = path.as_ref();
        let file_str: String = read_to_string(path_ref)?;
        let config = toml::from_str(&file_str)
            .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?;
        Ok(ConfigFile::new(path_ref.to_owned(), config))
    }

    

    /// find default path where config is stored.  precedent is:
    /// 1) supplied path
    /// 2) environment variable in FLV_PROFILE_PATH
    /// 3) home directory ~/.fluvio/config
    fn default_file_path(path: Option<String>) -> Result<PathBuf,IoError> {

        path
            .map(|p| Ok(PathBuf::from(p)))
            .unwrap_or_else(|| {
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
                                "can't get home directory",
                            ))
                        }
                    })
            })
    }

    // save to file
    fn save_to<T: AsRef<Path>>(&self, path: T) -> Result<(),IoError>  {
        let toml = toml::to_vec(self)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("{}", err)))?;

        let mut file = File::create(path)?;
        file.write_all(&toml)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    /// current profile
    pub fn current_profile_name(&self) -> &str {
        &self.current_profile
    }

    pub fn current_profile(&self) -> Option<&Profile> {
        self.profile.get(&self.current_profile)
    }

    /// set current profile, if profile doesn't exists return false
    pub fn set_current_profile(&mut self,profile_name: &str) -> bool {

        if self.profile.contains_key(profile_name) {
            self.current_profile = profile_name.to_owned();
            true
        } else {
            false
        }
    }


    pub fn current_cluster(&self) -> Option<&Cluster> {
        self.current_profile().and_then(|profile| self.cluster.get(&profile.cluster))
    }

    /// look up replica config
    /// this will iterate and find all configuration that can resolve config
    /// 1) match all config that matches criteria including asterik
    /// 2) apply in terms of precedent
    pub fn resolve_replica_config(&self,_topic_name: &str,_partition: i32) -> Replica {

            for (key, val) in self.topic.iter() {
                println!("key: {:#?}, value: {:#?}",key,val);
            }
        

        Replica::default()
    }
    
}



#[derive(Debug, PartialEq,Serialize, Deserialize)]
pub struct Topic {
    replica: HashMap<String,String>
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    pub cluster: String,
    pub topic: Option<String>,
    pub partition: Option<i32>
}

#[derive(Debug, Default,PartialEq, Serialize, Deserialize)]
pub struct Replica {
    pub max_bytes: Option<i32>,
    pub isolation: Option<String>
}


#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    domain: String,
    certificate_authority_data: Option<String>
}

impl From<String> for Cluster {
    fn from(domain: String) -> Self {
        Self {
            domain,
            ..Default::default()
        }
    }
}

impl Cluster {

    pub fn domain(&self) -> &str {
        &self.domain
    }

}


impl From<Cluster> for ClientConfig {
    fn from(cluster: Cluster) -> Self {

        ClientConfig::new(cluster.domain,AllDomainConnector::default_tcp())

    }
}



#[cfg(test)]
pub mod test {
    use super::*;
    use std::path::PathBuf;
    
    #[test]
    fn test_default_path_arg() {
        assert_eq!(Config::default_file_path(Some("/user1/test".to_string())).expect("file"),
            PathBuf::from("/user1/test"));
    }
     
    //#[test]
    fn test_default_path_env() {
        env::set_var("FLV_PROFILE_PATH", "/user2/config");
        assert_eq!(Config::default_file_path(None).expect("file"),
            PathBuf::from("/user2/config"));
        env::remove_var("FLV_PROFILE_PATH");
    }

    #[test]
    fn test_default_path_home() {
        
        let mut path = home_dir().expect("home dir must exist");
        path.push(CLI_CONFIG_PATH);
        path.push("config");
        assert_eq!(Config::default_file_path(None).expect("file"),path);
    }

    /// test basic reading
    #[test]
    fn test_config() {
    
        // test read & parse
        let mut conf_file = Config::load(Some("test-data/profiles/config.toml".to_owned())).expect("parse failed");
        let config = conf_file.mut_config();

        assert_eq!(config.version(),"1.0");
        assert_eq!(config.current_profile_name(),"local");
        let profile = config.current_profile().expect("profile should exists");
        assert_eq!(profile.cluster,"local");

        assert!(!config.set_current_profile("dummy"));
        assert!(config.set_current_profile("local2"));
        assert_eq!(config.current_profile_name(),"local2");

        let cluster = config.current_cluster().expect("cluster should exist");
        assert_eq!(cluster.domain,"127.0.0.1:9003");
    }

    #[test]
    fn test_set_config() {
        let mut conf_file = Config::load(Some("test-data/profiles/config.toml".to_owned())).expect("parse failed");
        let config = conf_file.mut_config();
        config.set_current_profile("local3");
        config.save_to("/tmp/test_config.toml").expect("save should succeed");
        let update_conf_file = Config::load(Some("/tmp/test_config.toml".to_owned())).expect("parse failed");
        assert_eq!(update_conf_file.config().current_profile_name(),"local3");
    }

    #[test]
    fn test_topic_config() {
        let conf_file = Config::load(Some("test-data/profiles/config.toml".to_owned())).expect("parse failed");
        let config = conf_file.config().resolve_replica_config("test3",0);
    }


}
