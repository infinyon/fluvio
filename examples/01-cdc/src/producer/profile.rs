//!
//! # Profile file
//!
use serde::{Deserialize, Serialize};
use std::fs::read_to_string;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

const DEFAULT_TOPIC: &str = "rust-mysql-cdc";
const DEFAULT_REPLICAS: i16 = 1;
pub struct Config {
    profile: Profile,
}

impl Config {
    /// try to load from default locations
    pub fn load(path: &PathBuf) -> Result<Self, Error> {
        Self::from_file(path)
    }

    /// read from file
    fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, Error> {
        let path_ref = path.as_ref();

        let file_str: String = read_to_string(path_ref)
            .map_err(|err| Error::new(ErrorKind::NotFound, format!("{}", err)))?;
        let profile = toml::from_str(&file_str)
            .map_err(|err| Error::new(ErrorKind::InvalidData, format!("{}", err)))?;

        Ok(Self { profile })
    }

    /// retrieve profile
    pub fn profile(&self) -> &Profile {
        &self.profile
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    binlog_index_file: PathBuf,
    mysql_resource_name: String,
    resume_file: PathBuf,
    database: Database,
    filters: Option<Filters>,
    fluvio: Option<Fluvio>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
pub struct Database {
    ip_or_host: String,
    port: Option<u16>,
    user: String,
    password: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Filters {
    Include { include_dbs: Vec<String> },
    Exclude { exclude_dbs: Vec<String> },
}
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Fluvio {
    topic: String,
    replicas: Option<i16>,
}

impl Profile {
    #[allow(dead_code)] // used in unit
    pub fn set_binlog_index_file(&mut self, bn_file_idx: PathBuf) {
        self.binlog_index_file = bn_file_idx;
    }

    pub fn binlog_index_file(&self) -> &PathBuf {
        &self.binlog_index_file
    }

    pub fn resume_file(&self) -> &Path {
        &self.resume_file
    }

    pub fn mysql_resource_name(&self) -> &String {
        &self.mysql_resource_name
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn filters(&self) -> Option<Filters> {
        self.filters.clone()
    }

    pub fn topic(&self) -> String {
        if let Some(fluvio) = &self.fluvio {
            fluvio.topic.clone()
        } else {
            DEFAULT_TOPIC.to_owned()
        }
    }

    pub fn replicas(&self) -> i16 {
        if let Some(fluvio) = &self.fluvio {
            if let Some(replicas) = fluvio.replicas {
                return replicas;
            }
        }
        DEFAULT_REPLICAS
    }
}

impl Database {
    pub fn ip_or_host(&self) -> Option<String> {
        Some(self.ip_or_host.clone())
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(3306)
    }

    pub fn user(&self) -> Option<String> {
        Some(self.user.clone())
    }

    pub fn password(&self) -> Option<String> {
        self.password.clone()
    }
}
