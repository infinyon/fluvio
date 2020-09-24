//!
//! # Profile file
//!
use serde::{Deserialize, Serialize};
use std::fs::read_to_string;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

const DEFAULT_TOPIC: &str = "rust-mysql-cdc";

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
        let mut profile: Profile = toml::from_str(&file_str)
            .map_err(|err| Error::new(ErrorKind::InvalidData, format!("{}", err)))?;

        if let Some(last_offset_file) = expand_tilde(&profile.last_offset_file) {
            profile.last_offset_file = last_offset_file;
        }

        Ok(Self { profile })
    }

    /// retrieve profile
    pub fn profile(&self) -> &Profile {
        &self.profile
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    last_offset_file: PathBuf,
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
}

impl Profile {
    pub fn last_offset_file(&self) -> &PathBuf {
        &self.last_offset_file
    }

    pub fn ip_or_host(&self) -> Option<String> {
        Some(self.database.ip_or_host.clone())
    }

    pub fn port(&self) -> u16 {
        self.database.port.unwrap_or(3306)
    }

    pub fn user(&self) -> Option<String> {
        Some(self.database.user.clone())
    }

    pub fn password(&self) -> Option<String> {
        self.database.password.clone()
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
}

fn expand_tilde<P: AsRef<Path>>(path_user_input: P) -> Option<PathBuf> {
    let p = path_user_input.as_ref();

    if !p.starts_with("~") {
        return Some(p.to_path_buf());
    }

    if p == Path::new("~") {
        return dirs::home_dir();
    }

    dirs::home_dir().map(|mut h| {
        if h == Path::new("/") {
            p.strip_prefix("~").unwrap().to_path_buf()
        } else {
            h.push(p.strip_prefix("~/").unwrap());
            h
        }
    })
}

#[cfg(test)]
pub mod test {
    use super::*;
    use std::path::PathBuf;

    const TEST_PATH: &str = "test_files";
    const PROFILE_FULL: &str = "consumer_profile_full.toml";
    const PROFILE_MIN: &str = "consumer_profile_min.toml";

    fn get_base_dir() -> PathBuf {
        let program_dir = std::env::current_dir().unwrap();
        program_dir.join(TEST_PATH)
    }

    #[test]
    fn test_expand_tilde() {
        let test_file = PathBuf::from("~/data/cdc-consumer/mysql80.offset");

        let expanded = expand_tilde(&test_file);
        assert!(expanded.is_some());

        let expected = PathBuf::from(format!(
            "{}{}",
            std::env::var("HOME").unwrap(),
            "/data/cdc-consumer/mysql80.offset"
        ));
        assert_eq!(expanded.unwrap(), expected);
    }

    #[test]
    fn test_full_profile() {
        let last_offset_file = expand_tilde(&PathBuf::from("~/data/consumer.offset")).unwrap();
        let profile_path = get_base_dir().join(PROFILE_FULL);
        let profile_file = Config::load(&profile_path);

        if let Err(err) = &profile_file {
            println!("{:?}", err);
        };

        assert!(profile_file.is_ok());
        let expected = Profile {
            last_offset_file: last_offset_file.clone(),
            database: Database {
                ip_or_host: "localhost".to_owned(),
                port: Some(3306),
                user: "root".to_owned(),
                password: Some("root".to_owned()),
            },
            filters: Some(Filters::Exclude {
                exclude_dbs: vec!["mysql".to_owned(), "sys".to_owned()],
            }),
            fluvio: Some(Fluvio {
                topic: "rust-mysql-cdc".to_owned(),
            }),
        };

        let profile = profile_file.as_ref().unwrap().profile();
        assert_eq!(profile, &expected);
        assert_eq!(profile.last_offset_file(), &last_offset_file);
        assert_eq!(profile.ip_or_host(), Some("localhost".to_owned()));
        assert_eq!(profile.port(), 3306);
        assert_eq!(profile.user(), Some("root".to_owned()));
        assert_eq!(profile.password(), Some("root".to_owned()));
        assert_eq!(profile.topic(), "rust-mysql-cdc".to_owned());
    }

    #[test]
    fn test_min_profile() {
        let last_offset_file = expand_tilde(&PathBuf::from("/tmp/data/consumer2.offset")).unwrap();
        let profile_path = get_base_dir().join(PROFILE_MIN);
        let profile_file = Config::load(&profile_path);

        if let Err(err) = &profile_file {
            println!("{:?}", err);
        };

        assert!(profile_file.is_ok());
        let expected = Profile {
            last_offset_file: last_offset_file.clone(),
            database: Database {
                ip_or_host: "localhost".to_owned(),
                user: "root".to_owned(),
                port: None,
                password: None,
            },
            filters: None,
            fluvio: None,
        };

        let profile = profile_file.as_ref().unwrap().profile();
        assert_eq!(profile, &expected);
        assert_eq!(profile.last_offset_file(), &last_offset_file);
        assert_eq!(profile.ip_or_host(), Some("localhost".to_owned()));
        assert_eq!(profile.port(), 3306);
        assert_eq!(profile.user(), Some("root".to_owned()));
        assert_eq!(profile.password(), None);
        assert_eq!(profile.topic(), DEFAULT_TOPIC.to_owned());
    }
}
