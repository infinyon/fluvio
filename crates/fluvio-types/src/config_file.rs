use std::fmt::Debug;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Write;
use std::path::Path;
use std::fs::{File, read_to_string};

use tracing::debug;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadConfigError {
    #[error("IoError: {0}")]
    IoError(IoError),
    #[error("TomlError: {0}")]
    TomlError(toml::de::Error),
}

pub trait SaveLoadConfig {
    fn save_to<T: AsRef<Path>>(&self, path: T) -> Result<(), IoError>;
    fn load_from<T: AsRef<Path>>(path: T) -> Result<Self, LoadConfigError>
    where
        Self: Sized;
    fn load_str(config: &str) -> Result<Self, LoadConfigError>
    where
        Self: Sized;
}

impl<S> SaveLoadConfig for S
where
    S: Serialize + DeserializeOwned + Debug,
{
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

    fn load_from<T: AsRef<Path>>(path: T) -> Result<Self, LoadConfigError> {
        let path_ref = path.as_ref();
        debug!(?path_ref, "loading from");

        let file_str = read_to_string(path_ref).map_err(LoadConfigError::IoError)?;

        let config = toml::from_str(&file_str).map_err(LoadConfigError::TomlError)?;
        Ok(config)
    }

    fn load_str(config: &str) -> Result<Self, LoadConfigError>
    where
        Self: Sized,
    {
        let config = toml::from_str(config).map_err(LoadConfigError::TomlError)?;
        Ok(config)
    }
}
