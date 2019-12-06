use std::fmt;
use std::io::Error as StdIoError;

use serde_yaml::Error as SerdYamlError;


#[derive(Debug)]
pub enum ConfigError {
    IoError(StdIoError),
    SerdeError(SerdYamlError),
    NoCurrentContext
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::SerdeError(err) => write!(f,"{}",err),
            Self::NoCurrentContext => write!(f,"no current context")
        }
    }
}

impl From<StdIoError> for ConfigError {
    fn from(error: StdIoError) -> Self {
        Self::IoError(error)
    }
}

impl From<SerdYamlError> for ConfigError {
    fn from(error: SerdYamlError) -> Self {
        Self::SerdeError(error)
    }
}
