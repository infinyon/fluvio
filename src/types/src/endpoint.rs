use std::str::FromStr;
use std::{convert::TryFrom, fmt};
use std::io::Error as IoError;
use std::io::ErrorKind as IoErrorKind;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for Endpoint {
    type Err = IoError;

    fn from_str(host_port: &str) -> Result<Self, Self::Err> {
        let v: Vec<&str> = host_port.split(':').collect();

        if v.len() != 2 {
            return Err(IoError::new(
                IoErrorKind::InvalidInput,
                format!("invalid host:port format {}", host_port).as_str(),
            ));
        }

        Ok(Self {
            host: v[0].to_string(),
            port: v[1]
                .parse::<u16>()
                .map_err(|err| IoError::new(IoErrorKind::InvalidData, format!("{}", err)))?,
        })
    }
}

impl TryFrom<String> for Endpoint {
    type Error = IoError;

    fn try_from(host_port: String) -> Result<Self, Self::Error> {
        Self::from_str(&host_port)
    }
}
