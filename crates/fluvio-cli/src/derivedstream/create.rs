use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Deserialize;
use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::derivedstream::{DerivedStreamSpec};

use crate::Result;
use crate::error::CliError;

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct CreateDerivedStreamOpt {
    /// The name for the new Managed Connector
    #[clap(short = 'c', long = "config", value_name = "config")]
    pub config: PathBuf,
}

impl CreateDerivedStreamOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let config = DerivedStreamCreateConfig::from_file(&self.config)?;
        let DerivedStreamCreateConfig { name, spec } = config;

        let admin = fluvio.admin().await;
        admin.create(name.clone(), false, spec).await?;
        println!("derived-stream \"{}\" created", name);

        Ok(())
    }
}

/// Used only for creation
#[derive(Debug, Deserialize, Clone)]
pub struct DerivedStreamCreateConfig {
    name: String,
    #[serde(flatten)]
    spec: DerivedStreamSpec,
}

impl DerivedStreamCreateConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, CliError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: Self = serde_yaml::from_str(&contents).map_err(|e| {
            CliError::Other(format!("failed to parse derived-stream config: {:#?}", e))
        })?;
        Ok(config)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_config_right() {
        let config = DerivedStreamCreateConfig::from_file("test-data/derivedstream/right.yaml")
            .expect("parse");
        assert_eq!(config.name, "rdouble");
    }

    #[test]
    fn test_config_left() {
        let config = DerivedStreamCreateConfig::from_file("test-data/derivedstream/left.yaml")
            .expect("parse");
        assert_eq!(config.name, "sjoin");
    }
}
