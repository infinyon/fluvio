use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Deserialize;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::smartstream::{SmartStreamSpec};

use crate::Result;
use crate::error::CliError;

/// Create a new SmartModule with a given name
#[derive(Debug, StructOpt)]
pub struct CreateSmartStreamOpt {
    /// The name for the new Managed Connector
    #[structopt(short = "c", long = "config", value_name = "config")]
    pub config: PathBuf,
}

impl CreateSmartStreamOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let config = SmartStreamCreateConfig::from_file(&self.config)?;
        let SmartStreamCreateConfig { name, spec } = config;

        let admin = fluvio.admin().await;
        admin.create(name.clone(), false, spec).await?;
        println!("smartstream \"{}\" created", name);

        Ok(())
    }
}

/// Used only for creation
#[derive(Debug, Deserialize, Clone)]
pub struct SmartStreamCreateConfig {
    name: String,
    #[serde(flatten)]
    spec: SmartStreamSpec,
}

impl SmartStreamCreateConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, CliError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: Self = serde_yaml::from_str(&contents).map_err(|e| {
            CliError::Other(format!("failed to parse smartstream config: {:#?}", e))
        })?;
        Ok(config)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_config_right() {
        let config =
            SmartStreamCreateConfig::from_file("test-data/smartstream/right.yaml").expect("parse");
        assert_eq!(config.name, "rdouble");
    }

    #[test]
    fn test_config_left() {
        let config =
            SmartStreamCreateConfig::from_file("test-data/smartstream/left.yaml").expect("parse");
        assert_eq!(config.name, "sjoin");
    }
}
