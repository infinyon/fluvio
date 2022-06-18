use std::sync::Arc;
use clap::Parser;

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio::Fluvio;
use fluvio::metadata::connector::{ManagedConnectorSpec, SecretString};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::COMMAND_TEMPLATE;

mod create;
mod update;
mod delete;
mod list;
mod logs;

use crate::Result;
use create::CreateManagedConnectorOpt;
use update::UpdateManagedConnectorOpt;
use delete::DeleteManagedConnectorOpt;
use list::ListManagedConnectorsOpt;
use logs::LogsManagedConnectorOpt;
use crate::CliError;

#[derive(Debug, Parser)]
pub enum ManagedConnectorCmd {
    /// Create a new Managed Connector
    #[clap(
        name = "create",
        help_template = COMMAND_TEMPLATE,
    )]
    Create(CreateManagedConnectorOpt),

    /// Update a Managed Connector
    #[clap(
        name = "update",
        help_template = COMMAND_TEMPLATE,
    )]
    Update(UpdateManagedConnectorOpt),

    /// Delete a Managed Connector
    #[clap(
        name = "delete",
        help_template = COMMAND_TEMPLATE,
    )]
    Delete(DeleteManagedConnectorOpt),

    /// Get the logs for a Managed Connector
    #[clap(
        name = "logs",
        help_template = COMMAND_TEMPLATE,
    )]
    Logs(LogsManagedConnectorOpt),

    /// List all Managed Connectors
    #[clap(
        name = "list",
        help_template = COMMAND_TEMPLATE,
    )]
    List(ListManagedConnectorsOpt),
}

impl ManagedConnectorCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            Self::Create(create) => {
                create.process(fluvio).await?;
            }
            Self::Update(update) => {
                update.process(fluvio).await?;
            }
            Self::Delete(delete) => {
                delete.process(fluvio).await?;
            }
            Self::Logs(logs) => {
                logs.process().await?;
            }
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectorConfig {
    name: String,
    #[serde(rename = "type")]
    type_: String,

    pub(crate) topic: String,
    pub(crate) version: Option<String>,

    #[serde(default)]
    parameters: BTreeMap<String, YamlParameter>,

    #[serde(default)]
    secrets: BTreeMap<String, SecretString>,
}
use serde::{Deserializer};

#[derive(Debug, Clone)]
pub struct YamlParameter {
    context: Vec<String>,
}
impl<'de> Deserialize<'de> for YamlParameter {
    fn deserialize<D>(deserializer: D) -> Result<YamlParameter, D::Error>
    where
        D: Deserializer<'de>,
    {
		deserializer.deserialize_any(YamlParameterVisitor)
    }
}
use serde::de::{self, Visitor, SeqAccess};

struct YamlParameterVisitor;

    use std::fmt;
impl<'de> Visitor<'de> for YamlParameterVisitor
{
    type Value = YamlParameter;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string or map")
    }

    fn visit_str<E>(self, value: &str) -> Result<YamlParameter, E>
    where
        E: de::Error,
    {
        Ok(YamlParameter{ context: vec![value.to_string()] })
    }

    fn visit_seq<A>(self, seq: A) -> Result<YamlParameter, A::Error>
        where A: SeqAccess<'de>,
    {
        Deserialize::deserialize(de::value::SeqAccessDeserializer::new(seq))
    }

}

use std::str::FromStr;
impl FromStr for YamlParameter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(YamlParameter {
            context: vec![s.to_string()],
        })
    }
}

impl ConnectorConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, CliError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let connector_config: Self = serde_yaml::from_str(&contents)?;
        Ok(connector_config)
    }
}

impl From<ConnectorConfig> for ManagedConnectorSpec {
    fn from(config: ConnectorConfig) -> ManagedConnectorSpec {
        let mut parameters = BTreeMap::new();
        for (key, value) in config.parameters.iter() {
            parameters.insert(key.clone(), value.context.clone());
        }
        ManagedConnectorSpec {
            name: config.name,
            type_: config.type_,
            topic: config.topic,
            parameters,
            secrets: config.secrets,
            version: config.version,
        }
    }
}

#[test]
fn config_test() {
    let out: ManagedConnectorSpec = ConnectorConfig::from_file("test-data/test-config.yaml")
        .expect("Failed to load test config")
        .into();
    println!("{:?}", out);
}
