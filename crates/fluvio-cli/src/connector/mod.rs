use std::sync::Arc;
use clap::Parser;

use serde::{Deserializer, Deserialize};
use serde::de::{self, Visitor, SeqAccess, MapAccess};
use std::fmt;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio::{Fluvio, Compression};
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

    #[serde(default)]
    producer: Option<ProducerParameters>,

    #[serde(default)]
    consumer: Option<ConsumerParameters>,
}

#[test]
fn config_test() {
    let connector_cfg = ConnectorConfig::from_file("test-data/test-config.yaml")
        .expect("Failed to load test config");
    println!("{:#?}", connector_cfg);
    let out: ManagedConnectorSpec = connector_cfg.into();
    println!("{:#?}", out);
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerParameters {
    #[serde(default)]
    partition: Option<i32>,
}
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct ProducerParameters {
    #[serde(with = "humantime_serde")]
    linger: Option<Duration>,

    compression: Option<Compression>,

    batch_size: Option<usize>,
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

        // Producer arguments are prefixed with `producer`
        if let Some(producer) = config.producer {
            if let Some(linger) = producer.linger {
                let linger = humantime::format_duration(linger).to_string();
                parameters.insert("producer-linger".to_string(), vec![linger]);
            }
            if let Some(compression) = producer.compression {
                let compression = format!("{:?}", compression);
                parameters.insert("producer-compression".to_string(), vec![compression]);
            }
            if let Some(batch_size) = producer.batch_size {
                let batch_size = format!("{}", batch_size);
                parameters.insert("producer-batch-size".to_string(), vec![batch_size]);
            }
        }

        // Consumer arguments are prefixed with `consumer`.
        if let Some(consumer) = config.consumer {
            if let Some(partition) = consumer.partition {
                let partition = format!("{}", partition);
                parameters.insert("consumer-partition".to_string(), vec![partition]);
            }
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

struct YamlParameterVisitor;

impl<'de> Visitor<'de> for YamlParameterVisitor {
    type Value = YamlParameter;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string or map")
    }

    fn visit_str<E>(self, value: &str) -> Result<YamlParameter, E>
    where
        E: de::Error,
    {
        Ok(YamlParameter {
            context: vec![value.to_string()],
        })
    }
    fn visit_map<M>(self, mut map: M) -> Result<YamlParameter, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut yaml_param = YamlParameter { context: vec![] };
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            let param = format!("{}:{}", key.clone(), value.clone());
            yaml_param.context.push(param);
        }

        Ok(yaml_param)
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<YamlParameter, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut yaml_param = YamlParameter { context: vec![] };
        while let Some(param) = seq.next_element::<String>()? {
            yaml_param.context.push(param);
        }
        Ok(yaml_param)
    }
}
