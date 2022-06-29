use std::sync::Arc;
use clap::Parser;

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;
use std::time::Duration;
use bytesize::ByteSize;

use fluvio::{Fluvio, Compression};
use fluvio::metadata::connector::{ManagedConnectorSpec, SecretString, ManageConnectorParameterValue};
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
    parameters: BTreeMap<String, ManageConnectorParameterValue>,

    #[serde(default)]
    secrets: BTreeMap<String, SecretString>,

    #[serde(default)]
    producer: Option<ProducerParameters>,

    #[serde(default)]
    consumer: Option<ConsumerParameters>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerParameters {
    #[serde(default)]
    partition: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProducerParameters {
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    linger: Option<Duration>,

    #[serde(default)]
    compression: Option<Compression>,

    // This is needed because `ByteSize` serde deserializes as bytes. We need to use the parse
    // feature to populate `batch_size`.
    #[serde(rename = "batch-size")]
    #[serde(default)]
    batch_size_string: Option<String>,

    #[serde(skip)]
    batch_size: Option<ByteSize>,
}

impl ConnectorConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, CliError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut connector_config: Self = serde_yaml::from_str(&contents)?;

        // This is needed because we want to use a human readable version of `BatchSize` but the
        // serde support for BatchSize serializes and deserializes as bytes.
        if let Some(ref mut producer) = &mut connector_config.producer {
            if let Some(batch_size_string) = &producer.batch_size_string {
                let batch_size = batch_size_string
                    .parse::<ByteSize>()
                    .map_err(CliError::Other)?;
                producer.batch_size = Some(batch_size);
            }
        }
        Ok(connector_config)
    }
}

impl From<ConnectorConfig> for ManagedConnectorSpec {
    fn from(config: ConnectorConfig) -> ManagedConnectorSpec {
        let mut parameters = config.parameters;

        // Producer arguments are prefixed with `producer`
        if let Some(producer) = config.producer {
            if let Some(linger) = producer.linger {
                let linger = humantime::format_duration(linger).to_string();
                parameters.insert(
                    "producer-linger".to_string(),
                    ManageConnectorParameterValue::from(linger),
                );
            }
            if let Some(compression) = producer.compression {
                let compression = format!("{:?}", compression);
                parameters.insert(
                    "producer-compression".to_string(),
                    ManageConnectorParameterValue::from(compression),
                );
            }
            if let Some(batch_size) = producer.batch_size {
                let batch_size = format!("{}", batch_size);
                parameters.insert(
                    "producer-batch-size".to_string(),
                    ManageConnectorParameterValue::from(batch_size),
                );
            }
        }

        // Consumer arguments are prefixed with `consumer`.
        if let Some(consumer) = config.consumer {
            if let Some(partition) = consumer.partition {
                let partition = format!("{}", partition);
                parameters.insert(
                    "consumer-partition".to_string(),
                    ManageConnectorParameterValue::from(partition),
                );
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

#[test]
fn full_yaml_test() {
    use pretty_assertions::assert_eq;
    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/full-config.yaml")
        .expect("Failed to load test config");
    let out: ManagedConnectorSpec = connector_cfg.into();
    let expected_params = BTreeMap::from([
        (
            "consumer-partition".to_string(),
            ManageConnectorParameterValue::from("10".to_string()),
        ),
        (
            "producer-linger".to_string(),
            ManageConnectorParameterValue::from("1ms".to_string()),
        ),
        (
            "producer-batch-size".to_string(),
            ManageConnectorParameterValue::from("44.0 MB".to_string()),
        ),
        (
            "producer-compression".to_string(),
            ManageConnectorParameterValue::from("Gzip".to_string()),
        ),
        (
            "param_1".to_string(),
            ManageConnectorParameterValue::from("mqtt.hsl.fi".to_string()),
        ),
        (
            "param_2".to_string(),
            ManageConnectorParameterValue::from(vec!["foo:baz".to_string(), "bar".to_string()]),
        ),
        (
            "param_3".to_string(),
            ManageConnectorParameterValue::from(BTreeMap::from([
                ("bar".to_string(), "10.0".to_string()),
                ("foo".to_string(), "bar".to_string()),
                ("linger.ms".to_string(), "10".to_string()),
            ])),
        ),
        (
            "param_4".to_string(),
            ManageConnectorParameterValue::from("true".to_string()),
        ),
        (
            "param_5".to_string(),
            ManageConnectorParameterValue::from("10".to_string()),
        ),
        (
            "param_6".to_string(),
            ManageConnectorParameterValue::from(vec!["-10".to_string(), "-10.0".to_string()]),
        ),
    ]);
    assert_eq!(out.parameters, expected_params);
}

#[test]
fn simple_yaml_test() {
    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/simple.yaml")
        .expect("Failed to load test config");
    let out: ManagedConnectorSpec = connector_cfg.into();
    let expected_params = BTreeMap::new();
    assert_eq!(out.parameters, expected_params);
}

#[test]
fn error_yaml_tests() {
    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-linger.yaml")
        .expect_err("This yaml should error");
    #[cfg(unix)]
    assert_eq!("ConnectorConfig(Message(\"invalid value: string \\\"1\\\", expected a duration\", Some(Pos { marker: Marker { index: 118, line: 8, col: 10 }, path: \"producer.linger\" })))", format!("{:?}", connector_cfg));
    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-compression.yaml")
        .expect_err("This yaml should error");
    #[cfg(unix)]
    assert_eq!("ConnectorConfig(Message(\"unknown variant `gzipaoeu`, expected one of `none`, `gzip`, `snappy`, `lz4`\", Some(Pos { marker: Marker { index: 123, line: 8, col: 15 }, path: \"producer.compression\" })))", format!("{:?}", connector_cfg));

    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-batchsize.yaml")
        .expect_err("This yaml should error");
    #[cfg(unix)]
    assert_eq!("Other(\"couldn't parse \\\"aoeu\\\" into a known SI unit, couldn't parse unit of \\\"aoeu\\\"\")", format!("{:?}", connector_cfg));
}
