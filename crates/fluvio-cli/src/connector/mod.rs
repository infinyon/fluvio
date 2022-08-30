use std::sync::Arc;
use clap::Parser;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;
use std::time::Duration;
use std::str::FromStr;
use bytesize::ByteSize;

use fluvio::{Fluvio, Compression};
use fluvio::metadata::connector::{
    ManagedConnectorSpec, SecretString, ManagedConnectorParameterValue,
    ManagedConnectorParameterValueInner,
};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::COMMAND_TEMPLATE;

mod create;
mod update;
mod delete;
mod list;
mod logs;
mod show_config;

use crate::Result;
use create::CreateManagedConnectorOpt;
use update::UpdateManagedConnectorOpt;
use delete::DeleteManagedConnectorOpt;
use list::ListManagedConnectorsOpt;
use logs::LogsManagedConnectorOpt;
use show_config::GetConfigManagedConnectorOpt;
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

    /// Delete one or more Managed Connectors with the given name(s)
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

    /// Show the connector spec
    #[clap(
        name = "config",
        help_template = COMMAND_TEMPLATE,
    )]
    Config(GetConfigManagedConnectorOpt),
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
            Self::Config(describe) => {
                describe.process(out, fluvio).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct ConnectorConfig {
    name: String,
    #[serde(rename = "type")]
    type_: String,

    pub(crate) topic: String,
    pub(crate) version: String,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    parameters: BTreeMap<String, ManagedConnectorParameterValue>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    secrets: BTreeMap<String, SecretString>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    producer: Option<ProducerParameters>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    consumer: Option<ConsumerParameters>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    transform: Option<TransformParameters>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct ConsumerParameters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition: Option<i32>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct ProducerParameters {
    #[serde(with = "humantime_serde")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    linger: Option<Duration>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    compression: Option<Compression>,

    // This is needed because `ByteSize` serde deserializes as bytes. We need to use the parse
    // feature to populate `batch_size`.
    #[serde(rename = "batch-size")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    batch_size_string: Option<String>,

    #[serde(skip)]
    batch_size: Option<ByteSize>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct TransformParameters(pub Vec<TransformStep>);

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct TransformStep {
    uses: String,
    invoke: String,
    #[serde(deserialize_with = "deserialize_to_json_string")]
    with: String,
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
                parameters.insert("producer-linger".to_string(), linger.into());
            }
            if let Some(compression) = producer.compression {
                let compression = format!("{:?}", compression).to_lowercase();
                parameters.insert("producer-compression".to_string(), compression.into());
            }
            if let Some(batch_size) = producer.batch_size {
                let batch_size = format!("{}", batch_size);
                parameters.insert("producer-batch-size".to_string(), batch_size.into());
            }
        }

        // Consumer arguments are prefixed with `consumer`.
        if let Some(consumer) = config.consumer {
            if let Some(partition) = consumer.partition {
                let partition = format!("{}", partition);
                parameters.insert("consumer-partition".to_string(), partition.into());
            }
        }

        if let Some(transform) = config.transform {
            let result: Vec<String> = transform
                .0
                .iter()
                .flat_map(|step| serde_json::to_string(step).ok())
                .collect();
            parameters.insert("transform".to_string(), result.into());
        }
        ManagedConnectorSpec {
            name: config.name,
            type_: config.type_,
            topic: config.topic,
            parameters,
            secrets: config.secrets,
            version: config.version.into(),
        }
    }
}
impl From<ManagedConnectorSpec> for ConnectorConfig {
    fn from(spec: ManagedConnectorSpec) -> ConnectorConfig {
        let mut parameters = spec.parameters;
        let mut producer: ProducerParameters = ProducerParameters {
            linger: None,
            compression: None,
            batch_size_string: None,
            batch_size: None,
        };
        if let Some(ManagedConnectorParameterValue(ManagedConnectorParameterValueInner::String(
            linger,
        ))) = parameters.remove("producer-linger")
        {
            producer.linger = humantime::parse_duration(&linger).ok();
        }
        if let Some(ManagedConnectorParameterValue(ManagedConnectorParameterValueInner::String(
            compression,
        ))) = parameters.remove("producer-compression")
        {
            producer.compression = Compression::from_str(&compression).ok();
        }
        if let Some(ManagedConnectorParameterValue(ManagedConnectorParameterValueInner::String(
            batch_size_string,
        ))) = parameters.remove("producer-batch-size")
        {
            let batch_size = batch_size_string.parse::<ByteSize>().ok();
            producer.batch_size_string = Some(batch_size_string);
            producer.batch_size = batch_size;
        }

        let producer = if producer.linger.is_none()
            && producer.compression.is_none()
            && producer.batch_size_string.is_none()
        {
            None
        } else {
            Some(producer)
        };

        let consumer = if let Some(ManagedConnectorParameterValue(
            ManagedConnectorParameterValueInner::String(partition),
        )) = parameters.remove("consumer-partition")
        {
            Some(ConsumerParameters {
                partition: partition.parse::<i32>().ok(),
            })
        } else {
            None
        };

        let transform = if let Some(ManagedConnectorParameterValue(
            ManagedConnectorParameterValueInner::Vec(transforms),
        )) = parameters.remove("transform")
        {
            let steps: Vec<TransformStep> = transforms
                .iter()
                .flat_map(|step| serde_json::from_str(step).ok())
                .collect();
            Some(TransformParameters(steps))
        } else {
            None
        };
        ConnectorConfig {
            name: spec.name,
            type_: spec.type_,
            topic: spec.topic,
            version: spec.version.to_string(),
            parameters,
            secrets: spec.secrets,
            producer,
            consumer,
            transform,
        }
    }
}

fn deserialize_to_json_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct MapAsJsonString;
    impl<'de> serde::de::Visitor<'de> for MapAsJsonString {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("str, string or map")
        }

        fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.to_string())
        }

        fn visit_string<E>(self, v: String) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v)
        }

        fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let json: serde_json::Value =
                Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
            serde_json::to_string(&json).map_err(|err| {
                serde::de::Error::custom(format!("unable to serialize to json: {}", err))
            })
        }
    }
    deserializer.deserialize_any(MapAsJsonString)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_yaml_in_and_out() {
        use pretty_assertions::assert_eq;
        let connector_input = ConnectorConfig::from_file("test-data/connectors/full-config.yaml")
            .expect("Failed to load test config");
        let spec_middle: ManagedConnectorSpec = connector_input.clone().into();
        let connector_output: ConnectorConfig = spec_middle.into();
        assert_eq!(connector_input, connector_output);
    }

    #[test]
    fn full_yaml_test() {
        use pretty_assertions::assert_eq;
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/full-config.yaml")
            .expect("Failed to load test config");
        let out: ManagedConnectorSpec = connector_cfg.into();
        let expected_params = BTreeMap::from([
            ("consumer-partition".to_string(), "10".to_string().into()),
            ("producer-linger".to_string(), "1ms".to_string().into()),
            (
                "producer-batch-size".to_string(),
                "44.0 MB".to_string().into(),
            ),
            (
                "producer-compression".to_string(),
                "gzip".to_string().into(),
            ),
            ("param_1".to_string(), "mqtt.hsl.fi".to_string().into()),
            (
                "param_2".to_string(),
                vec!["foo:baz".to_string(), "bar".to_string()].into(),
            ),
            (
                "param_3".to_string(),
                BTreeMap::from([
                    ("bar".to_string(), "10.0".to_string()),
                    ("foo".to_string(), "bar".to_string()),
                    ("linger.ms".to_string(), "10".to_string()),
                ])
                .into(),
            ),
            ("param_4".to_string(), "true".to_string().into()),
            ("param_5".to_string(), "10".to_string().into()),
            (
                "param_6".to_string(),
                vec!["-10".to_string(), "-10.0".to_string()].into(),
            ),
            ("transform".to_string(), vec!["{\"uses\":\"infinyon/json-sql\",\"invoke\":\"insert\",\"with\":\"{\\\"table\\\":\\\"topic_message\\\"}\"}".to_string()].into())
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
        assert_eq!("ConnectorConfig(Error(\"producer.linger: invalid value: string \\\"1\\\", expected a duration\", line: 8, column: 11))", format!("{:?}", connector_cfg));
        let connector_cfg =
            ConnectorConfig::from_file("test-data/connectors/error-compression.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!("ConnectorConfig(Error(\"producer.compression: unknown variant `gzipaoeu`, expected one of `none`, `gzip`, `snappy`, `lz4`\", line: 8, column: 16))", format!("{:?}", connector_cfg));

        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-batchsize.yaml")
            .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!("Other(\"couldn't parse \\\"aoeu\\\" into a known SI unit, couldn't parse unit of \\\"aoeu\\\"\")", format!("{:?}", connector_cfg));
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-version.yaml")
            .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "ConnectorConfig(Error(\"missing field `version`\", line: 1, column: 1))",
            format!("{:?}", connector_cfg)
        );
    }

    #[test]
    fn with_transform_yaml_test() {
        //given
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/with_transform.yaml")
            .expect("Failed to load test config");

        //when
        let out: ManagedConnectorSpec = connector_cfg.into();

        //then
        assert_eq!(
            out.parameters.get("transform"),
            Some(&ManagedConnectorParameterValue(
                ManagedConnectorParameterValueInner::Vec(vec!["{\"uses\":\"infinyon/sql\",\"invoke\":\"insert\",\"with\":\"{\\\"map-columns\\\":{\\\"device_id\\\":{\\\"json-key\\\":\\\"device.device_id\\\",\\\"value\\\":{\\\"default\\\":0,\\\"required\\\":true,\\\"type\\\":\\\"int\\\"}},\\\"record\\\":{\\\"json-key\\\":\\\"$\\\",\\\"value\\\":{\\\"required\\\":true,\\\"type\\\":\\\"jsonb\\\"}}},\\\"table\\\":\\\"topic_message\\\"}\"}".to_string()])
            ))
        );
    }

    #[test]
    fn with_many_transforms_yaml_test() {
        //given
        let connector_cfg =
            ConnectorConfig::from_file("test-data/connectors/with_transform_many.yaml")
                .expect("Failed to load test config");

        //when
        let out: ManagedConnectorSpec = connector_cfg.into();

        //then
        assert_eq!(
            out.parameters.get("transform"),
            Some(&ManagedConnectorParameterValue(
                ManagedConnectorParameterValueInner::Vec(vec![
                    "{\"uses\":\"infinyon/json-sql\",\"invoke\":\"insert\",\"with\":\"{\\\"table\\\":\\\"topic_message\\\"}\"}".to_string(),
                    "{\"uses\":\"infinyon/avro-sql\",\"invoke\":\"insert\",\"with\":\"{\\\"table\\\":\\\"topic_message\\\"}\"}".to_string(),
                ])
            ))
        );
    }
}
