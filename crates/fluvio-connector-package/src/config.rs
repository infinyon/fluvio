use std::fs::File;
use std::io::Read;
use std::path::{PathBuf, Path};
use std::time::Duration;

use fluvio_types::PartitionId;
use tracing::debug;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use bytesize::ByteSize;

use fluvio_smartengine::transformation::TransformationConfig;
use fluvio_compression::Compression;
use crate::metadata::Direction;

const SOURCE_SUFFIX: &str = "-source";
const IMAGE_PREFFIX: &str = "infinyon/fluvio-connect";

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ConnectorConfig {
    pub meta: MetaConfig,

    #[serde(default, flatten, skip_serializing_if = "Option::is_none")]
    pub transforms: Option<TransformationConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct MetaConfig {
    pub name: String,

    #[serde(rename = "type")]
    pub type_: String,

    pub topic: String,

    pub version: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerParameters>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<ConsumerParameters>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ConsumerParameters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<PartitionId>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ProducerParameters {
    #[serde(with = "humantime_serde")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linger: Option<Duration>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,

    // This is needed because `ByteSize` serde deserializes as bytes. We need to use the parse
    // feature to populate `batch_size`.
    #[serde(rename = "batch-size")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    batch_size_string: Option<String>,

    #[serde(skip)]
    pub batch_size: Option<ByteSize>,
}

impl ConnectorConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        ConnectorConfig::config_from_str(&contents)
    }

    /// Only parses the meta section of the config
    pub fn config_from_str(config_str: &str) -> Result<Self> {
        let mut connector_config: Self = serde_yaml::from_str(config_str)?;
        connector_config.normalize_batch_size()?;

        debug!("Using connector config {connector_config:#?}");
        Ok(connector_config)
    }

    pub fn from_value(value: serde_yaml::Value) -> Result<Self> {
        let mut connector_config: Self = serde_yaml::from_value(value)?;
        connector_config.normalize_batch_size()?;

        debug!("Using connector config {connector_config:#?}");
        Ok(connector_config)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        std::fs::write(path, serde_yaml::to_vec(self)?)?;
        Ok(())
    }

    pub fn direction(&self) -> Direction {
        if self.meta.type_.ends_with(SOURCE_SUFFIX) {
            Direction::source()
        } else {
            Direction::dest()
        }
    }

    pub fn image(&self) -> String {
        format!(
            "{}-{}:{}",
            IMAGE_PREFFIX, self.meta.type_, self.meta.version
        )
    }

    fn normalize_batch_size(&mut self) -> Result<()> {
        // This is needed because we want to use a human readable version of `BatchSize` but the
        // serde support for BatchSize serializes and deserializes as bytes.
        if let Some(ref mut producer) = &mut self.meta.producer {
            if let Some(batch_size_string) = &producer.batch_size_string {
                let batch_size = batch_size_string
                    .parse::<ByteSize>()
                    .map_err(|err| anyhow::anyhow!("Fail to parse byte size {}", err))?;
                producer.batch_size = Some(batch_size);
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use fluvio_smartengine::transformation::TransformationStep;
    use pretty_assertions::assert_eq;

    #[test]
    fn full_yaml_test() {
        //given
        let expected = ConnectorConfig {
            meta: MetaConfig {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt".to_string(),
                topic: "my-mqtt".to_string(),
                version: "0.1.0".to_string(),
                producer: Some(ProducerParameters {
                    linger: Some(Duration::from_millis(1)),
                    compression: Some(Compression::Gzip),
                    batch_size_string: Some("44.0 MB".to_string()),
                    batch_size: Some(ByteSize::mb(44)),
                }),
                consumer: Some(ConsumerParameters {
                    partition: Some(10),
                }),
            },
            transforms: Some(
                TransformationStep {
                    uses: "infinyon/json-sql".to_string(),
                    with: BTreeMap::from([
                        (
                            "mapping".to_string(),
                            "{\"table\":\"topic_message\"}".into(),
                        ),
                        ("param".to_string(), "param_value".into()),
                    ]),
                }
                .into(),
            ),
        };

        //when
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/full-config.yaml")
            .expect("Failed to load test config");

        //then
        assert_eq!(connector_cfg, expected);
    }

    #[test]
    fn simple_yaml_test() {
        //given
        let expected = ConnectorConfig {
            meta: MetaConfig {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt".to_string(),
                topic: "my-mqtt".to_string(),
                version: "0.1.0".to_string(),
                producer: None,
                consumer: None,
            },
            transforms: None,
        };

        //when
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/simple.yaml")
            .expect("Failed to load test config");

        //then
        assert_eq!(connector_cfg, expected);
    }

    #[test]
    fn error_yaml_tests() {
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-linger.yaml")
            .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "meta.producer.linger: invalid value: string \"1\", expected a duration at line 8 column 13",
            format!("{connector_cfg:?}")
        );
        let connector_cfg =
            ConnectorConfig::from_file("test-data/connectors/error-compression.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!("meta.producer.compression: unknown variant `gzipaoeu`, expected one of `none`, `gzip`, `snappy`, `lz4` at line 8 column 18", format!("{connector_cfg:?}"));

        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-batchsize.yaml")
            .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "Fail to parse byte size couldn't parse \"aoeu\" into a known SI unit, couldn't parse unit of \"aoeu\"",
            format!("{connector_cfg:?}")
        );
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-version.yaml")
            .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "meta: missing field `version` at line 2 column 7",
            format!("{connector_cfg:?}")
        );
    }

    #[test]
    fn deserialize_test() {
        //given
        let yaml = r#"
            meta:
                name: kafka-out
                topic: poc1
                type: kafka-sink
                version: latest
            "#;

        let expected = ConnectorConfig {
            meta: MetaConfig {
                name: "kafka-out".to_string(),
                type_: "kafka-sink".to_string(),
                topic: "poc1".to_string(),
                version: "latest".to_string(),
                producer: None,
                consumer: None,
            },
            transforms: None,
        };

        //when
        let connector_spec: ConnectorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        //then
        assert_eq!(connector_spec, expected);
    }

    #[test]
    fn test_deserialize_transform() {
        //given

        //when
        let connector_spec: ConnectorConfig =
            ConnectorConfig::from_file("test-data/connectors/with_transform.yaml")
                .expect("Failed to deserialize");

        //then
        assert!(connector_spec.transforms.is_some());
        assert_eq!(
            connector_spec.transforms.as_ref().unwrap().transforms[0]
                .uses
                .as_str(),
            "infinyon/sql"
        );
        assert_eq!(connector_spec.transforms.as_ref().unwrap().transforms[0].with,
                       BTreeMap::from([("mapping".to_string(), "{\"map-columns\":{\"device_id\":{\"json-key\":\"device.device_id\",\"value\":{\"default\":0,\"required\":true,\"type\":\"int\"}},\"record\":{\"json-key\":\"$\",\"value\":{\"required\":true,\"type\":\"jsonb\"}}},\"table\":\"topic_message\"}".into())]));
    }

    #[test]
    fn test_deserialize_transform_many() {
        //given

        //when
        let connector_spec: ConnectorConfig =
            ConnectorConfig::from_file("test-data/connectors/with_transform_many.yaml")
                .expect("Failed to deserialize");

        //then
        assert!(connector_spec.transforms.is_some());
        let transform = connector_spec.transforms.unwrap().transforms;
        assert_eq!(transform.len(), 3);
        assert_eq!(transform[0].uses.as_str(), "infinyon/json-sql");
        assert_eq!(
            transform[0].with,
            BTreeMap::from([(
                "mapping".to_string(),
                "{\"table\":\"topic_message\"}".into()
            )])
        );
        assert_eq!(transform[1].uses.as_str(), "infinyon/avro-sql");
        assert_eq!(transform[1].with, BTreeMap::default());
        assert_eq!(transform[2].uses.as_str(), "infinyon/regex-filter");
        assert_eq!(
            transform[2].with,
            BTreeMap::from([("regex".to_string(), "\\w".into())])
        );
    }

    #[test]
    fn sample_yaml_test_files() {
        let testfiles = vec!["tests/sample-http.yaml", "tests/sample-mqtt.yaml"];

        for tfile in testfiles {
            let res = ConnectorConfig::from_file(tfile);
            assert!(res.is_ok(), "failed to load {tfile}");
            let connector_cfg = res.unwrap();
            println!("{tfile}: {connector_cfg:?}");
        }
    }
}
