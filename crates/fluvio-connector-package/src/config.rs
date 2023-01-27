use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{PathBuf, Path};
use std::str::FromStr;
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

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, ConnectorParameterValue>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, SecretString>,

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
        let mut connector_config: Self = serde_yaml::from_str(&contents)?;
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

    pub fn consumer_parameters(&self) -> Vec<String> {
        let mut params = Vec::new();
        if let Some(consumer) = self.meta.consumer.as_ref() {
            if let Some(partition) = consumer.partition {
                params.push("--consumer-partition".to_string());
                params.push(format!("{partition}"));
            }
        }
        params
    }
    pub fn producer_parameters(&self) -> Vec<String> {
        let mut params = Vec::new();
        if let Some(producer) = self.meta.producer.as_ref() {
            if let Some(linger) = producer.linger {
                params.push("--producer-linger".to_string());

                params.push(format!("{}ms", linger.as_millis()));
            }
            if let Some(compression) = producer.compression {
                params.push("--producer-compression".to_string());
                params.push(compression.to_string());
            }
            if let Some(batch_size_string) = producer.batch_size_string.as_ref() {
                params.push("--producer-batch-size".to_string());
                params.push(batch_size_string.to_string());
            }
        }
        params
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

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Wrapper for string that does not reveal its internal
/// content in its display and debug implementation
pub struct SecretString(String);

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl FromStr for SecretString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.into()))
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl Deref for SecretString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ConnectorParameterValue {
    Vec(Vec<String>),
    Map(BTreeMap<String, String>),
    String(String),
}

impl Default for ConnectorParameterValue {
    fn default() -> Self {
        Self::Vec(Vec::new())
    }
}

impl From<String> for ConnectorParameterValue {
    fn from(str: String) -> Self {
        Self::String(str)
    }
}

impl From<&str> for ConnectorParameterValue {
    fn from(str: &str) -> Self {
        Self::String(str.to_string())
    }
}

impl From<Vec<String>> for ConnectorParameterValue {
    fn from(vec: Vec<String>) -> Self {
        Self::Vec(vec)
    }
}

impl From<BTreeMap<String, String>> for ConnectorParameterValue {
    fn from(map: BTreeMap<String, String>) -> Self {
        Self::Map(map)
    }
}

impl ConnectorParameterValue {
    pub fn as_string(&self) -> Result<String> {
        match self {
            Self::String(str) => Ok(str.to_owned()),
            _ => anyhow::bail!("Parameter value is not a string"),
        }
    }

    pub fn as_u32(&self) -> Result<u32> {
        self.as_string()?
            .parse::<u32>()
            .map_err(|err| anyhow::anyhow!("Fail to parse u32 {}", err))
    }
}

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::Deserializer;

struct ParameterValueVisitor;
impl<'de> Deserialize<'de> for ConnectorParameterValue {
    fn deserialize<D>(deserializer: D) -> Result<ConnectorParameterValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ParameterValueVisitor)
    }
}

impl<'de> Visitor<'de> for ParameterValueVisitor {
    type Value = ConnectorParameterValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string, map or sequence")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str("null")
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str("null")
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(ConnectorParameterValue::String(value.to_string()))
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut inner = BTreeMap::new();
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            inner.insert(key.clone(), value.clone());
        }

        Ok(ConnectorParameterValue::Map(inner))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec_inner = Vec::new();
        while let Some(param) = seq.next_element::<String>()? {
            vec_inner.push(param);
        }
        Ok(ConnectorParameterValue::Vec(vec_inner))
    }
}

#[cfg(test)]
mod tests {
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
                parameters: BTreeMap::from([
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
                ]),
                secrets: BTreeMap::from([("foo".to_string(), SecretString("bar".to_string()))]),
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
                parameters: BTreeMap::new(),
                secrets: BTreeMap::new(),
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
                parameters:
                  param_1: "param_str"
                  param_2:
                   - item_1
                   - item_2
                   - 10
                   - 10.0
                   - true
                   - On
                   - Off
                   - null
                  param_3:
                    arg1: val1
                    arg2: 10
                    arg3: -10
                    arg4: false
                    arg5: 1.0
                    arg6: null
                    arg7: On
                    arg8: Off
                  param_4: 10
                  param_5: 10.0
                  param_6: -10
                  param_7: True
                  param_8: 0xf1
                  param_9: null
                  param_10: 12.3015e+05
                  param_11: [On, Off]
                  param_12: true
                secrets: {}
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
                parameters: BTreeMap::from([
                    ("param_1".to_string(), "param_str".into()),
                    ("param_10".to_string(), "1230150".into()),
                    (
                        "param_11".to_string(),
                        vec!["On".to_string(), "Off".to_string()].into(),
                    ),
                    ("param_12".to_string(), "true".into()),
                    (
                        "param_2".to_string(),
                        vec![
                            "item_1".to_string(),
                            "item_2".to_string(),
                            "10".to_string(),
                            "10.0".to_string(),
                            "true".to_string(),
                            "On".to_string(),
                            "Off".to_string(),
                            "null".to_string(),
                        ]
                        .into(),
                    ),
                    (
                        "param_3".to_string(),
                        BTreeMap::from([
                            ("arg1".to_string(), "val1".to_string()),
                            ("arg2".to_string(), "10".to_string()),
                            ("arg3".to_string(), "-10".to_string()),
                            ("arg4".to_string(), "false".to_string()),
                            ("arg5".to_string(), "1.0".to_string()),
                            ("arg6".to_string(), "null".to_string()),
                            ("arg7".to_string(), "On".to_string()),
                            ("arg8".to_string(), "Off".to_string()),
                        ])
                        .into(),
                    ),
                    ("param_4".to_string(), "10".into()),
                    ("param_5".to_string(), "10".into()),
                    ("param_6".to_string(), "-10".into()),
                    ("param_7".to_string(), "True".into()),
                    ("param_8".to_string(), "241".into()),
                    ("param_9".to_string(), "null".into()),
                ]),
                secrets: BTreeMap::new(),
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
}
