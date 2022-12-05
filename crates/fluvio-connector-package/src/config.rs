use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use tracing::debug;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use bytesize::ByteSize;

use fluvio_smartengine::transformation::TransformationConfig;
use fluvio_compression::Compression;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ConnectorConfig {
    pub name: String,

    #[serde(rename = "type")]
    pub type_: String,

    pub topic: String,

    pub version: String,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, ManagedConnectorParameterValue>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, SecretString>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerParameters>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<ConsumerParameters>,

    #[serde(default, flatten, skip_serializing_if = "Option::is_none")]
    pub transforms: Option<TransformationConfig>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ConsumerParameters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition: Option<i32>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
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

impl ConnectorConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self> {
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
                    .map_err(|err| anyhow::anyhow!("Fail to parse byte size {}", err))?;
                producer.batch_size = Some(batch_size);
            }
        }
        debug!("Using connector config {connector_config:#?}");
        Ok(connector_config)
    }

    pub fn consumer_parameters(&self) -> Vec<String> {
        let mut params = Vec::new();
        if let Some(consumer) = self.consumer.as_ref() {
            if let Some(partition) = consumer.partition {
                params.push("--consumer-partition".to_string());
                params.push(format!("{}", partition));
            }
        }
        params
    }
    pub fn producer_parameters(&self) -> Vec<String> {
        let mut params = Vec::new();
        if let Some(producer) = self.producer.as_ref() {
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
pub enum ManagedConnectorParameterValue {
    Vec(Vec<String>),
    Map(BTreeMap<String, String>),
    String(String),
}

impl Default for ManagedConnectorParameterValue {
    fn default() -> Self {
        Self::Vec(Vec::new())
    }
}

impl From<String> for ManagedConnectorParameterValue {
    fn from(str: String) -> Self {
        Self::String(str)
    }
}

impl From<&str> for ManagedConnectorParameterValue {
    fn from(str: &str) -> Self {
        Self::String(str.to_string())
    }
}

impl From<Vec<String>> for ManagedConnectorParameterValue {
    fn from(vec: Vec<String>) -> Self {
        Self::Vec(vec)
    }
}

impl From<BTreeMap<String, String>> for ManagedConnectorParameterValue {
    fn from(map: BTreeMap<String, String>) -> Self {
        Self::Map(map)
    }
}

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::Deserializer;
struct ParameterValueVisitor;
impl<'de> Deserialize<'de> for ManagedConnectorParameterValue {
    fn deserialize<D>(deserializer: D) -> Result<ManagedConnectorParameterValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ParameterValueVisitor)
    }
}

impl<'de> Visitor<'de> for ParameterValueVisitor {
    type Value = ManagedConnectorParameterValue;

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
        Ok(ManagedConnectorParameterValue::String(value.to_string()))
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut inner = BTreeMap::new();
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            inner.insert(key.clone(), value.clone());
        }

        Ok(ManagedConnectorParameterValue::Map(inner))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec_inner = Vec::new();
        while let Some(param) = seq.next_element::<String>()? {
            vec_inner.push(param);
        }
        Ok(ManagedConnectorParameterValue::Vec(vec_inner))
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
            name: "my-test-mqtt".to_string(),
            type_: "mqtt".to_string(),
            topic: "my-mqtt".to_string(),
            version: "0.1.0".to_string(),
            parameters: BTreeMap::new(),
            secrets: BTreeMap::new(),
            producer: None,
            consumer: None,
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
            "producer.linger: invalid value: string \"1\", expected a duration at line 7 column 11",
            format!("{:?}", connector_cfg)
        );
        let connector_cfg =
            ConnectorConfig::from_file("test-data/connectors/error-compression.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!("producer.compression: unknown variant `gzipaoeu`, expected one of `none`, `gzip`, `snappy`, `lz4` at line 7 column 16", format!("{:?}", connector_cfg));

        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-batchsize.yaml")
            .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "Fail to parse byte size couldn't parse \"aoeu\" into a known SI unit, couldn't parse unit of \"aoeu\"",
            format!("{:?}", connector_cfg)
        );
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-version.yaml")
            .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "missing field `version` at line 1 column 5",
            format!("{:?}", connector_cfg)
        );
    }

    #[test]
    fn deserialize_test() {
        //given
        let yaml = r#"
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
