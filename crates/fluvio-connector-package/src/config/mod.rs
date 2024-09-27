use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{PathBuf, Path};
use std::str::FromStr;
use std::time::Duration;

use serde::de::{Visitor, SeqAccess};
use serde::ser::{SerializeMap, SerializeSeq};
use tracing::debug;
use anyhow::Result;
use serde::{Deserialize, Serialize, Deserializer, Serializer};
pub use bytesize::ByteSize;

pub use fluvio_controlplane_metadata::topic::config as topic_config;
pub use fluvio_smartengine::transformation::TransformationStep;
pub use fluvio_types::PartitionId;
pub use fluvio_types::compression::Compression;

use crate::metadata::Direction;

pub use self::v1::ConnectorConfigV1;
pub use self::v1::MetaConfigV1;
pub use self::v2::ConnectorConfigV2;
pub use self::v2::MetaConfigV2;

mod bytesize_serde;

const SOURCE_SUFFIX: &str = "-source";
const IMAGE_PREFFIX: &str = "infinyon/fluvio-connect";

/// Versioned connector config
/// Use this config in the places where you need to enforce the version.
/// for example on the CLI create command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "apiVersion")]
pub enum ConnectorConfig {
    // V0 is the version of the config that was used before we introduced the versioning.
    #[serde(rename = "0.0.0")]
    V0_0_0(ConnectorConfigV1),
    #[serde(rename = "0.1.0")]
    V0_1_0(ConnectorConfigV1),
    #[serde(rename = "0.2.0")]
    V0_2_0(ConnectorConfigV2),
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        ConnectorConfig::V0_1_0(ConnectorConfigV1::default())
    }
}

mod serde_impl {
    use serde::Deserialize;

    use crate::config::{ConnectorConfigV1, ConnectorConfigV2};

    use super::ConnectorConfig;

    impl<'a> Deserialize<'a> for ConnectorConfig {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'a>,
        {
            #[derive(Deserialize)]
            enum Version {
                #[serde(rename = "0.0.0")]
                V0,
                #[serde(rename = "0.1.0")]
                V1,
                #[serde(rename = "0.2.0")]
                V2,
            }
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct VersionedConfig {
                api_version: Option<Version>,
                #[serde(flatten)]
                config: serde_yaml::Value,
            }
            let versioned_config: VersionedConfig = VersionedConfig::deserialize(deserializer)?;
            let version = versioned_config.api_version.unwrap_or(Version::V0);
            match version {
                Version::V0 => ConnectorConfigV1::deserialize(versioned_config.config)
                    .map(ConnectorConfig::V0_0_0)
                    .map_err(serde::de::Error::custom),

                Version::V1 => ConnectorConfigV1::deserialize(versioned_config.config)
                    .map(ConnectorConfig::V0_1_0)
                    .map_err(serde::de::Error::custom),

                Version::V2 => ConnectorConfigV2::deserialize(versioned_config.config)
                    .map(ConnectorConfig::V0_2_0)
                    .map_err(serde::de::Error::custom),
            }
        }
    }
}

mod v1 {
    use super::*;

    #[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
    pub struct ConnectorConfigV1 {
        pub meta: MetaConfigV1,

        #[serde(default)]
        pub transforms: Vec<TransformationStep>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
    pub struct MetaConfigV1 {
        pub name: String,

        #[serde(rename = "type")]
        pub type_: String,

        pub topic: String,

        pub version: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub producer: Option<ProducerParameters>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub consumer: Option<ConsumerParameters>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub secrets: Option<Vec<SecretConfig>>,
    }

    impl MetaConfigV1 {
        pub fn secrets(&self) -> HashSet<SecretConfig> {
            HashSet::from_iter(self.secrets.clone().unwrap_or_default())
        }

        pub fn direction(&self) -> Direction {
            if self.type_.ends_with(SOURCE_SUFFIX) {
                Direction::source()
            } else {
                Direction::dest()
            }
        }

        pub fn image(&self) -> String {
            format!("{}-{}:{}", IMAGE_PREFFIX, self.type_, self.version)
        }
    }
}

mod v2 {
    use fluvio_controlplane_metadata::topic::config::TopicConfig;

    use super::*;

    #[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
    pub struct ConnectorConfigV2 {
        pub meta: MetaConfigV2,

        #[serde(default)]
        pub transforms: Vec<TransformationStep>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
    pub struct MetaConfigV2 {
        pub name: String,

        #[serde(rename = "type")]
        pub type_: String,

        pub topic: TopicConfig,

        pub version: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub producer: Option<ProducerParameters>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub consumer: Option<ConsumerParameters>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub secrets: Option<Vec<SecretConfig>>,
    }

    impl MetaConfigV2 {
        pub fn secrets(&self) -> HashSet<SecretConfig> {
            HashSet::from_iter(self.secrets.clone().unwrap_or_default())
        }

        pub fn direction(&self) -> Direction {
            if self.type_.ends_with(SOURCE_SUFFIX) {
                Direction::source()
            } else {
                Direction::dest()
            }
        }

        pub fn image(&self) -> String {
            format!("{}-{}:{}", IMAGE_PREFFIX, self.type_, self.version)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaConfig<'a> {
    V0_1_0(&'a MetaConfigV1),
    V0_2_0(&'a MetaConfigV2),
}

impl MetaConfig<'_> {
    fn direction(&self) -> Direction {
        match self {
            MetaConfig::V0_1_0(inner) => inner.direction(),
            MetaConfig::V0_2_0(inner) => inner.direction(),
        }
    }

    pub fn image(&self) -> String {
        match self {
            MetaConfig::V0_1_0(inner) => inner.image(),
            MetaConfig::V0_2_0(inner) => inner.image(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            MetaConfig::V0_1_0(inner) => &inner.name,
            MetaConfig::V0_2_0(inner) => &inner.name,
        }
    }

    pub fn type_(&self) -> &str {
        match self {
            MetaConfig::V0_1_0(inner) => &inner.type_,
            MetaConfig::V0_2_0(inner) => &inner.type_,
        }
    }

    pub fn topic(&self) -> &str {
        match self {
            MetaConfig::V0_1_0(inner) => &inner.topic,
            MetaConfig::V0_2_0(inner) => &inner.topic.meta.name,
        }
    }

    pub fn version(&self) -> &str {
        match self {
            MetaConfig::V0_1_0(inner) => &inner.version,
            MetaConfig::V0_2_0(inner) => &inner.version,
        }
    }

    pub fn consumer(&self) -> Option<&ConsumerParameters> {
        match self {
            MetaConfig::V0_1_0(inner) => inner.consumer.as_ref(),
            MetaConfig::V0_2_0(inner) => inner.consumer.as_ref(),
        }
    }

    pub fn producer(&self) -> Option<&ProducerParameters> {
        match self {
            MetaConfig::V0_1_0(inner) => inner.producer.as_ref(),
            MetaConfig::V0_2_0(inner) => inner.producer.as_ref(),
        }
    }

    pub fn topic_config(&self) -> Option<&topic_config::TopicConfig> {
        match self {
            MetaConfig::V0_1_0(_) => None,
            MetaConfig::V0_2_0(inner) => Some(&inner.topic),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ConsumerParameters {
    #[serde(default)]
    pub partition: ConsumerPartitionConfig,
    #[serde(
        with = "bytesize_serde",
        skip_serializing_if = "Option::is_none",
        default,
        alias = "max_bytes"
    )]
    pub max_bytes: Option<ByteSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<ConsumerOffsetConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProducerParameters {
    #[serde(with = "humantime_serde")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linger: Option<Duration>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,

    #[serde(
        rename = "batch-size",
        alias = "batch_size",
        with = "bytesize_serde",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub batch_size: Option<ByteSize>,
}
#[derive(Default, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct SecretConfig {
    /// The name of the secret. It can only contain alphanumeric ASCII characters and underscores. It cannot start with a number.
    name: SecretName,
}

impl SecretConfig {
    pub fn name(&self) -> &str {
        &self.name.inner
    }

    pub fn new(secret_name: SecretName) -> Self {
        Self { name: secret_name }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretName {
    inner: String,
}

impl SecretName {
    fn validate(&self) -> anyhow::Result<()> {
        if self.inner.chars().count() == 0 {
            return Err(anyhow::anyhow!("Secret name cannot be empty"));
        }
        if !self
            .inner
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(anyhow::anyhow!(
                "Secret name `{}` can only contain alphanumeric ASCII characters and underscores",
                self.inner
            ));
        }
        if self.inner.chars().next().unwrap().is_ascii_digit() {
            return Err(anyhow::anyhow!(
                "Secret name `{}` cannot start with a number",
                self.inner
            ));
        }
        Ok(())
    }
}
impl FromStr for SecretName {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let secret_name = Self {
            inner: value.into(),
        };
        secret_name.validate()?;
        Ok(secret_name)
    }
}

impl Deref for SecretName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> Deserialize<'a> for SecretName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let inner = String::deserialize(deserializer)?;
        let secret = Self { inner };
        secret.validate().map_err(serde::de::Error::custom)?;
        Ok(secret)
    }
}

impl Serialize for SecretName {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.inner)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerPartitionConfig {
    All,
    One(PartitionId),
    Many(Vec<PartitionId>),
}

impl Default for ConsumerPartitionConfig {
    fn default() -> Self {
        Self::All
    }
}

struct PartitionConfigVisitor;
impl<'de> Visitor<'de> for PartitionConfigVisitor {
    type Value = ConsumerPartitionConfig;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("integer, sequence of integers or `all` string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.eq("all") {
            Ok(ConsumerPartitionConfig::All)
        } else {
            Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(v),
                &self,
            ))
        }
    }

    fn visit_u32<E>(self, v: u32) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(ConsumerPartitionConfig::One(v))
    }

    fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let partition = PartitionId::try_from(v).map_err(E::custom)?;
        Ok(ConsumerPartitionConfig::One(partition))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut partitions = Vec::with_capacity(seq.size_hint().unwrap_or(2));
        while let Some(next) = seq.next_element()? {
            partitions.push(next);
        }
        Ok(ConsumerPartitionConfig::Many(partitions))
    }
}

impl<'de> Deserialize<'de> for ConsumerPartitionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(PartitionConfigVisitor)
    }
}

impl Serialize for ConsumerPartitionConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ConsumerPartitionConfig::All => serializer.serialize_str("all"),
            ConsumerPartitionConfig::One(partition) => serializer.serialize_u32(*partition),
            ConsumerPartitionConfig::Many(partitions) => {
                let mut seq_serializer = serializer.serialize_seq(Some(partitions.len()))?;
                for p in partitions {
                    seq_serializer.serialize_element(p)?;
                }
                seq_serializer.end()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ConsumerOffsetConfig {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub start: Option<OffsetConfig>,
    pub strategy: OffsetStrategyConfig,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub flush_period: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetConfig {
    Absolute(i64),
    Beginning,
    FromBeginning(u32),
    End,
    FromEnd(u32),
}

impl Serialize for OffsetConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            OffsetConfig::Absolute(abs) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("absolute", abs)?;
                map.end()
            }
            OffsetConfig::Beginning => serializer.serialize_str("beginning"),
            OffsetConfig::FromBeginning(offset) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("from-beginning", offset)?;
                map.end()
            }
            OffsetConfig::End => serializer.serialize_str("end"),
            OffsetConfig::FromEnd(offset) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("from-end", offset)?;
                map.end()
            }
        }
    }
}

struct OffsetConfigVisitor;
impl<'de> Visitor<'de> for OffsetConfigVisitor {
    type Value = OffsetConfig;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("strings \"beginning\", \"end\" or map keys \"absolute\", \"from-beginning\", \"from-end\"")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v {
            "beginning" => Ok(OffsetConfig::Beginning),
            "end" => Ok(OffsetConfig::End),
            other => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(other),
                &self,
            )),
        }
    }

    fn visit_map<A>(self, mut map: A) -> std::prelude::v1::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let key = map.next_key::<String>()?;
        match key.as_deref() {
            Some("absolute") => Ok(OffsetConfig::Absolute(map.next_value()?)),
            Some("from-beginning") => Ok(OffsetConfig::FromBeginning(map.next_value()?)),
            Some("from-end") => Ok(OffsetConfig::FromEnd(map.next_value()?)),
            Some(other) => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(other),
                &self,
            )),
            None => Err(serde::de::Error::custom("expected a map entry")),
        }
    }
}

impl<'de> Deserialize<'de> for OffsetConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(OffsetConfigVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OffsetStrategyConfig {
    None,
    Manual,
    Auto,
}

impl ConnectorConfig {
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        Self::config_from_str(&contents)
    }

    /// Only parses the meta section of the config
    pub fn config_from_str(config_str: &str) -> Result<Self> {
        let connector_config: Self = serde_yaml::from_str(config_str)?;
        connector_config.validate_secret_names()?;

        debug!("Using connector config {connector_config:#?}");
        Ok(connector_config)
    }

    fn validate_secret_names(&self) -> Result<()> {
        for secret in self.secrets() {
            secret.name.validate()?;
        }
        Ok(())
    }

    pub fn meta(&self) -> MetaConfig {
        match self {
            Self::V0_0_0(inner) => MetaConfig::V0_1_0(&inner.meta),
            Self::V0_1_0(inner) => MetaConfig::V0_1_0(&inner.meta),
            Self::V0_2_0(inner) => MetaConfig::V0_2_0(&inner.meta),
        }
    }

    pub fn from_value(value: serde_yaml::Value) -> Result<Self> {
        let connector_config: Self = serde_yaml::from_value(value)?;
        connector_config.validate_secret_names()?;

        debug!("Using connector config {connector_config:#?}");
        Ok(connector_config)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        std::fs::write(path, serde_yaml::to_string(self)?)?;
        Ok(())
    }

    pub fn secrets(&self) -> HashSet<SecretConfig> {
        match self {
            Self::V0_0_0(_) => Default::default(),
            Self::V0_1_0(config) => config.meta.secrets(),
            Self::V0_2_0(config) => config.meta.secrets(),
        }
    }

    pub fn transforms(&self) -> Vec<TransformationStep> {
        match self {
            Self::V0_0_0(config) => config.transforms.clone(),
            Self::V0_1_0(config) => config.transforms.clone(),
            Self::V0_2_0(config) => config.transforms.clone(),
        }
    }

    pub fn direction(&self) -> Direction {
        self.meta().direction()
    }

    pub fn image(&self) -> String {
        self.meta().image()
    }

    pub fn name(&self) -> String {
        self.meta().name().to_string()
    }

    pub fn version(&self) -> String {
        self.meta().version().to_string()
    }

    pub fn r#type(&self) -> String {
        self.meta().type_().to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use fluvio_controlplane_metadata::topic::{
        config::{
            TopicConfigBuilder, MetaConfigBuilder, MetaConfig, PartitionConfig, RetentionConfig,
            CompressionConfig,
        },
        CompressionAlgorithm, Deduplication, Bounds, Filter, Transform,
    };
    use fluvio_smartengine::transformation::{TransformationStep, Lookback};
    use pretty_assertions::assert_eq;

    #[test]
    fn full_yaml_test() {
        //given
        let expected = ConnectorConfig::V0_1_0(ConnectorConfigV1 {
            meta: MetaConfigV1 {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt".to_string(),
                topic: "my-mqtt".to_string(),
                version: "0.1.0".to_string(),
                producer: Some(ProducerParameters {
                    linger: Some(Duration::from_millis(1)),
                    compression: Some(Compression::Gzip),
                    batch_size: Some(ByteSize::mb(44)),
                }),
                consumer: Some(ConsumerParameters {
                    partition: ConsumerPartitionConfig::One(10),
                    max_bytes: Some(ByteSize::mb(1)),
                    id: None,
                    offset: None,
                }),
                secrets: Some(vec![SecretConfig {
                    name: "secret1".parse().unwrap(),
                }]),
            },
            transforms: vec![TransformationStep {
                uses: "infinyon/json-sql".to_string(),
                lookback: None,
                with: BTreeMap::from([
                    (
                        "mapping".to_string(),
                        "{\"table\":\"topic_message\"}".into(),
                    ),
                    ("param".to_string(), "param_value".into()),
                ]),
            }],
        });

        //when
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/full-config.yaml")
            .expect("Failed to load test config");

        //then
        assert_eq!(connector_cfg, expected);
        assert!(connector_cfg.meta().topic_config().is_none());
    }

    #[test]
    fn deserialize_full_config_v2() {
        //given
        let expected = ConnectorConfig::V0_2_0(ConnectorConfigV2 {
            meta: MetaConfigV2 {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt".to_string(),
                topic: topic_config::TopicConfig {
                    version: "0.1.0".to_string(),
                    meta: MetaConfig {
                        name: "test-topic".to_string(),
                    },
                    partition: PartitionConfig {
                        count: Some(3),
                        max_size: Some(bytesize::ByteSize(1000)),
                        replication: Some(2),
                        ignore_rack_assignment: Some(true),
                        maps: None,
                    },
                    retention: RetentionConfig {
                        time: Some(Duration::from_secs(120)),
                        segment_size: Some(bytesize::ByteSize(2000)),
                    },
                    compression: CompressionConfig {
                        type_: CompressionAlgorithm::Lz4,
                    },
                    deduplication: Some(Deduplication {
                        bounds: Bounds {
                            count: 100,
                            age: Some(Duration::from_secs(60)),
                        },
                        filter: Filter {
                            transform: Transform {
                                uses: "infinyon/fluvio-smartmodule-filter-lookback@0.1.0"
                                    .to_string(),
                                with: Default::default(),
                            },
                        },
                    }),
                },
                version: "0.1.0".to_string(),
                producer: Some(ProducerParameters {
                    linger: Some(Duration::from_millis(1)),
                    compression: Some(Compression::Gzip),
                    batch_size: Some(ByteSize::mb(44)),
                }),
                consumer: Some(ConsumerParameters {
                    partition: ConsumerPartitionConfig::One(10),
                    max_bytes: Some(ByteSize::mb(1)),
                    id: Some("consumer_id_1".to_string()),
                    offset: Some(ConsumerOffsetConfig {
                        start: Some(OffsetConfig::Absolute(100)),
                        strategy: OffsetStrategyConfig::Auto,
                        flush_period: Some(Duration::from_secs(160)),
                    }),
                }),
                secrets: Some(vec![SecretConfig {
                    name: "secret1".parse().unwrap(),
                }]),
            },
            transforms: vec![TransformationStep {
                uses: "infinyon/json-sql".to_string(),
                lookback: None,
                with: BTreeMap::from([
                    (
                        "mapping".to_string(),
                        "{\"table\":\"topic_message\"}".into(),
                    ),
                    ("param".to_string(), "param_value".into()),
                ]),
            }],
        });

        //when
        let connector_cfg = ConnectorConfig::from_file("test-data/connectors/full-config-v2.yaml")
            .expect("Failed to load test config");

        //then
        assert_eq!(connector_cfg, expected);
    }

    #[test]
    fn simple_yaml_test() {
        //given
        let expected = ConnectorConfig::V0_1_0(ConnectorConfigV1 {
            meta: MetaConfigV1 {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt".to_string(),
                topic: "my-mqtt".to_string(),
                version: "0.1.0".to_string(),
                producer: None,
                consumer: None,
                secrets: None,
            },
            transforms: Vec::default(),
        });

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
            "invalid value: string \"1\", expected a duration",
            format!("{connector_cfg}")
        );
        let connector_cfg_err =
            ConnectorConfig::from_file("test-data/connectors/error-compression.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "unknown variant `gzipaoeu`, expected one of `none`, `gzip`, `snappy`, `lz4`, `zstd`",
            format!("{connector_cfg_err}")
        );

        let connector_cfg_err =
            ConnectorConfig::from_file("test-data/connectors/error-batchsize.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "invalid value: string \"1aoeu\", expected parsable string",
            format!("{connector_cfg_err}")
        );
        let connector_cfg_err =
            ConnectorConfig::from_file("test-data/connectors/error-version.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!("missing field `version`", format!("{connector_cfg_err}"));

        let connector_cfg_err =
            ConnectorConfig::from_file("test-data/connectors/error-secret-with-spaces.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "Secret name `secret name` can only contain alphanumeric ASCII characters and underscores",
            format!("{connector_cfg_err}")
        );

        let connector_cfg_err =
            ConnectorConfig::from_file("test-data/connectors/error-secret-starts-with-number.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "Secret name `1secret` cannot start with a number",
            format!("{connector_cfg_err}")
        );

        let connector_cfg_err =
            ConnectorConfig::from_file("test-data/connectors/error-invalid-api-version.yaml")
                .expect_err("This yaml should error");
        #[cfg(unix)]
        assert_eq!(
            "apiVersion: unknown variant `v1`, expected one of `0.0.0`, `0.1.0`, `0.2.0` at line 1 column 13",
            format!("{connector_cfg_err}")
        );
    }

    #[test]
    fn deserialize_test() {
        //given
        let yaml = r#"
            apiVersion: 0.1.0
            meta:
                name: kafka-out
                topic: poc1
                type: kafka-sink
                version: latest
            "#;

        let expected = ConnectorConfig::V0_1_0(ConnectorConfigV1 {
            meta: MetaConfigV1 {
                name: "kafka-out".to_string(),
                type_: "kafka-sink".to_string(),
                topic: "poc1".to_string(),
                version: "latest".to_string(),
                producer: None,
                consumer: None,
                secrets: None,
            },
            transforms: Vec::default(),
        });

        //when
        let connector_spec: ConnectorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        //then
        assert_eq!(connector_spec, expected);
    }

    #[test]
    fn deserialize_test_untagged() {
        //given
        let yaml = r#"
            meta:
                name: kafka-out
                topic: poc1
                type: kafka-sink
                version: latest
            "#;

        let expected = ConnectorConfig::V0_0_0(ConnectorConfigV1 {
            meta: MetaConfigV1 {
                name: "kafka-out".to_string(),
                type_: "kafka-sink".to_string(),
                topic: "poc1".to_string(),
                version: "latest".to_string(),
                producer: None,
                consumer: None,
                secrets: None,
            },
            transforms: Vec::default(),
        });

        //when
        let connector_spec: ConnectorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        //then
        assert_eq!(connector_spec, expected);
    }

    #[test]
    fn deserialize_with_integer_batch_size() {
        //given
        let yaml = r#"
        apiVersion: 0.1.0
        meta:
          version: 0.1.0
          name: my-test-mqtt
          type: mqtt-source
          topic: my-mqtt
          consumer:
            max_bytes: 1400
          producer:
            batch_size: 1600
        "#;

        let expected = ConnectorConfig::V0_1_0(ConnectorConfigV1 {
            meta: MetaConfigV1 {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt-source".to_string(),
                topic: "my-mqtt".to_string(),
                version: "0.1.0".to_string(),
                producer: Some(ProducerParameters {
                    linger: None,
                    compression: None,
                    batch_size: Some(ByteSize::b(1600)),
                }),
                consumer: Some(ConsumerParameters {
                    max_bytes: Some(ByteSize::b(1400)),
                    partition: Default::default(),
                    id: None,
                    offset: None,
                }),
                secrets: None,
            },
            transforms: Vec::default(),
        });

        //when
        let connector_spec: ConnectorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        //then
        assert_eq!(connector_spec, expected);
    }

    #[test]
    fn deserialize_with_topic_config() {
        //given
        let yaml = r#"
        apiVersion: 0.2.0
        meta:
          version: 0.1.0
          name: my-test-mqtt
          type: mqtt-source
          topic: 
            meta:
              name: my-mqtt
        "#;

        let expected = ConnectorConfig::V0_2_0(ConnectorConfigV2 {
            meta: MetaConfigV2 {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt-source".to_string(),
                topic: TopicConfigBuilder::default()
                    .meta(
                        MetaConfigBuilder::default()
                            .name("my-mqtt".to_string())
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
                version: "0.1.0".to_string(),
                producer: None,
                consumer: None,
                secrets: None,
            },
            transforms: Vec::default(),
        });

        //when
        let connector_spec: ConnectorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        //then
        assert_eq!(connector_spec, expected);
        assert_eq!(connector_spec.meta().topic(), "my-mqtt");
        assert_eq!(
            connector_spec
                .meta()
                .topic_config()
                .map(|c| c.meta.name.as_str()),
            Some("my-mqtt")
        );
    }

    #[test]
    fn test_deserialize_transform() {
        //given

        //when
        let connector_spec: ConnectorConfig =
            ConnectorConfig::from_file("test-data/connectors/with_transform.yaml")
                .expect("Failed to deserialize");

        //then
        assert_eq!(connector_spec.transforms().len(), 1);
        assert_eq!(connector_spec.transforms()[0].uses.as_str(), "infinyon/sql");
        assert_eq!(
            connector_spec.transforms()[0].lookback,
            Some(Lookback {
                last: 100,
                age: Some(Duration::from_secs(3600))
            })
        );

        assert_eq!(connector_spec.transforms()[0].with,
                       BTreeMap::from([("mapping".to_string(), "{\"map-columns\":{\"device_id\":{\"json-key\":\"device.device_id\",\"value\":{\"default\":0,\"required\":true,\"type\":\"int\"}},\"record\":{\"json-key\":\"$\",\"value\":{\"required\":true,\"type\":\"jsonb\"}}},\"table\":\"topic_message\"}".into())]));
    }

    #[test]
    fn test_deserialize_secret_name() {
        let secret_name: SecretName = serde_yaml::from_str("secret_name").unwrap();
        assert_eq!("secret_name", &*secret_name);

        assert!(
            serde_yaml::from_str::<SecretName>("secret name").is_err(),
            "string with space is not a valid secret name"
        );
    }

    #[test]
    fn test_serialize_secret_config() {
        let secrets = ["secret_name", "secret_name2"];
        let secret_configs = secrets
            .iter()
            .map(|s| SecretConfig::new(s.parse().unwrap()))
            .collect::<Vec<_>>();

        let serialized = serde_yaml::to_string(&secret_configs).expect("failed to serialize");
        assert_eq!(
            "- name: secret_name
- name: secret_name2
",
            serialized
        );
    }

    #[test]
    fn test_parse_secret_name() {
        let secret_name: SecretName = "secret_name".parse().unwrap();
        assert_eq!("secret_name", &*secret_name);

        assert!(
            "secret name".parse::<SecretName>().is_err(),
            "secret name should fail if has space"
        );

        assert!(
            "1secretname".parse::<SecretName>().is_err(),
            "secret name should fail if starts with number"
        );
        assert!(
            "secret-name".parse::<SecretName>().is_err(),
            "secret name should fail if has dash"
        );
    }

    #[test]
    fn test_serialize_secret_name() {
        let secret_name: SecretName = "secret_name".parse().unwrap();
        assert_eq!("secret_name", &*secret_name);

        let secret_name: SecretName = serde_yaml::from_str("secret_name").unwrap();
        assert_eq!("secret_name", &*secret_name);
    }

    #[test]
    fn test_deserialize_transform_many() {
        //given

        //when
        let connector_spec: ConnectorConfig =
            ConnectorConfig::from_file("test-data/connectors/with_transform_many.yaml")
                .expect("Failed to deserialize");

        //then
        let transform = &connector_spec.transforms();

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
    fn test_deserialize_transform_invalid() {
        let connector_spec =
            ConnectorConfig::from_file("test-data/connectors/with_transform_invalid.yaml");

        assert!(
            connector_spec.is_err(),
            "should fail to deserialize into `ConnectorConfig`"
        );

        let err = connector_spec.unwrap_err();

        assert!(err
            .to_string()
            .contains("mapping values are not allowed in this context at line 16 column 17"));
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

    #[test]
    fn retrieves_name_and_version() {
        let have = ConnectorConfig::V0_1_0(ConnectorConfigV1 {
            meta: MetaConfigV1 {
                name: "my-test-mqtt".to_string(),
                type_: "mqtt-source".to_string(),
                topic: "my-mqtt".to_string(),
                version: "0.1.0".to_string(),
                producer: Some(ProducerParameters {
                    linger: None,
                    compression: None,
                    batch_size: Some(ByteSize::b(1600)),
                }),
                consumer: Some(ConsumerParameters {
                    max_bytes: Some(ByteSize::b(1400)),
                    partition: Default::default(),
                    id: None,
                    offset: None,
                }),
                secrets: None,
            },
            transforms: Vec::default(),
        });

        assert_eq!(have.name(), "my-test-mqtt");
        assert_eq!(have.version(), "0.1.0");
        assert_eq!(have.r#type(), "mqtt-source");
    }

    #[test]
    fn test_deser_partition_config() {
        //when

        let with_one_partition: ConsumerParameters = serde_yaml::from_str(
            r#"
            partition: 1
        "#,
        )
        .expect("one partition");

        let with_multiple_partitions: ConsumerParameters = serde_yaml::from_str(
            r#"
            partition: 
                - 120
                - 230
        "#,
        )
        .expect("sequence of partitions");

        let with_all_partitions: ConsumerParameters = serde_yaml::from_str(
            r#"
            partition: all
        "#,
        )
        .expect("all partitions");

        let connector_cfg_all_partitions =
            ConnectorConfig::from_file("test-data/connectors/all-partitions.yaml")
                .expect("Failed to load test config");

        let connector_cfg_many_partitions =
            ConnectorConfig::from_file("test-data/connectors/many-partitions.yaml")
                .expect("Failed to load test config");

        //then
        assert_eq!(
            with_one_partition.partition,
            ConsumerPartitionConfig::One(1)
        );

        assert_eq!(
            with_multiple_partitions.partition,
            ConsumerPartitionConfig::Many(vec![120, 230])
        );

        assert_eq!(with_all_partitions.partition, ConsumerPartitionConfig::All);

        assert_eq!(
            connector_cfg_all_partitions
                .meta()
                .consumer()
                .unwrap()
                .partition,
            ConsumerPartitionConfig::All
        );

        assert_eq!(
            connector_cfg_many_partitions
                .meta()
                .consumer()
                .unwrap()
                .partition,
            ConsumerPartitionConfig::Many(vec![0, 1])
        );
    }

    #[test]
    fn test_ser_partition_config() {
        //given
        let one = ConsumerParameters {
            partition: ConsumerPartitionConfig::One(1),
            max_bytes: Default::default(),
            id: None,
            offset: None,
        };
        let many = ConsumerParameters {
            partition: ConsumerPartitionConfig::Many(vec![2, 3]),
            max_bytes: Default::default(),
            id: None,
            offset: None,
        };

        let all = ConsumerParameters {
            partition: ConsumerPartitionConfig::All,
            max_bytes: Default::default(),
            id: None,
            offset: None,
        };

        //when
        let one_ser = serde_yaml::to_string(&one).expect("one");
        let many_ser = serde_yaml::to_string(&many).expect("many");
        let all_ser = serde_yaml::to_string(&all).expect("all");

        //then
        assert_eq!(one_ser, "partition: 1\n");
        assert_eq!(many_ser, "partition:\n- 2\n- 3\n");
        assert_eq!(all_ser, "partition: all\n");
    }

    #[test]
    fn test_ser_offset_config() {
        //given
        let absolute = OffsetConfig::Absolute(10);
        let beginning = OffsetConfig::Beginning;
        let from_beginning = OffsetConfig::FromBeginning(5);
        let end = OffsetConfig::End;
        let from_end = OffsetConfig::FromEnd(12);

        //when
        let absolute_ser = serde_yaml::to_string(&absolute).expect("absolute");
        let beginning_ser = serde_yaml::to_string(&beginning).expect("beginning");
        let from_beginning_ser = serde_yaml::to_string(&from_beginning).expect("from_beginning");
        let end_ser = serde_yaml::to_string(&end).expect("end");
        let from_end_ser = serde_yaml::to_string(&from_end).expect("from_end");

        //then
        assert_eq!(absolute_ser, "absolute: 10\n");
        assert_eq!(beginning_ser, "beginning\n");
        assert_eq!(from_beginning_ser, "from-beginning: 5\n");
        assert_eq!(end_ser, "end\n");
        assert_eq!(from_end_ser, "from-end: 12\n");
    }

    #[test]
    fn test_deser_offset_config() {
        //given
        //when
        let absolute: OffsetConfig = serde_yaml::from_str(
            r#"
            absolute: 11
        "#,
        )
        .expect("absolute");
        let beginning: OffsetConfig = serde_yaml::from_str(
            r#"
            beginning
        "#,
        )
        .expect("beginning");
        let end: OffsetConfig = serde_yaml::from_str(
            r#"
            end
        "#,
        )
        .expect("end");
        let from_end: OffsetConfig = serde_yaml::from_str(
            r#"
            from-end: 12
        "#,
        )
        .expect("from end");
        let from_beginning: OffsetConfig = serde_yaml::from_str(
            r#"
            from-beginning: 14
        "#,
        )
        .expect("from beginning");

        //then
        assert_eq!(absolute, OffsetConfig::Absolute(11));
        assert_eq!(beginning, OffsetConfig::Beginning);
        assert_eq!(end, OffsetConfig::End);
        assert_eq!(from_end, OffsetConfig::FromEnd(12));
        assert_eq!(from_beginning, OffsetConfig::FromBeginning(14));
    }

    #[test]
    fn test_ser_consumer_offset_config() {
        //given
        let config = ConsumerOffsetConfig {
            start: Some(OffsetConfig::Absolute(10)),
            strategy: OffsetStrategyConfig::Manual,
            flush_period: Some(Duration::from_secs(60)),
        };

        //when
        let config_ser = serde_yaml::to_string(&config).expect("config");

        //then
        assert_eq!(
            config_ser,
            "start:\n  absolute: 10\nstrategy: manual\nflush-period:\n  secs: 60\n  nanos: 0\n"
        );
    }

    #[test]
    fn test_deser_consumer_offset_config() {
        //given
        //when
        let config: ConsumerOffsetConfig = serde_yaml::from_str(
            r#"
            start:
              absolute: 11
            strategy: auto
            flush-period:
              secs: 160
              nanos: 0
        "#,
        )
        .expect("config");

        //then
        assert_eq!(
            config,
            ConsumerOffsetConfig {
                start: Some(OffsetConfig::Absolute(11)),
                strategy: OffsetStrategyConfig::Auto,
                flush_period: Some(Duration::from_secs(160))
            }
        );
    }
}
