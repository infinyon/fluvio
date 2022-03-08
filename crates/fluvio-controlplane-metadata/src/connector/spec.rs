#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};
use std::convert::Infallible;
use std::ops::Deref;
use std::str::FromStr;
use std::collections::BTreeMap;
use std::fmt;

#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorSpec {
    pub name: String,

    pub version: Option<String>,

    pub metadata: ManagedConnectorMetadata,

    pub topic: String,
    pub parameters: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretString>,
}

#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorMetadata {
    pub image: String,
    pub author: Option<String>,
    pub license: Option<String>,
}
impl FromStr for ManagedConnectorMetadata {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            image: s.to_string(),
            ..Default::default()
        })
    }
}

#[cfg(feature = "use_serde")]
mod deserialize_managed_connector {
    use super::*;
    use serde::de::{self, Visitor, MapAccess, Deserializer, Deserialize};
    struct ManagedConnectorMetadataVisitor;
    impl<'de> Visitor<'de> for ManagedConnectorMetadataVisitor
    {
        type Value = ManagedConnectorMetadata;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<ManagedConnectorMetadata, E>
        where
            E: de::Error,
        {
            Ok(FromStr::from_str(value).unwrap())
        }

        fn visit_map<M>(self, map: M) -> Result<ManagedConnectorMetadata, M::Error>
        where
            M: MapAccess<'de>,
        {
            // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
            // into a `Deserializer`, allowing it to be used as the input to T's
            // `Deserialize` implementation. T then deserializes itself using
            // the entries from the map visitor.
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }
    impl<'de> Deserialize<'de> for ManagedConnectorMetadata {
        fn deserialize<D>(deserializer: D) -> Result<ManagedConnectorMetadata, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(ManagedConnectorMetadataVisitor)
        }
    }
}

impl ManagedConnectorSpec {
    pub fn version(&self) -> String {
        self.version.clone().unwrap_or_else(|| "latest".to_string())
    }
}

#[derive(Encoder, Decoder, Default, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]

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
