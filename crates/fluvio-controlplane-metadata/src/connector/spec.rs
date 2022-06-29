#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder, Version,
};
use std::convert::Infallible;
use std::ops::Deref;
use std::str::FromStr;
use std::collections::BTreeMap;
use std::fmt;

use bytes::Buf;
use bytes::BufMut;

#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorSpec {
    pub name: String,

    pub version: Option<String>,

    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: String, // syslog, github star, slack

    pub topic: String,

    pub parameters: BTreeMap<String, ManageConnectorParameterValue>,

    pub secrets: BTreeMap<String, SecretString>,
}

#[test]
fn deserialize_test() {
    let yaml = r#"
name: kafka-out
parameters:
  param_1: "param_str"
  param_2:
   - item_1
   - item_2
   - "10"
  param_3:
    arg1: val1
    arg2: "10"
  param_4: "10"
secrets: {}
topic: poc1
type: kafka-sink
"#;
    let connector_spec: ManagedConnectorSpec =
        serde_yaml::from_str(&yaml).expect("Failed to deserialize");
    println!("{:?}", connector_spec);
}

#[derive(Encoder, Decoder, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase", untagged)
)]
pub enum ManageConnectorParameterValueInner {
    String(String),
    Vec(Vec<String>),
    Map(BTreeMap<String, String>),
}

impl Default for ManageConnectorParameterValueInner {
    fn default() -> Self {
        Self::Vec(Vec::new())
    }
}

#[derive(Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent),
)]
pub struct ManageConnectorParameterValue (
    pub ManageConnectorParameterValueInner
);

impl From<Vec<String>> for ManageConnectorParameterValue {
    fn from(vec: Vec<String>) -> ManageConnectorParameterValue {
        ManageConnectorParameterValue(ManageConnectorParameterValueInner::Vec(vec))
    }
}
impl From<String> for ManageConnectorParameterValue {
    fn from(inner: String) -> ManageConnectorParameterValue {
        ManageConnectorParameterValue(ManageConnectorParameterValueInner::String(inner))
    }
}

impl Decoder for ManageConnectorParameterValue {
    fn decode<T: Buf>(&mut self, src: &mut T, version: Version) -> Result<(), std::io::Error> {
        if version >= 8 {
            self.0.decode(src, version)
        } else {
            let mut new_string = String::new();
            new_string.decode(src, version)?;
            self.0 = ManageConnectorParameterValueInner::String(new_string);
            Ok(())
        }
    }
}
impl Encoder for ManageConnectorParameterValue {
    fn write_size(&self, version: Version) -> usize {
        if version >= 8 {
            self.0.write_size(version)
        } else {
            match &self.0 {
                ManageConnectorParameterValueInner::String(ref inner) => {
                    inner.write_size(version)
                },
                _ => {
                    String::new().write_size(version)
                }
            }
        }
    }

    /// encoding contents for buffer
    fn encode<T: BufMut>(&self, dest: &mut T, version: Version) -> Result<(), std::io::Error>
    {
        if version >= 8 {
            self.0.encode(dest, version)
        } else {
            match &self.0 {
                ManageConnectorParameterValueInner::String(ref inner) => {
                    inner.encode(dest, version)
                },
                _ => {
                    String::new().encode(dest, version)
                }
            }
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
