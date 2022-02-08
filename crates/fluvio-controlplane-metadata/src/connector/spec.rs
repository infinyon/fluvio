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

    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: String, // syslog, github star, slack

    pub topic: String,
    pub parameters: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretString>,
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
        Ok(Self { 0: s.into() })
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self { 0: s }
    }
}

impl Deref for SecretString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
