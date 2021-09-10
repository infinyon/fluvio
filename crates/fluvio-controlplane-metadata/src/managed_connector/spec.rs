#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorSpec {
    pub name: String,
    pub config: ManagedConnectorConfig,
}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorConfig {
    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: String, // syslog, github star, slack
    pub topic: String,
    pub args: Vec<String>,
}

impl ManagedConnectorConfig {}
