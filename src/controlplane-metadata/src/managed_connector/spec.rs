#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};
use fluvio_types::defaults::{SPU_LOG_BASE_DIR, SPU_LOG_SIZE};

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
    pub r#type: String, // syslog, github star, slack
    pub topic: String,
}

impl ManagedConnectorConfig {}
