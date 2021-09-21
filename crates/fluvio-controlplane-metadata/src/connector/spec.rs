#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};
use std::collections::BTreeMap;

#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorSpec {
    pub name: String,

    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: String, // syslog, github star, slack

    pub topic: String,
    pub paramaters: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, String>,
}
