#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};
//use std::collections::BTreeMap;

#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TableSpec {
    pub name: String,
    pub input_format: InputFormat,
    //pub column: TableColumnConfig,
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub smartmodule: Option<String>,
}

#[derive(Encoder, Decoder, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InputFormat {
    JSON,
    YAML,
    TOML,
}

impl Default for InputFormat {
    fn default() -> Self {
        Self::JSON
    }
}
//pub struct TableColumnConfig {
//    pub label: String,
//    pub width: String,
//    pub alignment: String, // Can I enum this?
//    pub path: String,
//    pub format: String,
//}
