#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};
//use std::collections::BTreeMap;

#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TableFormatSpec {
    pub name: String,
    pub input_format: Option<InputFormat>,
    pub columns: Option<Vec<TableFormatColumnConfig>>,
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
#[derive(Encoder, Decoder, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TableFormatColumnConfig {
    pub header_label: Option<String>,
    pub width: Option<String>,
    pub alignment: Option<TableFormatAlignment>,
    pub key_path: String,
    pub format: Option<String>,
    pub display: Option<bool>,
    pub primary_key: Option<bool>,
    pub header_bg_color: Option<Color>,
    pub header_text_color: Option<Color>,
}

#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "UPPERCASE")
)]
#[derive(Encoder, Decoder, Debug, PartialEq, Clone)]
pub enum TableFormatAlignment {
    Left,
    Right,
    Center,
}

impl Default for TableFormatAlignment {
    fn default() -> Self {
        Self::Center
    }
}

#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "UPPERCASE")
)]
#[derive(Encoder, Decoder, Debug, PartialEq, Clone)]
pub enum Color {
    Blue,
    Yellow,
    Green,
}

impl Default for Color {
    fn default() -> Self {
        Self::Blue
    }
}
