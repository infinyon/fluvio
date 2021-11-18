#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};
//use std::collections::BTreeMap;


// This is what I put in spu schema. Should it go here instead?

//#[derive(Debug, Clone, Encoder, Decoder)]
//pub enum TableDataKind {
//    Json,
//    Yaml,
//    Toml,
//}
//
//impl Default for TableDataKind {
//    fn default() -> Self {
//        Self::Json
//    }
//}
//
//// Worry about this later
////pub enum DataFormat {
////    DateTime,
////}
//
//#[derive(Debug, Default, Clone, Encoder, Decoder)]
//pub struct TableColumn {
//    key_path: String,
//    primary_key: bool,
//    display: bool,
//    header_label: Option<String>,
//    alignment: Option<String>,
//    header_fg_color: Option<String>,
//    header_bg_color: Option<String>,
//    //width: Option<String>,
//    //format: Option<String>,
//}







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
pub enum DataFormat {
    JSON,
    //YAML,
    //TOML,
}

impl Default for DataFormat {
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
