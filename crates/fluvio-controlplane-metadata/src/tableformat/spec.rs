#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TableFormatSpec {
    pub name: String,
    pub input_format: Option<DataFormat>,
    pub columns: Option<Vec<TableFormatColumnConfig>>,
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub smartmodule: Option<String>,
}

impl TableFormatSpec {
    pub fn get_primary_keys(&self) -> Vec<String> {
        if let Some(columns) = &self.columns {
            let mut primary_keys = Vec::new();

            for c in columns {
                if let Some(is_primary_key) = c.primary_key {
                    if is_primary_key {
                        primary_keys.push(c.key_path.clone());
                    }
                }
            }

            primary_keys
        } else {
            Vec::new()
        }
    }
}

#[derive(Encoder, Decoder, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "UPPERCASE")
)]
pub enum DataFormat {
    #[fluvio(tag = 0)]
    JSON,
    //YAML,
    //TOML,
}

impl Default for DataFormat {
    fn default() -> Self {
        Self::JSON
    }
}
#[derive(Encoder, Decoder, Default, Debug, Eq, PartialEq, Clone)]
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
#[derive(Encoder, Decoder, Debug, Eq, PartialEq, Clone)]
pub enum TableFormatAlignment {
    #[fluvio(tag = 0)]
    Left,
    #[fluvio(tag = 1)]
    Right,
    #[fluvio(tag = 2)]
    Center,
}

impl Default for TableFormatAlignment {
    fn default() -> Self {
        Self::Center
    }
}

impl TableFormatColumnConfig {
    pub fn new(key_path: String) -> Self {
        Self {
            key_path,
            ..Default::default()
        }
    }

    pub fn with_primary_key(mut self, is_primary_key: Option<bool>) -> Self {
        self.primary_key = is_primary_key;
        self
    }

    pub fn with_display(mut self, do_display_column: Option<bool>) -> Self {
        self.display = do_display_column;
        self
    }

    pub fn with_header_label(mut self, header_label: Option<String>) -> Self {
        self.header_label = header_label;
        self
    }

    pub fn with_alignment(mut self, alignment: Option<TableFormatAlignment>) -> Self {
        self.alignment = alignment;
        self
    }

    pub fn with_header_text_color(mut self, header_text_color: Option<Color>) -> Self {
        self.header_text_color = header_text_color;
        self
    }

    pub fn with_header_bg_color(mut self, header_bg_color: Option<Color>) -> Self {
        self.header_bg_color = header_bg_color;
        self
    }
}

#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "UPPERCASE")
)]
#[derive(Encoder, Decoder, Debug, Eq, PartialEq, Clone)]
pub enum Color {
    #[fluvio(tag = 0)]
    Blue,
    #[fluvio(tag = 1)]
    Yellow,
    #[fluvio(tag = 2)]
    Green,
}

impl Default for Color {
    fn default() -> Self {
        Self::Blue
    }
}
