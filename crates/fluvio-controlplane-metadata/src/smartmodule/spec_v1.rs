use fluvio_protocol::{Encoder, Decoder};

use super::spec::SmartModuleWasm;

#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleSpecV1 {
    pub input_kind: SmartModuleInputKind,
    pub output_kind: SmartModuleOutputKind,
    pub source_code: Option<SmartModuleSourceCode>,
    pub wasm: SmartModuleWasm,
    pub parameters: Option<Vec<SmartModuleParameter>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleSourceCode {
    language: SmartModuleSourceCodeLanguage,
    payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Encoder, Decoder, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleSourceCodeLanguage {
    #[default]
    Rust,
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleInputKind {
    #[default]
    Stream,
    External,
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleOutputKind {
    #[default]
    Stream,
    External,
    Table,
}

impl std::fmt::Display for SmartModuleSpecV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SmartModuleSpec")
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleParameter {
    name: String,
}
