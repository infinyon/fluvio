//!
//! # SmartModule Spec
//!

use std::collections::{BTreeMap};

use dataplane::core::{Encoder, Decoder};

#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleSpec {
    pub package: Option<SmartModulePackage>,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub init_params: BTreeMap<String, SmartModuleInitParams>,
    pub input_kind: SmartModuleInputKind,
    pub output_kind: SmartModuleOutputKind,
    pub source_code: Option<SmartModuleSourceCode>,
    pub wasm: SmartModuleWasm,
    #[deprecated(
        since = "0.17.3",
        note = "Use `package` instead. This field will be removed in 0.18.0"
    )]
    pub parameters: Option<Vec<SmartModuleParameter>>,
}


#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModulePackage {
    pub name: String,
    pub group: String,
    pub version: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleInitParams {
    params: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleSourceCode {
    language: SmartModuleSourceCodeLanguage,
    payload: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleSourceCodeLanguage {
    Rust,
}

impl Default for SmartModuleSourceCodeLanguage {
    fn default() -> SmartModuleSourceCodeLanguage {
        SmartModuleSourceCodeLanguage::Rust
    }
}

#[derive(Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleWasm {
    pub format: SmartModuleWasmFormat,
    #[cfg_attr(feature = "use_serde", serde(with = "base64"))]
    pub payload: Vec<u8>,
}
impl std::fmt::Debug for SmartModuleWasm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "SmartModuleWasm {{ format: {:?}, payload: [REDACTED] }}",
            self.format
        ))
    }
}

#[cfg(feature = "use_serde")]
mod base64 {
    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};

    #[allow(clippy::ptr_arg)]
    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}
impl SmartModuleWasm {
    pub fn from_binary_payload(payload: Vec<u8>) -> Self {
        SmartModuleWasm {
            payload,
            format: SmartModuleWasmFormat::Binary,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleWasmFormat {
    #[cfg_attr(feature = "use_serde", serde(rename = "BINARY"))]
    Binary,
    #[cfg_attr(feature = "use_serde", serde(rename = "TEXT"))]
    Text,
}

impl Default for SmartModuleWasmFormat {
    fn default() -> SmartModuleWasmFormat {
        SmartModuleWasmFormat::Binary
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleParameter {
    name: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleInputKind {
    Stream,
    External,
}

impl Default for SmartModuleInputKind {
    fn default() -> SmartModuleInputKind {
        SmartModuleInputKind::Stream
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleOutputKind {
    Stream,
    External,
    Table,
}

impl Default for SmartModuleOutputKind {
    fn default() -> SmartModuleOutputKind {
        SmartModuleOutputKind::Stream
    }
}

impl std::fmt::Display for SmartModuleSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SmartModuleSpec")
    }
}

#[cfg(test)]
mod tests {
    use crate::smartmodule::SmartModuleInputKind;

    #[test]
    fn test_sm_spec_simple() {
        use super::SmartModuleSpec;

        let yaml_spec: &str = r#"
input_kind: Stream
output_kind: Stream
wasm:
    format: BINARY
    payload: H4sIAAAAAAA
"#;
        let sm_spec: SmartModuleSpec =
            serde_yaml::from_str(yaml_spec).expect("Failed to deserialize");

        assert_eq!(sm_spec.input_kind, SmartModuleInputKind::Stream);
    }


    #[test]
    fn test_sm_spec_init_params() {
        use super::SmartModuleSpec;

        let yaml_spec: &str = r#"
input_kind: Stream
output_kind: Stream
wasm:
    format: BINARY
    payload: H4sIAAAAAAA
init_params:
    - name: param1
      value: value1
    - name: param2
      value: value2
"#;
        let sm_spec: SmartModuleSpec =
            serde_yaml::from_str(yaml_spec).expect("Failed to deserialize");

        assert_eq!(sm_spec.input_kind, SmartModuleInputKind::Stream);
    }
}
