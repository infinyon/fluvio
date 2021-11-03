//!
//! # SmartStream Spec
//!
//!

use dataplane::core::{Encoder, Decoder};

//use crate::smartmodule::SmartModuleInputKind;

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartStreamSpec {
    pub inputs: SmartStreamInputs,
    pub modules: SmartStreamModules,
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartStreamInputs {
    pub left: SmartStreamInput,
    pub right: Option<SmartStreamInput>,
}

#[derive(Debug, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartStreamInput {
    #[cfg_attr(feature = "use_serde", serde(rename = "topic"))]
    Topic(SmartStreamRef),
    #[cfg_attr(feature = "use_serde", serde(rename = "smartstream"))]
    SmartStream(SmartStreamRef),
}

impl Default for SmartStreamInput {
    fn default() -> Self {
        SmartStreamInput::Topic(SmartStreamRef::default())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamRef {
    pub name: String,
}

impl SmartStreamRef {
    pub fn new(name: String) -> Self {
        SmartStreamRef { name }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamModules {
    pub transforms: Vec<SmartStreamModuleRef>,
    pub outputs: Vec<SmartStreamModuleRef>,
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamModuleRef {
    pub name: String,
}

impl SmartStreamModuleRef {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[cfg(test)]
mod test {

    use super::SmartStreamInputs;

    #[test]
    fn test_smartstream_spec_deserialiation() {
        let _spec: SmartStreamInputs = serde_json::from_str(
            r#"
                {
                    "left": {
                        "topic": {
                            "name": "test"
                        }
                    }
                }
            "#,
        )
        .expect("spec");
    }
}
