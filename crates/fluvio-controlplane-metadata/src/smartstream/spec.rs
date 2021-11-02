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

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamModules {
    transform: Vec<SmartStreamModuleRef>,
    outputs: Vec<SmartStreamModuleRef>,
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
