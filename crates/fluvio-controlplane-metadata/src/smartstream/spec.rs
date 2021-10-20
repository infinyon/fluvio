//!
//! # SmartStream Spec
//!
//!

use dataplane::core::{Encoder, Decoder};

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartStreamSpec {
    pub inputs: SmartStreamInput,
    pub streams: Vec<SmartStreamSelector>,
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamInput {
    pub topic_selector: TopicSelector,
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TopicSelector {
    pub name: String,
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartStreamSelector {
    pub name: String,
    pub id: String,
}
