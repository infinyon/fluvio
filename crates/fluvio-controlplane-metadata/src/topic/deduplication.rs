use std::{time::Duration, collections::BTreeMap};

use derive_builder::Builder;
use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct Deduplication {
    pub bounds: Bounds,
    pub filter: Filter,
}

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct Bounds {
    #[cfg_attr(feature = "use_serde", serde(default, skip_serializing_if = "is_zero"))]
    pub count: u64,
    #[cfg_attr(
        feature = "use_serde",
        serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "humantime_serde"
        )
    )]
    pub age: Option<Duration>,
}

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct Filter {
    pub transform: Transform,
}

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct Transform {
    pub uses: String,
    #[cfg_attr(
        feature = "use_serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub with: BTreeMap<String, String>,
}

#[cfg(feature = "use_serde")]
fn is_zero(val: &u64) -> bool {
    *val == 0
}
