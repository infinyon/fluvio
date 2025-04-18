use std::{time::Duration, collections::BTreeMap};

use derive_builder::Builder;
use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    derive(schemars::JsonSchema),
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
    derive(schemars::JsonSchema),
    serde(rename_all = "kebab-case")
)]
pub struct Bounds {
    #[cfg_attr(feature = "use_serde", serde(deserialize_with = "non_zero_count"))]
    pub count: u64,
    #[cfg_attr(
        feature = "use_serde",
        serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "humantime_serde"
        ),
        schemars(with = "String")
    )]
    pub age: Option<Duration>,
}

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    derive(schemars::JsonSchema),
    serde(rename_all = "kebab-case")
)]
pub struct Filter {
    pub transform: Transform,
}

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    derive(schemars::JsonSchema),
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
fn non_zero_count<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let count = u64::deserialize(deserializer)?;
    if count == 0 {
        Err(serde::de::Error::custom("count must be non-zero"))
    } else {
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "use_serde")]
    use serde_yaml;

    #[cfg(feature = "use_serde")]
    #[test]
    fn test_deserialize_bounds_missing_count() {
        let yaml = r#"
            age: 30s
        "#;
        let bounds: Result<Bounds, _> = serde_yaml::from_str(yaml);
        assert_eq!(
            bounds.unwrap_err().to_string(),
            "missing field `count` at line 2 column 13"
        );
    }

    #[cfg(feature = "use_serde")]
    #[test]
    fn test_deserialize_bounds_zero() {
        let yaml = r#"
            count: 0
            age: 30s
        "#;
        let bounds: Result<Bounds, _> = serde_yaml::from_str(yaml);
        assert_eq!(
            bounds.unwrap_err().to_string(),
            "count must be non-zero at line 2 column 13"
        );
    }

    #[cfg(feature = "use_serde")]
    #[test]
    fn test_deserialize_bounds_non_zero() {
        let yaml = r#"
            count: 10
            age: 30s
        "#;
        let bounds: Result<Bounds, _> = serde_yaml::from_str(yaml);
        assert!(bounds.is_ok());
    }
}
