use std::{
    collections::BTreeMap,
    fmt::{self, Display},
    fs::File,
    io::Read,
    ops::Deref,
    path::PathBuf,
};

use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TransformationConfig {
    pub transforms: Vec<TransformationStep>,
}

impl TransformationConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, anyhow::Error> {
        let mut file = File::open(path.into())?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        let config: Self = serde_yaml::from_slice(content.as_mut_slice())?;
        Ok(config)
    }
}

impl From<TransformationStep> for TransformationConfig {
    fn from(step: TransformationStep) -> Self {
        Self {
            transforms: vec![step],
        }
    }
}

impl<T: Deref<Target = str>> TryFrom<Vec<T>> for TransformationConfig {
    type Error = serde_json::Error;

    fn try_from(value: Vec<T>) -> Result<Self, Self::Error> {
        let transforms = value
            .into_iter()
            .map(|v| serde_json::from_str(v.deref()))
            .collect::<Result<Vec<TransformationStep>, Self::Error>>()?;
        Ok(Self { transforms })
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TransformationStep {
    pub uses: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub with: BTreeMap<String, JsonString>,
}

impl Display for TransformationStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl TryFrom<&str> for TransformationStep {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JsonString(String);

impl From<JsonString> for String {
    fn from(json: JsonString) -> Self {
        json.0
    }
}

impl From<&str> for JsonString {
    fn from(str: &str) -> Self {
        Self(str.into())
    }
}

impl<'de> Deserialize<'de> for JsonString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AsJsonString;
        impl<'de> Visitor<'de> for AsJsonString {
            type Value = JsonString;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("str, string, sequence or map")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonString(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonString(v))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let json: serde_json::Value =
                    Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
                serde_json::to_string(&json).map(JsonString).map_err(|err| {
                    de::Error::custom(format!("unable to serialize map to json: {err}"))
                })
            }

            fn visit_seq<M>(self, seq: M) -> Result<Self::Value, M::Error>
            where
                M: SeqAccess<'de>,
            {
                let json: serde_json::Value =
                    Deserialize::deserialize(de::value::SeqAccessDeserializer::new(seq))?;
                serde_json::to_string(&json).map(JsonString).map_err(|err| {
                    de::Error::custom(format!("unable to serialize seq to json: {err}"))
                })
            }
        }
        deserializer.deserialize_any(AsJsonString)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_from_file_empty() {
        //given
        //when
        let config = TransformationConfig::from_file("testdata/transformation/empty.yaml")
            .expect("config file");

        //then
        assert!(config.transforms.is_empty())
    }

    #[test]
    fn test_read_from_file() {
        //given
        //when
        let config = TransformationConfig::from_file("testdata/transformation/full.yaml")
            .expect("config file");

        //then
        assert_eq!(config.transforms.len(), 2);
        assert_eq!(
            config,
            TransformationConfig {
                transforms: vec![
                    TransformationStep {
                        uses: "infinyon/jolt@0.1.0".to_string(),
                        with: BTreeMap::from([(
                            "spec".to_string(),
                            JsonString("[{\"operation\":\"shift\",\"spec\":{\"payload\":{\"device\":\"device\"}}},{\"operation\":\"default\",\"spec\":{\"device\":{\"type\":\"mobile\"}}}]".to_string())
                        )])
                    },
                    TransformationStep {
                        uses: "infinyon/json-sql@0.1.0".to_string(),
                        with: BTreeMap::from([(
                            "mapping".to_string(),
                            JsonString("{\"map-columns\":{\"device_id\":{\"json-key\":\"device.device_id\",\"value\":{\"default\":\"0\",\"required\":true,\"type\":\"int\"}},\"record\":{\"json-key\":\"$\",\"value\":{\"required\":true,\"type\":\"jsonb\"}}},\"table\":\"topic_message_demo\"}".to_string())
                        )])
                    }
                ]
            }
        )
    }
    #[test]
    fn test_from_empty_vec() {
        //given
        let vec: Vec<String> = vec![];

        //when
        let config = TransformationConfig::try_from(vec).expect("transformation config");

        //then
        assert!(config.transforms.is_empty())
    }

    #[test]
    fn test_from_vec() {
        //given
        let vec = vec![
            r#"{"uses":"infinyon/jolt@0.1.0","invoke":"insert","with":{"spec":"[{\"operation\":\"remove\",\"spec\":{\"length\":\"\"}}]"}}"#,
            r#"{"uses":"infinyon/json-sql@0.1.0","invoke":"insert","with":{"mapping":"{\"table\":\"topic_message_demo\",\"map-columns\":{\"fact\":{\"json-key\":\"fact\",\"value\":{\"type\":\"text\",\"required\":true}},\"record\":{\"json-key\":\"$\",\"value\":{\"type\":\"jsonb\",\"required\":true}}}}"}}"#,
        ];

        //when
        let config = TransformationConfig::try_from(vec).expect("transformation config");

        //then
        assert_eq!(config.transforms.len(), 2);
        assert_eq!(config.transforms[0].uses, "infinyon/jolt@0.1.0");
        assert_eq!(
            config.transforms[0].with,
            BTreeMap::from([(
                "spec".to_string(),
                JsonString("[{\"operation\":\"remove\",\"spec\":{\"length\":\"\"}}]".to_string())
            )])
        );

        assert_eq!(config.transforms[1].uses, "infinyon/json-sql@0.1.0");
        assert_eq!(
            config.transforms[1].with,
            BTreeMap::from([(
                "mapping".to_string(),
                JsonString("{\"table\":\"topic_message_demo\",\"map-columns\":{\"fact\":{\"json-key\":\"fact\",\"value\":{\"type\":\"text\",\"required\":true}},\"record\":{\"json-key\":\"$\",\"value\":{\"type\":\"jsonb\",\"required\":true}}}}".to_string()) 
            )])
        );
    }
}
