use bytesize::ByteSize;
use serde::{Serializer, Deserializer, Deserialize};

pub fn serialize<S>(input: &Option<ByteSize>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match input {
        Some(size) => bytesize_serde::serialize(size, serializer),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ByteSize>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_yaml::Value>::deserialize(deserializer)?;

    value
        .map(|v| bytesize_serde::deserialize(v))
        .transpose()
        .map_err(serde::de::Error::custom)
}
