use std::str::FromStr;

use bytesize::ByteSize;
use serde::{Serializer, Deserializer, Deserialize};

pub fn serialize<S>(input: &Option<ByteSize>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match input {
        Some(size) => serializer.serialize_str(&size.to_string()),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ByteSize>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;

    s.map(|s| ByteSize::from_str(&s).map_err(serde::de::Error::custom))
        .transpose()
}
