use serde::de::Error;
use serde::Serialize;
use serde::{de, Deserialize};
use std::collections::HashMap;
use std::fmt;

use crate::model::sql::Type;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Mapping {
    pub table: String,
    #[serde(alias = "map-columns")]
    pub columns: HashMap<String, Column>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Column {
    #[serde(alias = "json-key")]
    pub json_key: String,
    pub value: Value,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Value {
    #[serde(rename = "type")]
    pub type_: ValueType,
    #[serde(default, deserialize_with = "deserialize_to_string")]
    pub default: Option<String>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    #[serde(alias = "int8")]
    Bigint,
    #[serde(alias = "int4")]
    #[serde(alias = "int")]
    Integer,
    #[serde(alias = "int2")]
    Smallint,

    #[serde(alias = "bool")]
    Boolean,
    #[serde(alias = "bytes")]
    Bytea,
    Text,

    #[serde(alias = "float4")]
    #[serde(alias = "real")]
    Float,

    #[serde(alias = "double precision")]
    #[serde(alias = "float8")]
    DoublePrecision,

    #[serde(alias = "decimal")]
    Numeric,

    Date,
    Time,
    Timestamp,
    #[serde(alias = "jsonb")]
    Json,
    Uuid,
}

impl From<ValueType> for Type {
    fn from(value_type: ValueType) -> Self {
        match value_type {
            ValueType::Bigint => Type::BigInt,
            ValueType::Integer => Type::Int,
            ValueType::Smallint => Type::SmallInt,
            ValueType::Boolean => Type::Bool,
            ValueType::Bytea => Type::Bytes,
            ValueType::Text => Type::Text,
            ValueType::Float => Type::Float,
            ValueType::DoublePrecision => Type::DoublePrecision,
            ValueType::Numeric => Type::Numeric,
            ValueType::Date => Type::Date,
            ValueType::Time => Type::Time,
            ValueType::Timestamp => Type::Timestamp,
            ValueType::Json => Type::Json,
            ValueType::Uuid => Type::Uuid,
        }
    }
}

fn deserialize_to_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StringOrInt;
    impl<'de> de::Visitor<'de> for StringOrInt {
        type Value = Option<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("str, string or int")
        }

        fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v.to_string()))
        }

        fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Some(v))
        }
    }
    deserializer.deserialize_any(StringOrInt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_deserialize() {
        // given
        let input = json!({
            "table" : "test_table",
            "map-columns": {
                "column_name" : {
                    "json-key": "test-key",
                    "value": {
                        "type": "int4",
                        "default": "4",
                        "required": false,
                    }
                }
            }
        });

        // when
        let mapping: Mapping = serde_json::from_value(input).expect("valid mapping");

        // then
        assert_eq!(
            mapping,
            Mapping {
                table: "test_table".to_string(),
                columns: HashMap::from([(
                    "column_name".to_string(),
                    Column {
                        json_key: "test-key".to_string(),
                        value: Value {
                            type_: ValueType::Integer,
                            default: Some("4".to_string()),
                            required: false
                        }
                    }
                )])
            }
        );
    }

    #[test]
    fn test_default_value_as_int() {
        // given
        let input = json!({
            "table" : "test_table",
            "map-columns": {
                "column_name" : {
                    "json-key": "test-key",
                    "value": {
                        "type": "int4",
                        "default": 4,
                        "required": false,
                    }
                }
            }
        });
        // when
        let mapping: Mapping = serde_json::from_value(input).expect("valid mapping");

        // then
        assert_eq!(
            mapping,
            Mapping {
                table: "test_table".to_string(),
                columns: HashMap::from([(
                    "column_name".to_string(),
                    Column {
                        json_key: "test-key".to_string(),
                        value: Value {
                            type_: ValueType::Integer,
                            default: Some("4".to_string()),
                            required: false
                        }
                    }
                )])
            }
        );
    }

    #[test]
    fn test_default_value_as_float() {
        // given
        let input = json!({
            "table" : "test_table",
            "map-columns": {
                "column_name" : {
                    "json-key": "test-key",
                    "value": {
                        "type": "int4",
                        "default": 4.5,
                        "required": false,
                    }
                }
            }
        });
        // when
        let mapping: Mapping = serde_json::from_value(input).expect("valid mapping");

        // then
        assert_eq!(
            mapping,
            Mapping {
                table: "test_table".to_string(),
                columns: HashMap::from([(
                    "column_name".to_string(),
                    Column {
                        json_key: "test-key".to_string(),
                        value: Value {
                            type_: ValueType::Integer,
                            default: Some("4.5".to_string()),
                            required: false
                        }
                    }
                )])
            }
        );
    }

    #[test]
    fn test_default_value_as_negative_int() {
        // given
        let input = json!({
            "table" : "test_table",
            "map-columns": {
                "column_name" : {
                    "json-key": "test-key",
                    "value": {
                        "type": "int4",
                        "default": -5,
                        "required": false,
                    }
                }
            }
        });
        // when
        let mapping: Mapping = serde_json::from_value(input).expect("valid mapping");

        // then
        assert_eq!(
            mapping,
            Mapping {
                table: "test_table".to_string(),
                columns: HashMap::from([(
                    "column_name".to_string(),
                    Column {
                        json_key: "test-key".to_string(),
                        value: Value {
                            type_: ValueType::Integer,
                            default: Some("-5".to_string()),
                            required: false
                        }
                    }
                )])
            }
        );
    }

    #[test]
    fn test_skip_optional_fields() {
        // given
        let input = json!({
            "table" : "test_table",
            "map-columns": {
                "column_name" : {
                    "json-key": "test-key",
                    "value": {
                        "type": "int4"
                    }
                }
            }
        });
        // when
        let mapping: Mapping = serde_json::from_value(input).expect("valid mapping");

        // then
        assert_eq!(
            mapping,
            Mapping {
                table: "test_table".to_string(),
                columns: HashMap::from([(
                    "column_name".to_string(),
                    Column {
                        json_key: "test-key".to_string(),
                        value: Value {
                            type_: ValueType::Integer,
                            default: None,
                            required: false
                        }
                    }
                )])
            }
        );
    }
}
