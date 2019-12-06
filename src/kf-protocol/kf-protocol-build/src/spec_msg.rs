//!
//! # SpecMessage Structure
//!
//! SpecMessage structures
//!
use std::fmt;
use std::io::Error;
use std::io::ErrorKind;

use serde_json::Value;

#[derive(Debug, PartialEq)]
pub struct SpecMessage {
    pub name: String,
    pub api_key: i64,
    pub api_versions: ApiVersions,
    pub typ: SpecMessageType,
    pub fields: Option<SpecFields>,
}

#[derive(Debug, PartialEq)]
pub struct SpecField {
    pub name: String,
    pub typ: SpecFieldType,
    pub versions: Versions,
    pub about: Option<String>,
    pub default: Option<DefaultType>,
    pub nullable_versions: Option<NullableVersions>,
    pub entity_type: Option<String>,
    pub ignorable: Option<bool>,
    pub map_key: Option<bool>,
    pub fields: Option<SpecFields>,
}
pub type SpecFields = Vec<SpecField>;

#[derive(Debug, PartialEq)]
pub enum SpecMessageType {
    Request,
    Response,
}

pub type StructName = String;

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub enum SpecFieldType {
    PRIMITIVE(PType),
    PRIMITIVE_ARRAY(PType),
    CUSTOM_ARRAY(StructName),
}

#[derive(Debug, PartialEq)]
pub enum PType {
    BOOL,
    INT8,
    INT16,
    INT32,
    INT64,
    STRING,
    BYTES,
}

#[derive(Debug, PartialEq)]
pub enum DefaultType {
    INT64(i64),
    STRING(String),
}

#[derive(Debug, PartialEq)]
pub enum Versions {
    Exact(i16),
    Range(i16, i16),
    GreaterOrEqualTo(i16),
}

#[derive(Debug, PartialEq)]
pub enum ApiVersions {
    Exact(i16),
    Range(i16, i16),
}

#[derive(Debug, PartialEq)]
pub struct NullableVersions {
    min_ver: i16,
}

// -----------------------------------
// Implement - SpecMessageType
// -----------------------------------

impl SpecMessageType {
    pub fn decode(key: &str, val: &str) -> Result<SpecMessageType, Error> {
        match val {
            "request" => Ok(SpecMessageType::Request),
            "response" => Ok(SpecMessageType::Response),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "key '{}', expected: 'request', response' - found '{}'",
                    key, val
                ),
            )),
        }
    }
}

impl fmt::Display for SpecMessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SpecMessageType::Request => write!(f, "request"),
            SpecMessageType::Response => write!(f, "response"),
        }
    }
}

// -----------------------------------
// Implement - PType
// -----------------------------------
impl PType {
    pub fn new(val: &str) -> Result<Self, Error> {
        match val {
            "bool" => Ok(PType::BOOL),
            "int8" => Ok(PType::INT8),
            "int16" => Ok(PType::INT16),
            "int32" => Ok(PType::INT32),
            "int64" => Ok(PType::INT64),
            "string" => Ok(PType::STRING),
            "bytes" => Ok(PType::BYTES),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid primitive type '{}'", val),
            )),
        }
    }

    pub fn from(&self) -> &str {
        match self {
            PType::BOOL => "bool",
            PType::INT8 => "i8",
            PType::INT16 => "i16",
            PType::INT32 => "i32",
            PType::INT64 => "i64",
            PType::STRING => "String",
            PType::BYTES => "Vec<u8>",
        }
    }
}

// -----------------------------------
// Implement - SpecFieldType
// -----------------------------------

impl SpecFieldType {
    /// Decode field type, primitives, primitive arrays, and custom arrays
    pub fn decode(key: &str, val: &str) -> Result<SpecFieldType, Error> {
        match PType::new(val) {
            Ok(primitive) => Ok(SpecFieldType::PRIMITIVE(primitive)),
            Err(_) => SpecFieldType::decode_array(key, val),
        }
    }

    /// Decode primitive or custom arrays
    pub fn decode_array(key: &str, val: &str) -> Result<SpecFieldType, Error> {
        if val.len() < 3 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', incorrect array type '{}'", key, val),
            ));
        }

        // ensure array "[]..."
        let arr_str = &val[..2];
        if arr_str != "[]" {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', not array type '{}'", key, val),
            ));
        }

        // parse content
        let content = &val[2..];
        match PType::new(content) {
            Ok(primitive) => Ok(SpecFieldType::PRIMITIVE_ARRAY(primitive)),
            Err(_) => Ok(SpecFieldType::CUSTOM_ARRAY(content.to_string())),
        }
    }

    /// True if primitive String or Array
    pub fn is_string_or_array(&self) -> bool {
        match self {
            SpecFieldType::PRIMITIVE(p_type) => match p_type {
                PType::BYTES => true, //array of u8
                PType::STRING => true,
                _ => false,
            },

            SpecFieldType::PRIMITIVE_ARRAY(_) => true,
            SpecFieldType::CUSTOM_ARRAY(_) => true,
        }
    }

    /// Convert into string value
    pub fn value(&self) -> String {
        match self {
            SpecFieldType::PRIMITIVE(p_type) => p_type.from().to_owned(),
            SpecFieldType::PRIMITIVE_ARRAY(p_type) => format!("Vec<{}>", p_type.from().to_owned()),
            SpecFieldType::CUSTOM_ARRAY(struct_name) => format!("Vec<{}>", struct_name),
        }
    }

    /// Return value name
    pub fn custom_value_name(&self) -> String {
        match self {
            SpecFieldType::CUSTOM_ARRAY(struct_name) => struct_name.clone(),
            _ => "unknown".to_owned(),
        }
    }
}

// -----------------------------------
// Implement - DefaultType
// -----------------------------------

impl DefaultType {
    pub fn decode(key: &str, val: &Value) -> Result<DefaultType, Error> {
        if let Some(v) = val.as_i64() {
            Ok(DefaultType::INT64(v))
        } else if let Some(v) = val.as_str() {
            Ok(DefaultType::STRING(v.to_string()))
        } else {
            Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', unknown default type", key),
            ))
        }
    }

    /// Convert into string value
    pub fn value(&self) -> String {
        match self {
            DefaultType::INT64(v) => v.to_string(),
            DefaultType::STRING(s) => s.clone(),
        }
    }
}

// -----------------------------------
// Implement - Version
// -----------------------------------

impl Versions {
    pub fn decode(key: &str, val: &str) -> Result<Versions, Error> {
        let versions: Vec<&str> = val.split('-').collect();
        // Parse Range
        if versions.len() == 2 {
            let min_range = match versions[0].parse::<i16>() {
                Ok(min_ver) => min_ver,
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("key '{}', incorrect version range '{}'", key, versions[0]),
                    ));
                }
            };

            let max_range = match versions[1].parse::<i16>() {
                Ok(max_ver) => max_ver,
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("key '{}', incorrect version range '{}'", key, versions[1]),
                    ));
                }
            };

            Ok(Versions::Range(min_range, max_range))
        } else {
            let versions: Vec<&str> = val.split('+').collect();

            // Parse exact value
            let ver_num = match versions[0].parse::<i16>() {
                Ok(ver_num) => ver_num,
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("key '{}', incorrect version '{}'", key, val),
                    ));
                }
            };

            // Exact or Greater
            match val.contains('+') {
                true => Ok(Versions::GreaterOrEqualTo(ver_num)),
                false => Ok(Versions::Exact(ver_num)),
            }
        }
    }

    /// Check if version is Exactly zero "0"
    pub fn is_zero(&self) -> bool {
        match self {
            Versions::Exact(ver) => *ver == 0,
            _ => false,
        }
    }

    /// Check if version is Greater or equal to zero "0+"
    pub fn is_zero_plus(&self) -> bool {
        match self {
            Versions::GreaterOrEqualTo(min_ver) => *min_ver == 0,
            _ => false,
        }
    }

    /// Retrieve API versions in a pair touple, where second is optional
    pub fn touples(&self) -> (i16, Option<i16>) {
        match self {
            Versions::Exact(ver) => (*ver, Some(*ver)),
            Versions::Range(min_ver, max_ver)  => (*min_ver, Some(*max_ver)),
            Versions::GreaterOrEqualTo(min_ver) => (*min_ver, None)
        }
    }
}

// -----------------------------------
// Implement - ApiVersions
// -----------------------------------

impl ApiVersions {
    pub fn decode(key: &str, val: &str) -> Result<ApiVersions, Error> {
        match Versions::decode(key, val)? {
            Versions::Exact(ver) => Ok(ApiVersions::Exact(ver)),
            Versions::Range(min, max) => Ok(ApiVersions::Range(min, max)),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', incorrect validVersions", key),
            )),
        }
    }

    /// Retrieve API versions in a pair touple
    pub fn touples(&self) -> (i16, i16) {
        match self {
            ApiVersions::Exact(ver) => (*ver, *ver),
            ApiVersions::Range(min_ver, max_ver) => (*min_ver, *max_ver)
        }
    }
}

// -----------------------------------
// Implement - NullableVersions
// -----------------------------------

impl NullableVersions {
    pub fn decode(key: &str, val: &str) -> Result<NullableVersions, Error> {
        match Versions::decode(key, val)? {
            Versions::GreaterOrEqualTo(min_ver) => Ok(NullableVersions { min_ver }),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', incorrect nullableVersions", key),
            )),
        }
    }
}
