//!
//! # Parse Json and generates Message Structure
//!
//! Takes a JSON value object and generates a data structure
//!
use std::io::Error;
use std::io::ErrorKind;

use serde_json;
use serde_json::Value;

use super::spec_msg::SpecMessage;
use super::spec_msg::SpecMessageType;
use super::spec_msg::{ApiVersions, DefaultType, NullableVersions, Versions};
use super::spec_msg::{SpecField, SpecFieldType, SpecFields};

/// Convert Json to Request Msg
pub fn parse_json_to_request(val: Value) -> Result<SpecMessage, Error> {
    let msg_type = get_msg_type(&val)?;

    match msg_type {
        SpecMessageType::Request => parse_message(&val, msg_type),
        SpecMessageType::Response => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("expected 'Request', found '{}'", msg_type),
            ));
        }
    }
}

/// Convert Json to Response Msg
pub fn parse_json_to_response(val: Value) -> Result<SpecMessage, Error> {
    let msg_type = get_msg_type(&val)?;

    match msg_type {
        SpecMessageType::Request => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("expected 'Response', found '{}'", msg_type),
            ));
        }
        SpecMessageType::Response => parse_message(&val, msg_type),
    }
}

/// Json object into Request/Response message
fn parse_message(val: &Value, typ: SpecMessageType) -> Result<SpecMessage, Error> {
    let name = get_name(&val)?;
    let api_key = get_api_key(&val)?;
    let api_versions = get_api_versions(&val)?;
    let fields = match maybe_get_fields(&val)? {
        Some(vals) => Some(parse_fields(vals)?),
        None => None,
    };

    // build message
    Ok(SpecMessage {
        name,
        api_key,
        api_versions,
        typ,
        fields,
    })
}

/// Json object into fields
fn parse_fields<'a>(vals: &'a Vec<Value>) -> Result<SpecFields, Error> {
    let mut fields: SpecFields = vec![];

    for val in vals {
        fields.push(parse_field(val)?);
    }

    Ok(fields)
}

/// Json object into fields
fn parse_field(val: &Value) -> Result<SpecField, Error> {
    let name = get_name(&val)?;
    let typ = get_field_type(&val)?;
    let versions = get_versions(&val)?;
    let about = maybe_string(&val, "about")?;
    let entity_type = maybe_string(&val, "entityType")?;
    let default = maybe_default(&val)?;
    let ignorable = maybe_bool(&val, "ignorable")?;
    let map_key = maybe_bool(&val, "mapKey")?;
    let nullable_versions = maybe_get_nullable_versions(&val)?;
    let fields = match maybe_get_fields(&val)? {
        Some(vals) => Some(parse_fields(vals)?),
        None => None,
    };

    Ok(SpecField {
        name,
        typ,
        versions,
        about,
        default,
        nullable_versions,
        entity_type,
        ignorable,
        map_key,
        fields,
    })
}

/// Decode 'value' at key or error
fn get_key<'a>(val: &'a Value, key: &str) -> Result<&'a Value, Error> {
    match val.get(key) {
        Some(v) => Ok(v),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected '{}', found none", key),
        )),
    }
}

/// Decode 'name' string or error
fn get_name(val: &Value) -> Result<String, Error> {
    let name = get_string(val, "name")?;
    //println!("{}", name);
    match name.as_str() {
        "Type" => Ok("Typ".to_string()),
        _ => Ok(name),
    }
}

/// Decode 'apiKey' string or error
fn get_api_key(val: &Value) -> Result<i64, Error> {
    get_i64(val, "apiKey")
}

/// Decode 'type' at key to MesageType or error
pub fn get_msg_type(val: &Value) -> Result<SpecMessageType, Error> {
    let key = "type";

    match get_key(&val, key)?.as_str() {
        Some(v) => SpecMessageType::decode(key, v),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("key '{}', not message type", key),
        )),
    }
}

/// Decode 'i64' at key or error
fn get_i64(val: &Value, key: &str) -> Result<i64, Error> {
    match get_key(&val, key)?.as_i64() {
        Some(v) => Ok(v),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("key '{}', not i64 number", key),
        )),
    }
}

#[allow(dead_code)]
/// Decode 'bool' at key or error
fn get_bool(val: &Value, key: &str) -> Result<bool, Error> {
    match get_key(&val, key)?.as_bool() {
        Some(v) => Ok(v),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("key '{}', not boolean", key),
        )),
    }
}

/// Decode 'bool' if avialable at key or error
fn maybe_bool(val: &Value, key: &str) -> Result<Option<bool>, Error> {
    match get_key(&val, key) {
        Ok(v) => match v.as_bool() {
            Some(v) => Ok(Some(v)),
            None => Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', not boolean", key),
            )),
        },
        Err(_) => Ok(None),
    }
}

/// Decode 'default' if avialable at key or error
fn maybe_default(val: &Value) -> Result<Option<DefaultType>, Error> {
    let key = "default";
    match get_key(&val, key) {
        Ok(val) => match DefaultType::decode(key, val) {
            Ok(v) => Ok(Some(v)),
            Err(err) => Err(err),
        },
        Err(_) => Ok(None),
    }
}

/// Decode 'String' at key or error
fn get_string(val: &Value, key: &str) -> Result<String, Error> {
    match get_key(&val, key)?.as_str() {
        Some(v) => Ok(v.to_string()),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("key '{}', not string", key),
        )),
    }
}

/// Decode 'String' if avialable at key or error
fn maybe_string(val: &Value, key: &str) -> Result<Option<String>, Error> {
    match get_key(&val, key) {
        Ok(v) => match v.as_str() {
            Some(v) => Ok(Some(v.to_string())),
            None => Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', not string", key),
            )),
        },
        Err(_) => Ok(None),
    }
}

/// Decode 'versions' at key to Version enum or error
fn get_versions(val: &Value) -> Result<Versions, Error> {
    let key = "versions";
    match get_key(&val, key)?.as_str() {
        Some(v) => Versions::decode(key, v),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("key '{}', incorrect versions format", key),
        )),
    }
}

/// Decode 'validVersion' at key to Version enum or error
fn get_api_versions(val: &Value) -> Result<ApiVersions, Error> {
    let key = "validVersions";
    match get_key(&val, key)?.as_str() {
        Some(v) => ApiVersions::decode(key, v),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("key '{}', not validVersion", key),
        )),
    }
}

/// Decode 'nullableVersions' if avaialble
fn maybe_get_nullable_versions(val: &Value) -> Result<Option<NullableVersions>, Error> {
    let key = "nullableVersions";
    match get_key(&val, key) {
        Ok(v_raw) => match v_raw.as_str() {
            Some(v) => match NullableVersions::decode(key, v) {
                Ok(ver) => Ok(Some(ver)),
                Err(err) => Err(err),
            },
            None => Err(Error::new(
                ErrorKind::InvalidData,
                format!("key '{}', not version string", key),
            )),
        },
        Err(_) => Ok(None),
    }
}

/// Decode 'fileds' is avaialble
fn maybe_get_fields(val: &Value) -> Result<Option<&Vec<Value>>, Error> {
    match get_key(&val, "fields") {
        Ok(v) => match v.as_array() {
            Some(v) => Ok(Some(v)),
            None => Err(Error::new(
                ErrorKind::InvalidData,
                "key 'fields', not a array",
            )),
        },
        Err(_) => Ok(None),
    }
}

/// Decode field 'type' message at key to SpecFieldType or error
fn get_field_type(val: &Value) -> Result<SpecFieldType, Error> {
    let key = "type";
    match get_key(&val, key)?.as_str() {
        Some(v) => SpecFieldType::decode("key", v),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            format!("key '{}', not field type", key),
        )),
    }
}

// -----------------------------------
// Test Cases
// -----------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::spec_msg::PType;

    use serde_json;
    use serde_json::Value;
    use std::io::Error as IoError;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;

    fn from_file<P: AsRef<Path>>(path: P) -> Result<Value, IoError> {
        Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
    }

    #[test]
    fn test_parse_read_json() {
        let value = from_file("./test-data/MetadataRequest_clean.json");
        assert!(value.is_ok());
    }

    #[test]
    fn test_get_i64() {
        let val = from_file("./test-data/MetadataRequest_clean.json").unwrap();
        let api_key = get_api_key(&val);
        assert!(api_key.is_ok());
        assert_eq!(api_key.unwrap(), 3);
    }

    #[test]
    fn test_get_string() {
        let val = from_file("./test-data/MetadataRequest_clean.json").unwrap();
        let name = get_name(&val);
        assert!(name.is_ok());
        assert_eq!(name.unwrap(), "MetadataRequest".to_owned());

        // maybe string
        let none = maybe_string(&val, "ttt");
        assert!(none.is_ok());
        assert!(none.unwrap().is_none());
    }

    #[test]
    fn test_get_msg_type() {
        let val = from_file("./test-data/MetadataRequest_clean.json").unwrap();
        let msg_type = get_msg_type(&val);
        assert!(msg_type.is_ok());
        assert_eq!(msg_type.unwrap(), SpecMessageType::Request);

        let val = serde_json::from_str("{\"notValid\": \"na\"}").unwrap();
        let msg_type = get_msg_type(&val);
        assert!(msg_type.is_err());
        assert_eq!(
            msg_type.unwrap_err().to_string(),
            "expected \'type\', found none".to_owned()
        );
    }

    #[test]
    fn test_get_version() {
        // Invalid
        let val = serde_json::from_str("{\"versions\": \"test\"}").unwrap();
        let version = get_versions(&val);
        assert!(version.is_err());
        assert_eq!(
            version.unwrap_err().to_string(),
            "key 'versions', incorrect version 'test'".to_owned()
        );

        // Range
        let val = serde_json::from_str("{\"versions\": \"0-2\"}").unwrap();
        let version = get_versions(&val);
        assert!(version.is_ok());
        assert_eq!(version.unwrap(), Versions::Range(0, 2));

        // Exact
        let val = serde_json::from_str("{\"versions\": \"11\"}").unwrap();
        let version = get_versions(&val);
        assert!(version.is_ok());
        assert_eq!(version.unwrap(), Versions::Exact(11));

        // Exact
        let val = serde_json::from_str("{\"versions\": \"0\"}").unwrap();
        let version = get_versions(&val);
        assert!(version.is_ok());
        assert_eq!(version.unwrap(), Versions::Exact(0));

        // GreaterOrEqualTo
        let val = serde_json::from_str("{\"versions\": \"0+\"}").unwrap();
        let version = get_versions(&val);
        assert!(version.is_ok());
        assert_eq!(version.unwrap(), Versions::GreaterOrEqualTo(0));
    }

    #[test]
    fn test_parse_fields() {
        let val = from_file("./test-data/MetadataRequest_clean.json").unwrap();
        let ok_fields = maybe_get_fields(&val);
        assert!(ok_fields.is_ok());

        let some_fields = ok_fields.unwrap();
        assert!(some_fields.is_some());
        assert_eq!(some_fields.unwrap().len() > 0, true);
    }

    #[test]
    fn test_parse_field_type() {
        // Invalid
        let val = serde_json::from_str("{\"notType\": \"not valid\"}").unwrap();
        let typ = get_field_type(&val);
        assert!(typ.is_err());
        assert_eq!(
            typ.unwrap_err().to_string(),
            "expected \'type\', found none".to_owned()
        );

        // Primitive
        let val = serde_json::from_str("{\"type\": \"int8\"}").unwrap();
        let typ = get_field_type(&val);
        assert!(typ.is_ok());
        assert_eq!(typ.unwrap(), SpecFieldType::PRIMITIVE(PType::INT8));

        // Primitive Array
        let val = serde_json::from_str("{\"type\": \"[]int32\"}").unwrap();
        let typ = get_field_type(&val);
        assert!(typ.is_ok());
        assert_eq!(typ.unwrap(), SpecFieldType::PRIMITIVE_ARRAY(PType::INT32));

        // Custom Array
        let val = serde_json::from_str("{\"type\": \"[]CreatableTopic\"}").unwrap();
        let typ = get_field_type(&val);
        assert!(typ.is_ok());
        assert_eq!(
            typ.unwrap(),
            SpecFieldType::CUSTOM_ARRAY("CreatableTopic".to_owned())
        );
    }

    #[test]
    fn test_parse_maybe_default() {
        // No default
        let val = serde_json::from_str("{\"notDefault\": \"not valid\"}").unwrap();
        let default = maybe_default(&val);
        assert!(default.is_ok());
        assert!(default.unwrap().is_none());

        // negative number (string)
        let val = serde_json::from_str("{\"default\": \"-1\"}").unwrap();
        let default = maybe_default(&val);

        assert!(default.is_ok());
        assert_eq!(
            default.unwrap(),
            Some(DefaultType::STRING("-1".to_string()))
        );

        // negative number
        let val = serde_json::from_str("{\"default\": -1}").unwrap();
        let default = maybe_default(&val);

        assert!(default.is_ok());
        assert_eq!(default.unwrap(), Some(DefaultType::INT64(-1)));

        // boolean
        let val = serde_json::from_str("{\"default\": \"true\"}").unwrap();
        let default = maybe_default(&val);

        assert!(default.is_ok());
        assert_eq!(
            default.unwrap(),
            Some(DefaultType::STRING("true".to_string()))
        );
    }
}
