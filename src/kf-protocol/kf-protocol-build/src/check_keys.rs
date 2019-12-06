//!
//! # Check Keys
//!
//! Takes a Kafka json spec and compares against known keys
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use serde_json::Value;

use super::file_pairs::FilePairs;
use super::file_to_json::file_to_json;

/// Check if all header and field keys are known
pub fn check_header_and_field_keys_are_known(dir: &String) -> Result<(), IoError> {
    let file_pairs = FilePairs::new(dir)?;

    for file_pair in &file_pairs.pairs {
        check_known_header_and_field_keys(&file_pair.req_file)?;
        check_known_header_and_field_keys(&file_pair.res_file)?;
    }

    Ok(())
}

/// Check if all Field keys are known (called recursively)
fn check_known_field_keys(fields: &Vec<Value>) -> Result<(), IoError> {
    let known_field_keys = vec![
        "name".to_owned(),
        "about".to_owned(),
        "type".to_owned(),
        "default".to_owned(),
        "ignorable".to_owned(),
        "mapKey".to_owned(),
        "versions".to_owned(),
        "nullableVersions".to_owned(),
        "entityType".to_owned(),
        "fields".to_owned(),
    ];

    for field in fields {
        for (key, val) in field.as_object().unwrap().iter() {
            if !known_field_keys.contains(key) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("unknown field key: '{}'", key),
                ));
            }

            if key == "fields" {
                match val.as_array() {
                    Some(field_val) => check_known_field_keys(field_val)?,
                    None => {
                        return Err(IoError::new(
                            ErrorKind::InvalidData,
                            "key 'fields' must be array",
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Check if all Header keys are known
fn check_known_header_and_field_keys(file_path: &PathBuf) -> Result<(), IoError> {
    let known_header_keys = vec![
        "name".to_owned(),
        "validVersions".to_owned(),
        "type".to_owned(),
        "fields".to_owned(),
        "apiKey".to_owned(),
    ];

    match file_to_json(file_path) {
        Ok(val) => {
            for (key, val) in val.as_object().unwrap().iter() {
                if !known_header_keys.contains(key) {
                    return Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!("unknown header key: '{}'", key),
                    ));
                }
                if key == "fields" {
                    match val.as_array() {
                        Some(field_val) => check_known_field_keys(field_val)?,
                        None => {
                            return Err(IoError::new(
                                ErrorKind::InvalidData,
                                "key 'fields' must be array",
                            ));
                        }
                    }
                }
            }
            Ok(())
        }
        Err(err) => Err(IoError::new(ErrorKind::InvalidData, format!("{}", err))),
    }
}
