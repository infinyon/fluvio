//!
//! # Generate code from internal message
//!
//! Takes a message structure and generates a code file
//!

use inflector::Inflector;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use textwrap::fill;

use super::spec_msg::NullableVersions;
use super::spec_msg::SpecMessage;
use super::spec_msg::{SpecField, SpecFieldType, SpecFields};

// -----------------------------------
// Structures
// -----------------------------------

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct FileContent {
    pub request: Request,
    pub response: Response,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Request {
    pub name: String,
    pub annotation: RequestAnnotation,
    pub fields: Vec<Field>,
    pub structures: Vec<Structure>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RequestAnnotation {
    pub api_key: i16,
    pub min_api_version: i16,
    pub max_api_version: i16,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Response {
    pub name: String,
    pub fields: Vec<Field>,
    pub structures: Vec<Structure>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Structure {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Field {
    pub name: String,
    pub value: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotation: Option<FieldAnnotation>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct FieldAnnotation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_version: Option<i16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_version: Option<i16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignorable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

// -----------------------------------
// Macros
// -----------------------------------

macro_rules! make_kf_name {
    ($name:expr) => {
        &format!("Kf{}", $name);
    };
}

// -----------------------------------
// Implement FileContent
// -----------------------------------
impl FileContent {
    /// build BTreeMap of all Fields in <name, [values...]> map format
    pub fn all_fields(&self) -> BTreeMap<String, Vec<String>> {
        let mut all_fields = BTreeMap::new();

        // requests
        for field in &self.request.fields {
            FileContent::add_field_if_unique(&field.name, &field.value, &mut all_fields);
        }
        for req_struct in &self.request.structures {
            for field in &req_struct.fields {
                FileContent::add_field_if_unique(&field.name, &field.value, &mut all_fields);
            }
        }

        // responses
        for field in &self.response.fields {
            FileContent::add_field_if_unique(&field.name, &field.value, &mut all_fields);
        }
        for res_struct in &self.response.structures {
            for field in &res_struct.fields {
                FileContent::add_field_if_unique(&field.name, &field.value, &mut all_fields);
            }
        }

        all_fields
    }

    /// Add field to BTree map if not already there
    fn add_field_if_unique(name: &String, value: &String, map: &mut BTreeMap<String, Vec<String>>) {
        if let Some(map_values) = map.get_mut(name) {
            if !map_values.contains(value) {
                map_values.push(value.clone());
            }
        } else {
            map.insert(name.clone(), vec![value.clone()]);
        }
    }
}

// -----------------------------------
// Functions
// -----------------------------------

/// Convert Request/Response message spec into
pub fn build_file_content(s_req: &SpecMessage, s_res: &SpecMessage) -> FileContent {
    FileContent {
        request: build_request(s_req),
        response: build_response(s_res),
    }
}

/// Convert Request message Template friendly Message
fn build_request(s_msg: &SpecMessage) -> Request {
    let mut structures: Vec<Structure> = vec![];
    let name = make_kf_name!(&s_msg.name).clone();
    let annotation = request_annotation(s_msg);
    let fields = build_fields_and_structs(&s_msg.fields, &mut structures);
    structures.reverse();

    Request {
        name,
        annotation,
        fields,
        structures,
    }
}

/// Convert Response to Template friendly Message
fn build_response(s_msg: &SpecMessage) -> Response {
    let mut structures: Vec<Structure> = vec![];
    let name = make_kf_name!(&s_msg.name).clone();
    let fields = build_fields_and_structs(&s_msg.fields, &mut structures);
    structures.reverse();

    Response {
        name,
        fields,
        structures,
    }
}

/// Loop through SpecFields and generate GUI friendly Fields & Structs
pub fn build_fields_and_structs(
    maybe_s_fields: &Option<SpecFields>,
    parent_structures: &mut Vec<Structure>,
) -> Vec<Field> {
    let mut fields: Vec<Field> = vec![];
    let mut collect_structures: Vec<Structure> = vec![];

    if let Some(s_fields) = maybe_s_fields {
        for s_field in s_fields {
            // check if field and subtree should be skipped
            if skip_field(s_field) {
                continue;
            }

            // generate field
            fields.push(generate_field(s_field));

            // generate structs (if sub-fields)
            if s_field.fields.is_some() {
                let structure = generate_struct(
                    &s_field.typ.custom_value_name(),
                    &s_field.fields,
                    parent_structures,
                );

                collect_structures.insert(0, structure);
            }
        }
    }

    parent_structures.append(&mut collect_structures);

    fields
}

/// Generate Structure
pub fn generate_struct<'a>(
    name: &String,
    maybe_s_fields: &Option<SpecFields>,
    parent_structures: &mut Vec<Structure>,
) -> Structure {
    let name = name.clone();
    let fields = build_fields_and_structs(maybe_s_fields, parent_structures);
    Structure { name, fields }
}

/// Generate Field
pub fn generate_field(s_field: &SpecField) -> Field {
    let name = field_name(&s_field.name, &s_field.map_key, &s_field.entity_type);
    let value = field_value(&s_field.typ, &s_field.nullable_versions);
    let comment = field_comment(&s_field.about);
    let annotation = field_annotation(s_field);

    Field {
        name,
        value,
        comment,
        annotation,
    }
}

/// Generate annotation for request message
pub fn request_annotation(req: &SpecMessage) -> RequestAnnotation {
    let (min_api_version, max_api_version) = req.api_versions.touples();

    RequestAnnotation {
        api_key: req.api_key as i16,
        min_api_version,
        max_api_version,
    }
}

/// Generate field derive based on versions, nullable and defaults
pub fn field_annotation(field: &SpecField) -> Option<FieldAnnotation> {
    let mut annotation = FieldAnnotation::default();

    // provision versions
    if !field.versions.is_zero_plus() {
        let (min_version, max_version) = field.versions.touples();
        annotation.min_version = Some(min_version);
        annotation.max_version = max_version;
    }

    // provision ignorable
    if field.ignorable.unwrap_or(false) {
        annotation.ignorable = Some(true);
    }

    // provision default
    if let Some(default) = &field.default {
        annotation.default = Some(default.value());
    }

    if annotation.min_version.is_some()
        || annotation.ignorable.is_some()
        || annotation.default.is_some()
    {
        Some(annotation)
    } else {
        None
    }
}

/// Generate field name, replace with entity_type if map_key is set
pub fn field_name(name: &String, map_key: &Option<bool>, entity_type: &Option<String>) -> String {
    let new_name = if let Some(map_key) = map_key {
        if *map_key && entity_type.is_some() {
            &entity_type.as_ref().unwrap()
        } else {
            name
        }
    } else {
        name
    };

    new_name.to_snake_case()
}

/// Generate field value, if nullableVersion is set and Typpe is String or Array, make it an Option
pub fn field_value(field_type: &SpecFieldType, nullable_ver: &Option<NullableVersions>) -> String {
    if nullable_ver.is_some() {
        if field_type.is_string_or_array() {
            return format!("Option<{}>", field_type.value());
        }
    }
    field_type.value()
}

/// Converts about to sized (90 column) code comment
pub fn field_comment(about: &Option<String>) -> Option<String> {
    if let Some(text) = about {
        let data = fill(text, 92);
        let mut comment = String::new();
        for line in data.lines() {
            comment.push_str(&format!("/// {}\n", line));
        }
        Some(comment)
    } else {
        None
    }
}

/// Skip fields if they match the following criteria
///     - version is exactly 0.
pub fn skip_field(field: &SpecField) -> bool {
    field.versions.is_zero()
}

// -----------------------------------
// Test Cases
// -----------------------------------

#[cfg(test)]
mod test {
    use std::fs::read_to_string;
    use std::io::Error as IoError;
    use std::io::ErrorKind;
    use std::path::{Path, PathBuf};

    use super::*;

    use crate::file_to_json::file_to_json;
    use crate::json_to_msg::{parse_json_to_request, parse_json_to_response};

    pub fn file_content_decode<T: AsRef<Path>>(path: T) -> Result<FileContent, IoError> {
        let file_str: String = read_to_string(path)?;
        serde_json::from_str(&file_str)
            .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))
    }

    // read files and generate messages
    fn file_to_msgs(
        req_file_str: &'static str,
        res_file_str: &'static str,
    ) -> (SpecMessage, SpecMessage) {
        let mut req_file = PathBuf::new();
        req_file.push(req_file_str);
        let req_json = file_to_json(&req_file);
        assert!(req_json.is_ok());

        let mut res_file = PathBuf::new();
        res_file.push(res_file_str);
        let res_json = file_to_json(&res_file);
        assert!(res_json.is_ok());

        let req_msg = parse_json_to_request(req_json.unwrap());
        let res_msg = parse_json_to_response(res_json.unwrap());
        assert!(req_msg.is_ok());
        assert!(res_msg.is_ok());

        (req_msg.unwrap(), res_msg.unwrap())
    }

    #[test]
    fn test_convert_file_content() {
        let (req_msg, res_msg) = file_to_msgs(
            "./test-data/MetadataRequest.json",
            "./test-data/MetadataResponse.json",
        );
        let file_content_expected =
            file_content_decode(Path::new("./test-data/metadata_file_content.json")).unwrap();
        let file_content = build_file_content(&req_msg, &res_msg);

        println!("{:#?}", file_content);
        assert_eq!(file_content, file_content_expected);
    }
}
