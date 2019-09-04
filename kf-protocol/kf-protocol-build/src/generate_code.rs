//!
//! # Write Code to File
//!
//! Takes file pairs, generates code, and write to directory
//!

use std::fs::metadata;
use std::io::Error as IoError;
use std::io::ErrorKind;

use crate::constants::WARNING;

use crate::format_code::rustify_code;
use crate::format_code::TemplateFormat;

use crate::file_pairs::{FilePair, FilePairs};
use crate::json_to_msg::parse_json_to_request;
use crate::json_to_msg::parse_json_to_response;

use crate::file_content::build_file_content;
use crate::file_to_json::file_to_json;
use crate::output_to_file::code_to_output_file;
use crate::output_to_file::make_file_from_dir;

// -----------------------------------
// Implementation
// -----------------------------------

/// Generate code and to individual files in directory
pub fn gen_code_and_output_to_dir(
    input_dir: &String,
    dir: &String,
    template: &TemplateFormat,
    skip_formatter: bool,
) -> Result<(), IoError> {
    // ensure output directory exists
    if let Err(err) = metadata(dir) {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("{} - {}", dir, err),
        ));
    }

    // each generate goes into its own file
    let file_pairs = FilePairs::new(input_dir)?;
    for file_pair in &file_pairs.pairs {
        match generate_code_from_files(file_pair, template) {
            Ok(content) => {
                let code = augment_code(&content, skip_formatter)?;
                let mut file = make_file_from_dir(dir, &file_pair.filename)?;

                // add code to file
                match code_to_output_file(&mut file, code) {
                    Ok(()) => {}
                    Err(err) => {
                        return Err(IoError::new(ErrorKind::InvalidData, format!("{}", err)));
                    }
                }
            }
            Err(err) => return Err(IoError::new(ErrorKind::InvalidData, format!("{}", err))),
        }
    }

    Ok(())
}

// -----------------------------------
// Private functions
// -----------------------------------

/// Augment code
/// * add Warning,
/// * run Rust formatter (if not skipped)
fn augment_code(content: &String, skip_formatter: bool) -> Result<String, IoError> {
    let with_warning = format!("{}{}", WARNING, content);
    let new_content = if skip_formatter {
        with_warning
    } else {
        rustify_code(with_warning)?
    };

    Ok(new_content)
}

/// Take a (Request & Response)file  pair, a map, and a template to generate code
fn generate_code_from_files(
    file_pair: &FilePair,
    template: &TemplateFormat,
) -> Result<String, IoError> {
    let req_json = match file_to_json(&file_pair.req_file) {
        Ok(json) => json,
        Err(err) => return Err(IoError::new(ErrorKind::InvalidData, format!("{}", err))),
    };
    let res_json = match file_to_json(&file_pair.res_file) {
        Ok(json) => json,
        Err(err) => return Err(IoError::new(ErrorKind::InvalidData, format!("{}", err))),
    };

    // request/response file to json
    let req_msg = parse_json_to_request(req_json)?;
    let res_msg = parse_json_to_response(res_json)?;

    // use template and file content to generate code
    template.generate_code(&build_file_content(&req_msg, &res_msg))
}
