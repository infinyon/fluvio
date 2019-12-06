//!
//! # Format code based son Jinja2 templates
//!

use std::fs::metadata;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;
use std::str;

use serde_json;
use serde_json::value::{from_value, to_value};
use serde_json::Value;
use std::collections::BTreeMap;

use tera::compile_templates;
use tera::Error as TeraError;
use tera::{Context, GlobalFn};

use ::rustfmt_nightly::Config;
use ::rustfmt_nightly::EmitMode;
use ::rustfmt_nightly::Input;
use ::rustfmt_nightly::Session;
use ::rustfmt_nightly::Verbosity;

use crate::file_content::FileContent;

const BASE_FILE: &str = "fluvio_base.j2";
const DEFAULT_TEMPLATE: &str = "./templates/*";

#[derive(Debug, PartialEq)]
pub struct TemplateFormat {
    template_dir: String,
}

// -----------------------------------
// Implement - Template
// -----------------------------------

impl TemplateFormat {
    /// save template directory for custom templates
    pub fn new(maybe_dir: Option<String>) -> Result<Self, IoError> {
        let template_dir = if let Some(dir) = maybe_dir {
            // if set, ensure directory exists
            if let Err(err) = metadata(&dir) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("{} - {}", dir, err),
                ));
            }

            // ensure base file exists
            let base_file = Path::new(&dir).join(BASE_FILE);
            if let Err(err) = metadata(base_file) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("{} is mandatory - {}", BASE_FILE, err),
                ));
            }

            // concatonate wildcard /* to the directory
            Path::new(&dir).join("*").to_string_lossy().to_string()
        } else {
            DEFAULT_TEMPLATE.to_string()
        };

        Ok(TemplateFormat { template_dir })
    }

    /// Templates file are loaded from users CLI input or internal project directory.
    /// These files take the Request and Response messages as parameters to generate.
    /// File content is return in String format
    pub fn generate_code(&self, content: &FileContent) -> Result<String, IoError> {
        let mut context = Context::new();
        context.insert("request", &content.request);
        context.insert("response", &content.response);

        let mut tera = compile_templates!(&self.template_dir);
        tera.register_function("contains_field", make_contains_field(content.all_fields()));
        let code = match tera.render(BASE_FILE, &context) {
            Ok(result) => result,
            Err(err) => return Err(IoError::new(ErrorKind::InvalidInput, format!("{}", err))),
        };

        Ok(code)
    }
}

/// Takes an unformatted string of code and converts to Rustmformatted string
pub fn rustify_code(input: String) -> Result<String, IoError> {
    // configuration
    let mut config = Config::default();
    config.set().emit_mode(EmitMode::Stdout);
    config.set().verbose(Verbosity::Quiet);

    // output buffer
    let mut buf: Vec<u8> = vec![];

    // create a session and transform string (enclowse in braket, so it gets dropped
    // after call, otherwise buf cannot be used)
    {
        let mut session = Session::new(config, Some(&mut buf));
        if let Err(err) = session.format(Input::Text(input)) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("cannot format output: {}", err),
            ));
        }
    }

    // convert to string
    match str::from_utf8(&buf) {
        Ok(result) => Ok(result.to_string()),
        Err(err) => Err(IoError::new(
            ErrorKind::InvalidInput,
            format!("conversion error: {}", err),
        )),
    }
}

// macro function to look-up all fields in Tera templates
fn make_contains_field(all_fields: BTreeMap<String, Vec<String>>) -> GlobalFn {
    Box::new(move |args| -> Result<Value, TeraError> {
        // lookup name
        let req_name = match args.get("name") {
            Some(some_name) => match from_value::<String>(some_name.clone()) {
                Ok(name) => Some(name),
                Err(_) => None,
            },
            None => None,
        };

        // lookup value
        let req_value = match args.get("value") {
            Some(some_value) => match from_value::<String>(some_value.clone()) {
                Ok(value) => Some(value),
                Err(_) => None,
            },
            None => None,
        };

        if let Some(name) = req_name {
            if let Some(value) = req_value {
                if let Some(map_values) = all_fields.get(&name) {
                    if map_values.contains(&value) {
                        return Ok(to_value(true).unwrap());
                    }
                }
            }
        }

        Ok(to_value(false).unwrap())
    })
}
