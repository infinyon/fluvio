//!
//! # Fluvio Streaming Controller - Response handlers
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use sc_api::errors::FlvErrorCode;

use crate::error::CliError;

/// Handler for SC reponse codes (successful messages have Error code of None)
pub fn handle_sc_response(
    name: &String,
    label: &'static str,
    operation: &'static str,
    prepend_validation: &'static str,
    error_code: &FlvErrorCode,
    error_msg: &Option<String>,
) -> Result<String, CliError> {
    match error_code {
        // success
        FlvErrorCode::None => {
            if let Some(ref msg) = error_msg {
                Ok(format!(
                    "{}{} '{}' {} successfully, {}",
                    prepend_validation, label, name, operation, msg
                ))
            } else {
                Ok(format!(
                    "{}{} '{}' {} successfully",
                    prepend_validation, label, name, operation
                ))
            }
        }

        // error
        _ => {
            let err_msg = if let Some(err_msg) = error_msg {
                err_msg.clone()
            } else {
                format!("{} '{}' {}", label, name, error_code.to_sentence())
            };

            Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                format!("{}{}", prepend_validation, err_msg),
            )))

        }
    }
}