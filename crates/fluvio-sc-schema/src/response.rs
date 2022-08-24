#![allow(clippy::assign_op_pattern)]

//!
//! # Response Message
//!
//! Response sent to client. Sends entity name, error code and error message.
//!
use fluvio_protocol::{Encoder, Decoder};
use crate::errors::ErrorCode;

use crate::ApiError;

#[derive(Encoder, Decoder, Default, Debug)]
pub struct Status {
    pub name: String,
    pub error_code: ErrorCode,
    pub error_message: Option<String>,
}

impl Status {
    pub fn new_ok(name: String) -> Self {
        Self {
            name,
            error_code: ErrorCode::None,
            error_message: None,
        }
    }

    pub fn new(name: String, code: ErrorCode, msg: Option<String>) -> Self {
        Self {
            name,
            error_code: code,
            error_message: msg,
        }
    }

    pub fn is_error(&self) -> bool {
        self.error_code.is_error()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn as_result(self) -> Result<(), ApiError> {
        if self.error_code.is_ok() {
            Ok(())
        } else {
            Err(ApiError::Code(self.error_code, self.error_message))
        }
    }
}
