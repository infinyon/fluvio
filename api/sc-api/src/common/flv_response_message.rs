//!
//! # Response Message
//!
//! Response sent to client. Sends entity name, error code and error message.
//!
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::FlvErrorCode;

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvResponseMessage {
    pub name: String,
    pub error_code: FlvErrorCode,
    pub error_message: Option<String>,
}

impl FlvResponseMessage {
    pub fn new_ok(name: String) -> Self {
        FlvResponseMessage {
            name: name,
            error_code: FlvErrorCode::None,
            error_message: None,
        }
    }

    pub fn new(name: String, code: FlvErrorCode, msg: Option<String>) -> Self {
        FlvResponseMessage {
            name: name,
            error_code: code,
            error_message: msg,
        }
    }

    pub fn is_error(&self) -> bool {
        self.error_code != FlvErrorCode::None
    }
}
