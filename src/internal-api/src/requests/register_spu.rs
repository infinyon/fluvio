//!
//! # Register SPU
//!
//! SPU sends Register message to the SC to ask permission to join the cluster.
//! SC matches spu-id, token-name and token-secret to ensure this SPU can be authorized.
//!
//! Authorization result:
//!     * FlvErrorCode::None - for success
//!     * FLVErrorCode::SpuNotAuthorized - for error
//!
//! In subsequent releases, Register SPU will carry additional credentials for mTLS
//!
use kf_protocol::api::Request;
use kf_protocol::api::FlvErrorCode;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use flv_types::SpuId;

use crate::InternalScKey;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decode, Encode, Debug, Default)]
pub struct RegisterSpuRequest {
    spu: SpuId,
}

impl Request for RegisterSpuRequest {
    const API_KEY: u16 = InternalScKey::RegisterSpu as u16;
    type Response = RegisterSpuResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct RegisterSpuResponse {
    error_code: FlvErrorCode,
    error_message: Option<String>,
}

// -----------------------------------
// RegisterSpuRequest
// -----------------------------------

impl RegisterSpuRequest {
    pub fn new(spu: SpuId) -> Self {
        Self { spu }
    }

    pub fn spu(&self) -> SpuId {
        self.spu
    }
}

// -----------------------------------
// RegisterSpuResponse
// -----------------------------------

impl RegisterSpuResponse {
    pub fn ok() -> Self {
        RegisterSpuResponse {
            error_code: FlvErrorCode::None,
            error_message: None,
        }
    }

    pub fn failed_registeration() -> Self {
        RegisterSpuResponse {
            error_code: FlvErrorCode::SpuRegisterationFailed,
            error_message: None,
        }
    }

    pub fn is_error(&self) -> bool {
        self.error_code.is_error()
    }

    pub fn error_message(&self) -> String {
        if let Some(ref err_msg) = &self.error_message {
            err_msg.clone()
        } else {
            self.error_code.to_sentence()
        }
    }
}
