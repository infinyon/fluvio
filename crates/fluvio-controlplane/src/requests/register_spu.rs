#![allow(clippy::assign_op_pattern)]

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
use fluvio_protocol::api::Request;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_types::SpuId;

use crate::InternalScKey;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decoder, Encoder, Debug, Default)]
pub struct RegisterSpuRequest {
    spu: SpuId,
}

impl Request for RegisterSpuRequest {
    const API_KEY: u16 = InternalScKey::RegisterSpu as u16;
    type Response = RegisterSpuResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct RegisterSpuResponse {
    error_code: ErrorCode,
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
            error_code: ErrorCode::None,
            error_message: None,
        }
    }

    #[deprecated = "Replace by failed_registration"]
    pub fn failed_registeration() -> Self {
        Self::failed_registration()
    }

    pub fn failed_registration() -> Self {
        RegisterSpuResponse {
            error_code: ErrorCode::SpuRegisterationFailed,
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
