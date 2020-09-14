//!
//! # Fluvio Error Codes
//!
//! Error code definitions described here.
//!
use serde::Serialize;

use flv_util::string_helper::upper_cammel_case_to_sentence;
use kf_protocol_derive::Encode;
use kf_protocol_derive::Decode;

// -----------------------------------
// Error Definition & Implementation
// -----------------------------------

#[fluvio_kf(encode_discriminant)]
#[repr(i16)]
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy, Serialize)]
pub enum FlvErrorCode {
    // Not an error
    None = 0,

    // Spu errors
    SpuError = 1,
    SpuRegisterationFailed = 2,
    SpuOffline = 3,
    SpuNotFound = 4,
    SpuAlreadyExists = 5,

    // Topic errors
    TopicError = 6,
    TopicNotFound = 7,
    TopicAlreadyExists = 8,
    TopicPendingInitialization = 9,
    TopicInvalidConfiguration = 10,

    // Partition errors
    PartitionPendingInitialization = 11,
    PartitionNotLeader = 12,
}

impl Default for FlvErrorCode {
    fn default() -> FlvErrorCode {
        FlvErrorCode::None
    }
}

impl FlvErrorCode {

    pub fn is_ok(&self) -> bool {
        match self {
            Self::None => true,
            _ => false
        }
    }

    pub fn to_sentence(&self) -> String {
        match self {
            FlvErrorCode::None => "".to_owned(),
            _ => upper_cammel_case_to_sentence(format!("{:?}", self), true),
        }
    }

    pub fn is_error(&self) -> bool {
        !self.is_ok()
    }
}

// -----------------------------------
// Unit Tests
// -----------------------------------

#[cfg(test)]
mod test {

    use std::convert::TryInto;

    use super::FlvErrorCode;

    #[test]
    fn test_flv_error_code_from_conversion() {
        let erro_code: FlvErrorCode = (2 as i16).try_into().expect("convert");
        assert_eq!(erro_code, FlvErrorCode::SpuRegisterationFailed);
    }
}
