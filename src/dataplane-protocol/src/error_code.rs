//!
//! # Fluvio Error Codes
//!
//! Error code definitions described here.
//!


use flv_util::string_helper::upper_cammel_case_to_sentence;
use crate::derive::Encode;
use crate::derive::Decode;

// -----------------------------------
// Error Definition & Implementation
// -----------------------------------

#[fluvio(encode_discriminant)]
#[repr(i16)]
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
pub enum ErrorCode {
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

impl Default for ErrorCode {
    fn default() -> ErrorCode {
        ErrorCode::None
    }
}

impl ErrorCode {

    pub fn is_ok(&self) -> bool {
        match self {
            Self::None => true,
            _ => false
        }
    }

    pub fn to_sentence(&self) -> String {
        match self {
            ErrorCode::None => "".to_owned(),
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

    use super::ErrorCode;

    #[test]
    fn test_flv_error_code_from_conversion() {
        let erro_code: ErrorCode = (2 as i16).try_into().expect("convert");
        assert_eq!(erro_code, ErrorCode::SpuRegisterationFailed);
    }
}
