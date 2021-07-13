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

#[repr(i16)]
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum ErrorCode {
    UnknownServerError = -1,

    // Not an error
    None = 0,

    OffsetOutOfRange = 1,
    NotLeaderForPartition = 6,
    MessageTooLarge = 10,
    PermissionDenied = 13,
    StorageError = 56,

    // Spu errors
    SpuError = 1000,
    SpuRegisterationFailed = 1001,
    SpuOffline = 1002,
    SpuNotFound = 1003,
    SpuAlreadyExists = 1004,

    // Topic errors
    TopicError = 2000,
    TopicNotFound = 2001,
    TopicAlreadyExists = 2002,
    TopicPendingInitialization = 2003,
    TopicInvalidConfiguration = 2004,
    TopicNotProvisioned = 2005,

    // Partition errors
    PartitionPendingInitialization = 3000,
    PartitionNotLeader = 3001,

    // Stream Fetch error
    FetchSessionNotFoud = 3002,

    // SmartStream errors
    SmartStreamUserError = 4000,
}

impl Default for ErrorCode {
    fn default() -> ErrorCode {
        ErrorCode::None
    }
}

impl ErrorCode {
    pub fn is_ok(&self) -> bool {
        matches!(self, ErrorCode::None)
    }

    pub fn to_sentence(self) -> String {
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
    fn test_error_code_from_conversion() {
        let erro_code: ErrorCode = (1001i16).try_into().expect("convert");
        assert_eq!(erro_code, ErrorCode::SpuRegisterationFailed);
    }
}
