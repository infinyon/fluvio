//!
//! # Fluvio Error Codes
//!
//! Error code definitions described here.
//!

use flv_util::string_helper::upper_cammel_case_to_sentence;
use fluvio_protocol::{Encoder, Decoder};
use crate::smartstream::SmartStreamRuntimeError;

// -----------------------------------
// Error Definition & Implementation
// -----------------------------------

#[repr(i16)]
#[derive(Encoder, Decoder, PartialEq, Debug, Clone)]
pub enum ErrorCode {
    #[fluvio(tag = -1)]
    UnknownServerError,

    // Not an error
    #[fluvio(tag = 0)]
    None,

    #[fluvio(tag = 1)]
    OffsetOutOfRange,
    #[fluvio(tag = 6)]
    NotLeaderForPartition,
    #[fluvio(tag = 10)]
    MessageTooLarge,
    #[fluvio(tag = 13)]
    PermissionDenied,
    #[fluvio(tag = 56)]
    StorageError,

    // Spu errors
    #[fluvio(tag = 1000)]
    SpuError,
    #[fluvio(tag = 1001)]
    SpuRegisterationFailed,
    #[fluvio(tag = 1002)]
    SpuOffline,
    #[fluvio(tag = 1003)]
    SpuNotFound,
    #[fluvio(tag = 1004)]
    SpuAlreadyExists,

    // Topic errors
    #[fluvio(tag = 2000)]
    TopicError,
    #[fluvio(tag = 2001)]
    TopicNotFound,
    #[fluvio(tag = 2002)]
    TopicAlreadyExists,
    #[fluvio(tag = 2003)]
    TopicPendingInitialization,
    #[fluvio(tag = 2004)]
    TopicInvalidConfiguration,
    #[fluvio(tag = 2005)]
    TopicNotProvisioned,
    #[fluvio(tag = 2006)]
    TopicInvalidName,

    // Partition errors
    #[fluvio(tag = 3000)]
    PartitionPendingInitialization,
    #[fluvio(tag = 3001)]
    PartitionNotLeader,

    // Stream Fetch error
    #[fluvio(tag = 3002)]
    FetchSessionNotFoud,

    // SmartStream errors
    #[fluvio(tag = 4000)]
    SmartStreamError(SmartStreamError),

    // Managed Connector Errors
    #[fluvio(tag = 5000)]
    ManagedConnectorError,
    #[fluvio(tag = 5001)]
    ManagedConnectorNotFound,
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

/// A type representing the possible errors that may occur during SmartStream execution.
// This is also where we can update our error representation in the future
// TODO: Add variant for reporting panics
#[derive(Debug, Clone, PartialEq, Encoder, Decoder)]
pub enum SmartStreamError {
    Runtime(SmartStreamRuntimeError),
}

impl Default for SmartStreamError {
    fn default() -> Self {
        Self::Runtime(Default::default())
    }
}

// -----------------------------------
// Unit Tests
// -----------------------------------

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_protocol_tags_unchanged() {
        macro_rules! assert_tag {
            ($variant:expr, $tag:expr, $version:expr) => {{
                let mut data = Vec::new();
                let mut value = ErrorCode::default();

                fluvio_protocol::Encoder::encode(&$variant, &mut data, $version)
                    .expect(&format!("Failed to encode {}", stringify!($variant)));
                assert_eq!(
                    data,
                    ($tag as i16).to_be_bytes(),
                    "Data check failed for {}",
                    stringify!($variant)
                );
                fluvio_protocol::Decoder::decode(
                    &mut value,
                    &mut std::io::Cursor::new(&data),
                    $version,
                )
                .expect(&format!("Failed to decode {}", stringify!($variant)));
                assert_eq!(
                    &value,
                    &$variant,
                    "Value check failed for {}",
                    stringify!($variant)
                );
            }};
        }

        assert_tag!(ErrorCode::UnknownServerError, -1, 0);
        assert_tag!(ErrorCode::None, 0, 0);
        assert_tag!(ErrorCode::OffsetOutOfRange, 1, 0);
        assert_tag!(ErrorCode::NotLeaderForPartition, 6, 0);
        assert_tag!(ErrorCode::MessageTooLarge, 10, 0);
        assert_tag!(ErrorCode::PermissionDenied, 13, 0);
        assert_tag!(ErrorCode::StorageError, 56, 0);

        // Spu errors
        assert_tag!(ErrorCode::SpuError, 1000, 0);
        assert_tag!(ErrorCode::SpuRegisterationFailed, 1001, 0);
        assert_tag!(ErrorCode::SpuOffline, 1002, 0);
        assert_tag!(ErrorCode::SpuNotFound, 1003, 0);
        assert_tag!(ErrorCode::SpuAlreadyExists, 1004, 0);

        // Topic errors
        assert_tag!(ErrorCode::TopicError, 2000, 0);
        assert_tag!(ErrorCode::TopicNotFound, 2001, 0);
        assert_tag!(ErrorCode::TopicAlreadyExists, 2002, 0);
        assert_tag!(ErrorCode::TopicPendingInitialization, 2003, 0);
        assert_tag!(ErrorCode::TopicInvalidConfiguration, 2004, 0);
        assert_tag!(ErrorCode::TopicNotProvisioned, 2005, 0);

        // Partition errors
        assert_tag!(ErrorCode::PartitionPendingInitialization, 3000, 0);
        assert_tag!(ErrorCode::PartitionNotLeader, 3001, 0);

        // Stream Fetch error
        assert_tag!(ErrorCode::FetchSessionNotFoud, 3002, 0);
    }
}
