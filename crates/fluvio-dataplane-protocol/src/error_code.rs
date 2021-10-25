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
#[derive(thiserror::Error, Encoder, Decoder, PartialEq, Debug, Clone)]
pub enum ErrorCode {
    #[fluvio(tag = -1)]
    #[error("An unknown server error occurred")]
    UnknownServerError,

    // Not an error
    #[fluvio(tag = 0)]
    #[error("ErrorCode indicated success. If you see this it is likely a bug.")]
    None,

    #[fluvio(tag = 1)]
    #[error("Offset out of range")]
    OffsetOutOfRange,
    #[fluvio(tag = 6)]
    #[error("the given SPU is not the leader for the partition")]
    NotLeaderForPartition,
    #[fluvio(tag = 10)]
    #[error("the message is too large to send")]
    MessageTooLarge,
    #[fluvio(tag = 13)]
    #[error("permission denied")]
    PermissionDenied,
    #[fluvio(tag = 56)]
    #[error("a storage error occurred")]
    StorageError,

    // Spu errors
    #[fluvio(tag = 1000)]
    #[error("an error occurred on the SPU")]
    SpuError,
    #[fluvio(tag = 1001)]
    #[error("failed to register an SPU")]
    SpuRegisterationFailed,
    #[fluvio(tag = 1002)]
    #[error("the SPU is offline")]
    SpuOffline,
    #[fluvio(tag = 1003)]
    #[error("the SPU was not found")]
    SpuNotFound,
    #[fluvio(tag = 1004)]
    #[error("the SPU already exists")]
    SpuAlreadyExists,

    // Topic errors
    #[fluvio(tag = 2000)]
    #[error("a topic error occurred")]
    TopicError,
    #[fluvio(tag = 2001)]
    #[error("the topic was not found")]
    TopicNotFound,
    #[fluvio(tag = 2002)]
    #[error("the topic already exists")]
    TopicAlreadyExists,
    #[fluvio(tag = 2003)]
    #[error("the topic has not been initialized")]
    TopicPendingInitialization,
    #[fluvio(tag = 2004)]
    #[error("the topic configuration is invalid")]
    TopicInvalidConfiguration,
    #[fluvio(tag = 2005)]
    #[error("the topic is not provisioned")]
    TopicNotProvisioned,
    #[fluvio(tag = 2006)]
    #[error("the topic name is invalid")]
    TopicInvalidName,

    // Partition errors
    #[fluvio(tag = 3000)]
    #[error("the partition is not initialized")]
    PartitionPendingInitialization,
    #[fluvio(tag = 3001)]
    #[error("the partition is not a leader")]
    PartitionNotLeader,

    // Stream Fetch error
    #[fluvio(tag = 3002)]
    #[error("the fetch session was not found")]
    FetchSessionNotFoud,

    // SmartStream errors
    #[fluvio(tag = 4000)]
    #[error("a SmartStream error occurred")]
    SmartStreamError(#[from] SmartStreamError),

    // Managed Connector Errors
    #[fluvio(tag = 5000)]
    #[error("an error occurred while managing a connector")]
    ManagedConnectorError,
    #[fluvio(tag = 5001)]
    #[error("the managed connector was not found")]
    ManagedConnectorNotFound,

    // Smart Module Errors
    #[fluvio(tag = 6000)]
    #[error("an error occurred while managing a smart module")]
    SmartModuleError,
    #[fluvio(tag = 6001)]
    #[error("the smart module was not found")]
    SmartModuleNotFound,

    // Table Errors
    #[fluvio(tag = 7000)]
    #[error("a table error occurred")]
    TableError,
    #[fluvio(tag = 7001)]
    #[error("the table was not found")]
    TableNotFound,
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
#[derive(thiserror::Error, Debug, Clone, PartialEq, Encoder, Decoder)]
pub enum SmartStreamError {
    #[error("Runtime error")]
    Runtime(#[from] SmartStreamRuntimeError),
    #[error("WASM Module error: {0}")]
    InvalidWasmModule(String),
    #[error("WASM module is not a valid '{0}' SmartStream. Are you missing a #[smartstream({0})] attribute?")]
    InvalidSmartStreamModule(String),
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

    #[test]
    fn test_errorcode_impls_error() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<ErrorCode>();
    }
}
