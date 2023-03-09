//!
//! # Fluvio Error Codes
//!
//! Error code definitions described here.
//!
use flv_util::string_helper::upper_cammel_case_to_sentence;

use crate::{Encoder, Decoder, api::RequestKind};

// -----------------------------------
// Error Definition & Implementation
// -----------------------------------

#[repr(i16)]
#[derive(thiserror::Error, Encoder, Decoder, Eq, PartialEq, Debug, Clone)]
#[non_exhaustive]
#[derive(Default)]
pub enum ErrorCode {
    #[fluvio(tag = -1)]
    #[error("An unknown server error occurred")]
    UnknownServerError,

    // Not an error
    #[fluvio(tag = 0)]
    #[error("ErrorCode indicated success. If you see this it is likely a bug.")]
    #[default]
    None,

    #[fluvio(tag = 2)]
    #[error("Other error: {0}")]
    Other(String),

    #[fluvio(tag = 1)]
    #[error("Offset out of range")]
    OffsetOutOfRange,
    #[fluvio(tag = 6)]
    #[error("the given SPU is not the leader for the partition")]
    NotLeaderForPartition,
    #[fluvio(tag = 7)]
    #[error("the request '{kind}' exceeded the timeout {timeout_ms} ms")]
    RequestTimedOut { timeout_ms: u64, kind: RequestKind },
    #[fluvio(tag = 10)]
    #[error("the message is too large to send")]
    MessageTooLarge,
    #[fluvio(tag = 13)]
    #[error("permission denied")]
    PermissionDenied,
    #[fluvio(tag = 56)]
    #[error("a storage error occurred")]
    StorageError,
    #[fluvio(tag = 60)]
    #[error("invalid create request")]
    InvalidCreateRequest,
    #[fluvio(tag = 61)]
    #[error("invalid Delete request")]
    InvalidDeleteRequest,

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

    // Legacy SmartModule errors
    #[cfg(feature = "smartmodule")]
    #[deprecated(since = "0.9.13")]
    #[fluvio(tag = 4000)]
    #[error("a legacy SmartModule error occurred")]
    LegacySmartModuleError(#[from] crate::smartmodule::LegacySmartModuleError),

    // Managed Connector Errors
    #[fluvio(tag = 5000)]
    #[error("an error occurred while managing a connector")]
    ManagedConnectorError,

    #[fluvio(tag = 5001)]
    #[error("the managed connector was not found")]
    ManagedConnectorNotFound,

    #[fluvio(tag = 5002)]
    #[error("an error occurred while managing a connector")]
    ManagedConnectorAlreadyExists,

    // SmartModule Errors
    #[fluvio(tag = 6000)]
    #[error("an error occurred while managing a SmartModule")]
    SmartModuleError,
    #[fluvio(tag = 6001)]
    #[error("SmartModule {name} was not found")]
    SmartModuleNotFound { name: String },
    #[fluvio(tag = 6002)]
    #[error("SmartModule is invalid: {error}")]
    SmartModuleInvalid { error: String, name: Option<String> },
    #[fluvio(tag = 6003)]
    #[error("SmartModule is not a valid '{kind}' SmartModule due to {error}. Are you missing a #[smartmodule({kind})] attribute?")]
    SmartModuleInvalidExports { error: String, kind: String },
    #[fluvio(tag = 6004)]
    #[error("SmartModule transform error {0}")]
    SmartModuleRuntimeError(super::smartmodule::SmartModuleTransformRuntimeError),
    #[fluvio(tag = 6005)]
    #[error("Error initializing {0} SmartModule Chain")]
    SmartModuleChainInitError(String),
    #[fluvio(tag = 6006)]
    #[error("SmartModule init error {0}")]
    SmartModuleInitError(super::smartmodule::SmartModuleInitRuntimeError),

    // TableFormat Errors
    #[fluvio(tag = 7000)]
    #[error("a tableformat error occurred")]
    TableFormatError,
    #[fluvio(tag = 7001)]
    #[error("the tableformat was not found")]
    TableFormatNotFound,
    #[fluvio(tag = 7002)]
    #[error("the tableformat already exists")]
    TableFormatAlreadyExists,

    // DerivedStream Object Errors
    #[fluvio(tag = 8000)]
    #[error("DerivedStream object error")]
    DerivedStreamObjectError,
    #[fluvio(tag = 8001)]
    #[error("the derivedstream was not found")]
    DerivedStreamNotFound(String),
    #[fluvio(tag = 8002)]
    #[error("the derivedstream join data cannot be fetched")]
    DerivedStreamJoinFetchError,
    #[fluvio(tag = 8003)]
    #[error("the derivedstream {0} is invalid")]
    DerivedStreamInvalid(String),
    // Compression errors
    #[fluvio(tag = 9000)]
    #[error("a compression error occurred in the SPU")]
    CompressionError,
}

impl ErrorCode {
    pub fn is_ok(&self) -> bool {
        matches!(self, ErrorCode::None)
    }

    pub fn to_sentence(&self) -> String {
        match self {
            ErrorCode::None => "".to_owned(),
            _ => upper_cammel_case_to_sentence(format!("{self:?}"), true),
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
                    data[..2],
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
        assert_tag!(
            ErrorCode::RequestTimedOut {
                kind: RequestKind::Produce,
                timeout_ms: 1
            },
            7,
            0
        );
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
