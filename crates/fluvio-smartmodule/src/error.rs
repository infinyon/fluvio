use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

pub use fluvio_protocol::api::smartmodule::{
    SmartModuleRuntimeError, SmartModuleKindError, LegacySmartModuleError,
};

/// Indicates an internal error from within a SmartModule.
//
// The presence of one of these errors most likely indicates a logic bug.
// This error type is `#[repr(i32)]` because these errors are returned
// as the raw return type of a SmartModule WASM function, i.e. the return
// type in `extern "C" fn filter(ptr, len) -> i32`. Positive return values
// indicate the numbers of records, and negative values indicate various
// types of errors.
//
// THEREFORE, THE DISCRIMINANTS FOR ALL VARIANTS ON THIS TYPE MUST BE NEGATIVE
#[repr(i32)]
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[non_exhaustive]
#[fluvio(encode_discriminant)]
pub enum SmartModuleInternalError {
    #[error("encountered unknown error during SmartModule processing")]
    UnknownError = -1,
    #[error("failed to decode SmartModule base input")]
    DecodingBaseInput = -11,
    #[error("failed to decode SmartModule record input")]
    DecodingRecords = -22,
    #[error("failed to encode SmartModule output")]
    EncodingOutput = -33,
    #[error("failed to parse SmartModule extra params")]
    ParsingExtraParams = -44,
    #[error("undefined right record in Join SmartModule")]
    UndefinedRightRecord = -55,
    #[error("Init params are not found")]
    InitParamsNotFound = -60,
}

impl Default for SmartModuleInternalError {
    fn default() -> Self {
        Self::UnknownError
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
pub enum SmartModuleKind {
    Filter,
    Map,
    #[fluvio(min_version = 15)]
    ArrayMap,
    #[fluvio(min_version = 13)]
    Aggregate,
    #[fluvio(min_version = 16)]
    FilterMap,
    #[fluvio(min_version = 16)]
    Join,
    #[fluvio(min_version = 17)]
    Generic,
}

impl Default for SmartModuleKind {
    fn default() -> Self {
        Self::Filter
    }
}

impl fmt::Display for SmartModuleKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use Debug for Display to print variant name
        fmt::Debug::fmt(self, f)
    }
}
