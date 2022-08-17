use std::fmt;

use fluvio_protocol::{
    Encoder, Decoder,
    record::{Offset, RecordData, Record},
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
pub enum SmartModuleKindError {
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

impl Default for SmartModuleKindError {
    fn default() -> Self {
        Self::Filter
    }
}

impl fmt::Display for SmartModuleKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use Debug for Display to print variant name
        fmt::Debug::fmt(self, f)
    }
}

/// A type used to capture and serialize errors from within a SmartModule
#[derive(thiserror::Error, Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
pub struct SmartModuleRuntimeError {
    /// Error hint: meant for users, not for code
    pub hint: String,
    /// The offset of the Record that had a runtime error
    pub offset: Offset,
    /// The type of SmartModule that had a runtime error
    pub kind: SmartModuleKindError,
    /// The Record key that caused this error
    pub record_key: Option<RecordData>,
    /// The Record value that caused this error
    pub record_value: RecordData,
}

impl SmartModuleRuntimeError {
    pub fn new(
        record: &Record,
        base_offset: Offset,
        kind: SmartModuleKindError,
        error: eyre::Error,
    ) -> Self {
        let hint = format!("{:?}", error);
        let offset = base_offset + record.preamble.offset_delta();
        let record_key = record.key.clone();
        let record_value = record.value.clone();
        Self {
            hint,
            offset,
            kind,
            record_key,
            record_value,
        }
    }
}

impl fmt::Display for SmartModuleRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = self
            .record_key
            .as_ref()
            .map(display_record_data)
            .unwrap_or_else(|| "NULL".to_string());
        let value = display_record_data(&self.record_value);
        write!(
            f,
            "{}\n\n\
            SmartModule Info: \n    \
            Type: {}\n    \
            Offset: {}\n    \
            Key: {}\n    \
            Value: {}",
            self.hint, self.kind, self.offset, key, value,
        )
    }
}

fn display_record_data(record: &RecordData) -> String {
    match std::str::from_utf8(record.as_ref()) {
        Ok(s) => s.to_string(),
        _ => format!("Binary: {} bytes", record.as_ref().len()),
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
