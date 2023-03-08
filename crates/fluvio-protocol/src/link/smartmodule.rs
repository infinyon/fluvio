/// This module is kept in this crate because it is referenced in the ErrorCode
///
use std::fmt;

use crate::{
    Encoder, Decoder,
    record::{Offset, RecordData, Record},
};

/// A type used to capture and serialize errors from within a SmartModule
#[derive(thiserror::Error, Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
pub struct SmartModuleTransformRuntimeError {
    /// Error hint: meant for users, not for code
    pub hint: String,
    /// The offset of the Record that had a runtime error
    pub offset: Offset,
    /// The type of SmartModule that had a runtime error
    pub kind: SmartModuleKind,
    /// The Record key that caused this error
    pub record_key: Option<RecordData>,
    /// The Record value that caused this error
    pub record_value: RecordData,
}

impl SmartModuleTransformRuntimeError {
    pub fn new(
        record: &Record,
        base_offset: Offset,
        kind: SmartModuleKind,
        error: eyre::Error,
    ) -> Self {
        let hint = format!("{error:?}");
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

impl fmt::Display for SmartModuleTransformRuntimeError {
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
    #[fluvio(tag = 0)]
    Filter,
    #[fluvio(tag = 1)]
    Map,
    #[fluvio(min_version = 15, tag = 2)]
    ArrayMap,
    #[fluvio(min_version = 13, tag = 3)]
    Aggregate,
    #[fluvio(min_version = 16, tag = 4)]
    FilterMap,
    #[fluvio(min_version = 16, tag = 5)]
    Join,
    #[fluvio(min_version = 17, tag = 6)]
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

/// Deprecated. A type representing the possible errors that may occur during DerivedStream execution.
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
pub enum LegacySmartModuleError {
    #[error("Runtime error")]
    #[fluvio(tag = 0)]
    Runtime(#[from] SmartModuleTransformRuntimeError),
    #[error("WASM Module error: {0}")]
    #[fluvio(tag = 1)]
    InvalidWasmModule(String),
    #[error("WASM module is not a valid '{0}' DerivedStream. Are you missing a #[smartmodule({0})] attribute?")]
    #[fluvio(tag = 2)]
    InvalidDerivedStreamModule(String),
}

impl Default for LegacySmartModuleError {
    fn default() -> Self {
        Self::Runtime(Default::default())
    }
}

/// A type used to capture and serialize errors from within a SmartModule
#[derive(thiserror::Error, Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
pub struct SmartModuleInitRuntimeError {
    /// Error hint: meant for users, not for code
    pub hint: String,
}

impl SmartModuleInitRuntimeError {
    pub fn new(error: eyre::Error) -> Self {
        let hint = format!("{error:?}");
        Self { hint }
    }
}

impl fmt::Display for SmartModuleInitRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\n\n\
            SmartModule Init Error: \n",
            self.hint
        )
    }
}
