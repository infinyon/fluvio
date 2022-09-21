use fluvio_protocol::{Encoder, Decoder};

/// Error during processing SmartModule transform error
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
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq, Encoder, Default, Decoder)]
#[non_exhaustive]
#[fluvio(encode_discriminant)]
pub enum SmartModuleTransformErrorStatus {
    #[error("encountered unknown error during SmartModule processing")]
    #[default]
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
}

#[repr(i32)]
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq, Encoder, Default, Decoder)]
#[non_exhaustive]
#[fluvio(encode_discriminant)]
pub enum SmartModuleInitErrorStatus {
    #[error("encountered unknown error during SmartModule processing")]
    #[default]
    UnknownError = -1,
    #[error("Error during initialization")]
    InitError = -2,
    #[error("failed to decode SmartModule init input")]
    DecodingInput = -10,
    #[error("failed to encode SmartModule init output")]
    EncodingOutput = -11,
}

/// Error during processing SmartModule initialization
#[derive(thiserror::Error, Debug)]
pub enum SmartModuleInitError {
    #[error("Missing param {0}")]
    MissingParam(String),
}
