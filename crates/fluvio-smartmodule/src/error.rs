use fluvio_protocol::{Encoder, Decoder};

/// Error during processing SmartModule Instance processing
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
pub enum SmartModuleInstanceProcessError {
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
    #[error("Init params are not found")]
    InitParamsNotFound = -60,
    #[error("encountered unknown error in Init params parsing")]
    InitParamsParse = -61,
}

#[repr(i32)]
pub enum SmartModuleInstanceStatus {
    Success(u32),
    SmartModuleInstanceProcessError
}


pub enum SmartModuleInitError {



}