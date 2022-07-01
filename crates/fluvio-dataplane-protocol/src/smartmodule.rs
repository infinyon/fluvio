pub use encoding::{
    SmartModuleRuntimeError, SmartModuleInternalError, SmartModuleKind, SmartModuleInput,
    SmartModuleAggregateInput, SmartModuleOutput, SmartModuleExtraParams,
    SmartModuleAggregateOutput,
};

mod encoding {
    use std::fmt::{self, Display};
    use crate::Offset;
    use crate::record::{Record, RecordData};
    use fluvio_protocol::{Encoder, Decoder};
    use std::collections::BTreeMap;

    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartModuleExtraParams {
        inner: BTreeMap<String, String>,
    }

    impl From<BTreeMap<String, String>> for SmartModuleExtraParams {
        fn from(inner: BTreeMap<String, String>) -> SmartModuleExtraParams {
            SmartModuleExtraParams { inner }
        }
    }

    impl SmartModuleExtraParams {
        pub fn get(&self, key: &str) -> Option<&String> {
            self.inner.get(key)
        }
    }

    /// Common data that gets passed as input to every SmartModule WASM module
    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartModuleInput {
        /// The base offset of this batch of records
        pub base_offset: Offset,
        /// The records for the SmartModule to process
        pub record_data: Vec<u8>,
        pub params: SmartModuleExtraParams,
        #[fluvio(min_version = 16)]
        pub join_record: Vec<u8>,
    }
    impl std::convert::TryFrom<Vec<Record>> for SmartModuleInput {
        type Error = std::io::Error;
        fn try_from(records: Vec<Record>) -> Result<Self, Self::Error> {
            let mut record_data = Vec::new();
            records.encode(&mut record_data, 0)?;
            Ok(SmartModuleInput {
                record_data,
                ..Default::default()
            })
        }
    }

    impl Display for SmartModuleInput {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "SmartModuleInput {{ base_offset: {:?}, record_data: {:?}, join_data: {:#?} }}",
                self.base_offset,
                self.record_data.len(),
                self.join_record.len()
            )
        }
    }

    /// A type to pass input to an Aggregate SmartModule WASM module
    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartModuleAggregateInput {
        /// The base input required by all SmartModules
        pub base: SmartModuleInput,
        /// The current value of the Aggregate's accumulator
        pub accumulator: Vec<u8>,
    }
    /// A type used to return processed records and/or an error from a SmartModule
    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct SmartModuleOutput {
        /// The successfully processed output Records
        pub successes: Vec<Record>,
        /// Any runtime error if one was encountered
        pub error: Option<SmartModuleRuntimeError>,
    }

    /// A type used to return processed records and/or an error from an Aggregate SmartModule
    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct SmartModuleAggregateOutput {
        /// The base output required by all SmartModules
        pub base: SmartModuleOutput,
        #[fluvio(min_version = 16)]
        pub accumulator: Vec<u8>,
    }

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
    #[derive(thiserror::Error, Debug, Clone, PartialEq, Encoder, Decoder)]
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
    }

    impl Default for SmartModuleInternalError {
        fn default() -> Self {
            Self::UnknownError
        }
    }

    /// A type used to capture and serialize errors from within a SmartModule
    #[derive(thiserror::Error, Debug, Default, Clone, PartialEq, Encoder, Decoder)]
    pub struct SmartModuleRuntimeError {
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

    impl SmartModuleRuntimeError {
        pub fn new(
            record: &Record,
            base_offset: Offset,
            kind: SmartModuleKind,
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

    #[derive(Debug, Clone, PartialEq, Encoder, Decoder)]
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
}
