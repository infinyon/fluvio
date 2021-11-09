pub use encoding::{
    SmartStreamRuntimeError, SmartStreamInternalError, SmartStreamType, SmartStreamInput,
    SmartStreamAggregateInput, SmartStreamOutput, SmartStreamExtraParams,
};

mod encoding {
    use std::fmt;
    use crate::Offset;
    use crate::record::{Record, RecordData};
    use fluvio_protocol::{Encoder, Decoder};
    use std::collections::BTreeMap;

    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartStreamExtraParams {
        inner: BTreeMap<String, String>,
    }

    impl From<BTreeMap<String, String>> for SmartStreamExtraParams {
        fn from(inner: BTreeMap<String, String>) -> SmartStreamExtraParams {
            SmartStreamExtraParams { inner }
        }
    }

    impl SmartStreamExtraParams {
        pub fn get(&self, key: &str) -> Option<&String> {
            self.inner.get(key)
        }
    }

    /// Common data that gets passed as input to every SmartStream WASM module
    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartStreamInput {
        /// The base offset of this batch of records
        pub base_offset: Offset,
        /// The records for the SmartStream to process
        pub record_data: Vec<u8>,
        #[fluvio(min_version = 16)]
        pub join_record: Vec<u8>,
        pub params: SmartStreamExtraParams,
    }
    impl std::convert::TryFrom<Vec<Record>> for SmartStreamInput {
        type Error = std::io::Error;
        fn try_from(records: Vec<Record>) -> Result<Self, Self::Error> {
            let mut record_data = Vec::new();
            let _ = records.encode(&mut record_data, 0)?;
            Ok(SmartStreamInput {
                record_data,
                ..Default::default()
            })
        }
    }

    /// A type to pass input to an Aggregate SmartStream WASM module
    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartStreamAggregateInput {
        /// The base input required by all SmartStreams
        pub base: SmartStreamInput,
        /// The current value of the Aggregate's accumulator
        pub accumulator: Vec<u8>,
    }
    /// A type used to return processed records and/or an error from a SmartStream
    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct SmartStreamOutput {
        /// The successfully processed output Records
        pub successes: Vec<Record>,
        /// Any runtime error if one was encountered
        pub error: Option<SmartStreamRuntimeError>,
    }

    /// Indicates an internal error from within a SmartStream.
    //
    // The presence of one of these errors most likely indicates a logic bug.
    // This error type is `#[repr(i32)]` because these errors are returned
    // as the raw return type of a Smartstream WASM function, i.e. the return
    // type in `extern "C" fn filter(ptr, len) -> i32`. Positive return values
    // indicate the numbers of records, and negative values indicate various
    // types of errors.
    //
    // THEREFORE, THE DISCRIMINANTS FOR ALL VARIANTS ON THIS TYPE MUST BE NEGATIVE
    #[repr(i32)]
    #[derive(thiserror::Error, Debug, Clone, PartialEq, Encoder, Decoder)]
    #[fluvio(encode_discriminant)]
    pub enum SmartStreamInternalError {
        #[error("encountered unknown error during Smartstream processing")]
        UnknownError = -1,
        #[error("failed to decode Smartstream base input")]
        DecodingBaseInput = -11,
        #[error("failed to decode Smartstream record input")]
        DecodingRecords = -22,
        #[error("failed to encode Smartstream output")]
        EncodingOutput = -33,
        #[error("failed to parse Smartstream extra params")]
        ParsingExtraParams = -44,
        #[error("undefined right record in Join smartstream")]
        UndefinedRightRecord = -55,
    }

    impl Default for SmartStreamInternalError {
        fn default() -> Self {
            Self::UnknownError
        }
    }

    /// A type used to capture and serialize errors from within a SmartStream
    #[derive(thiserror::Error, Debug, Default, Clone, PartialEq, Encoder, Decoder)]
    pub struct SmartStreamRuntimeError {
        /// Error hint: meant for users, not for code
        pub hint: String,
        /// The offset of the Record that had a runtime error
        pub offset: Offset,
        /// The type of SmartStream that had a runtime error
        pub kind: SmartStreamType,
        /// The Record key that caused this error
        pub record_key: Option<RecordData>,
        /// The Record value that caused this error
        pub record_value: RecordData,
    }

    impl SmartStreamRuntimeError {
        pub fn new(
            record: &Record,
            base_offset: Offset,
            kind: SmartStreamType,
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

    impl fmt::Display for SmartStreamRuntimeError {
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
                SmartStream Info: \n    \
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
    pub enum SmartStreamType {
        Filter,
        Map,
        ArrayMap,
        FilterMap,
        Join,
        Aggregate,
    }

    impl Default for SmartStreamType {
        fn default() -> Self {
            Self::Filter
        }
    }

    impl fmt::Display for SmartStreamType {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // Use Debug for Display to print variant name
            fmt::Debug::fmt(self, f)
        }
    }
}
