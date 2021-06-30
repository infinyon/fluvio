pub use encoding::{SmartStreamRuntimeError, SmartStreamType, SmartStreamInput, SmartStreamOutput};

mod encoding {
    use std::fmt;
    use crate::Offset;
    use crate::record::{Record, RecordData};
    use fluvio_protocol::{Encoder, Decoder};

    /// A type used to pass data into a SmartStream WASM module
    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartStreamInput {
        /// The base offset of this batch of records
        pub base_offset: Offset,
        /// The records for the SmartStream to process
        pub record_data: Vec<u8>,
    }

    /// A type used to return processed records and/or an error from a SmartStream
    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct SmartStreamOutput {
        /// The successfully processed output Records
        pub successes: Vec<Record>,
        /// Any runtime error if one was encountered
        pub error: Option<SmartStreamRuntimeError>,
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

    /// Used to pass both the accumulator and records to an Aggregate smartstream
    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct Aggregate {
        pub accumulator: Vec<u8>,
        pub records: Vec<u8>,
    }
}
