pub use encoding::{
    SmartStreamRuntimeError, SmartStreamRuntimeErrorBuilder, SmartStreamType, SmartStreamOutput,
};

pub type Result<T> = std::result::Result<T, SmartStreamRuntimeErrorBuilder>;

mod encoding {
    use std::fmt;
    use crate::Offset;
    use crate::record::{Record, RecordData};
    use fluvio_protocol::{Encoder, Decoder};

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

    #[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
    pub struct SmartStreamRuntimeErrorBuilder {
        /// Error hint: meant for users, not for code
        hint: String,
        /// The type of SmartStream that had a runtime error
        kind: SmartStreamType,
        /// The offset delta of the Record that had a runtime error
        // Offset delta is relative to the beginning of a batch
        offset_delta: Offset,
        /// The absolute offset, after we calculate it
        offset: Option<Offset>,
        /// A preview of the Record key that caused this error
        record_key: Option<RecordData>,
        /// A preview of the Record value that caused this error
        record_value: Option<RecordData>,
    }

    impl SmartStreamRuntimeErrorBuilder {
        pub fn new(err: eyre::Error, offset_delta: Offset, kind: SmartStreamType) -> Self {
            Self {
                hint: format!("{:?}", err),
                kind,
                offset_delta,
                offset: None,
                record_key: None,
                record_value: None,
            }
        }

        /// Apply a base offset to this error to derive the absolute offset
        pub fn base_offset(&mut self, base: Offset) -> &mut Self {
            self.offset = Some(base + self.offset_delta);
            self
        }

        /// Generate a preview of the Record that failed
        pub fn record(&mut self, record: &Record) -> &mut Self {
            self.record_key = record.key.clone();
            self.record_value = Some(record.value.clone());
            self
        }

        /// Produce the SmartStreamRuntimeError
        pub fn build(&self) -> SmartStreamRuntimeError {
            SmartStreamRuntimeError {
                offset: self.offset.clone().unwrap(),
                kind: self.kind.clone(),
                record_key: self.record_key.clone(),
                record_value: self.record_value.clone().unwrap(),
                hint: self.hint.clone(),
            }
        }
    }

    /// A type used to return processed records and/or an error from a SmartStream
    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct SmartStreamOutput {
        pub successes: Vec<Record>,
        pub error: Option<SmartStreamRuntimeErrorBuilder>,
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
}
