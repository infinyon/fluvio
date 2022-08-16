pub use encoding::*;
pub use error::*;
pub use payload::*;

mod error {
    use fluvio_protocol::{Encoder, Decoder};

    use super::SmartModuleRuntimeError;

    /// Deprecated. A type representing the possible errors that may occur during DerivedStream execution.
    #[derive(thiserror::Error, Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
    pub enum LegacySmartModuleError {
        #[error("Runtime error")]
        Runtime(#[from] SmartModuleRuntimeError),
        #[error("WASM Module error: {0}")]
        InvalidWasmModule(String),
        #[error("WASM module is not a valid '{0}' DerivedStream. Are you missing a #[smartmodule({0})] attribute?")]
        InvalidDerivedStreamModule(String),
    }

    impl Default for LegacySmartModuleError {
        fn default() -> Self {
            Self::Runtime(Default::default())
        }
    }
}

mod payload {

    use std::io::Read;
    use std::{io, borrow::Cow};
    use std::fmt::{Debug, self};

    use flate2::{
        Compression,
        bufread::{GzEncoder, GzDecoder},
    };

    use fluvio_protocol::{Encoder, Decoder};

    use super::SmartModuleExtraParams;

    /// The request payload when using a Consumer SmartModule.
    ///
    /// This includes the WASM content as well as the type of SmartModule being used.
    /// It also carries any data that is required for specific types of SmartModules.
    /// TODO: remove in 0.10
    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct LegacySmartModulePayload {
        pub wasm: SmartModuleWasmCompressed,
        pub kind: SmartModuleKind,
        pub params: SmartModuleExtraParams,
    }

    /// The request payload when using a Consumer SmartModule.
    ///
    /// This includes the WASM module name as well as the invocation being used.
    /// It also carries any data that is required for specific invocations of SmartModules.
    #[derive(Debug, Default, Clone, Encoder, Decoder)]
    pub struct SmartModuleInvocation {
        pub wasm: SmartModuleInvocationWasm,
        pub kind: SmartModuleKind,
        pub params: SmartModuleExtraParams,
    }

    #[derive(Debug, Clone, Encoder, Decoder)]
    pub enum SmartModuleInvocationWasm {
        /// Name of SmartModule
        Predefined(String),
        /// Compressed WASM module payload using Gzip
        AdHoc(Vec<u8>),
    }

    impl SmartModuleInvocationWasm {
        pub fn adhoc_from_bytes(bytes: &[u8]) -> io::Result<Self> {
            Ok(Self::AdHoc(zip(bytes)?))
        }
    }

    impl Default for SmartModuleInvocationWasm {
        fn default() -> Self {
            Self::AdHoc(Vec::new())
        }
    }

    /// Indicates the type of SmartModule as well as any special data required
    #[derive(Debug, Clone, Encoder, Decoder)]
    pub enum SmartModuleKind {
        Filter,
        Map,
        #[fluvio(min_version = ARRAY_MAP_WASM_API)]
        ArrayMap,
        Aggregate {
            accumulator: Vec<u8>,
        },
        #[fluvio(min_version = ARRAY_MAP_WASM_API)]
        FilterMap,
        #[fluvio(min_version = SMART_MODULE_API)]
        Join(String),
        #[fluvio(min_version = SMART_MODULE_API)]
        JoinStream {
            topic: String,
            derivedstream: String,
        },
        #[fluvio(min_version = GENERIC_SMARTMODULE_API)]
        Generic(SmartModuleContextData),
    }

    impl std::fmt::Display for SmartModuleKind {
        fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            let name = match self {
                SmartModuleKind::Filter => "filter",
                SmartModuleKind::Map => "map",
                SmartModuleKind::ArrayMap => "array_map",
                SmartModuleKind::Aggregate { .. } => "aggregate",
                SmartModuleKind::FilterMap => "filter_map",
                SmartModuleKind::Join(..) => "join",
                SmartModuleKind::JoinStream { .. } => "join_stream",
                SmartModuleKind::Generic(..) => "smartmodule",
            };
            out.write_str(name)
        }
    }

    #[derive(Debug, Clone, Encoder, Decoder)]
    pub enum SmartModuleContextData {
        None,
        Aggregate {
            accumulator: Vec<u8>,
        },
        Join(String),
        JoinStream {
            topic: String,
            derivedstream: String,
        },
    }

    impl Default for SmartModuleContextData {
        fn default() -> Self {
            Self::None
        }
    }

    impl Default for SmartModuleKind {
        fn default() -> Self {
            Self::Filter
        }
    }

    /// Different possible representations of WASM modules.
    ///
    /// In a fetch request, a WASM module may be given directly in the request
    /// as raw bytes.
    ///
    // TODO: remove in 0.10
    #[derive(Clone, Encoder, Decoder)]
    pub enum SmartModuleWasmCompressed {
        Raw(Vec<u8>),
        /// compressed WASM module payload using Gzip
        #[fluvio(min_version = 14)]
        Gzip(Vec<u8>),
        // TODO implement named WASM modules once we have a WASM store
        // Url(String),
    }

    fn zip(raw: &[u8]) -> io::Result<Vec<u8>> {
        let mut encoder = GzEncoder::new(raw, Compression::default());
        let mut buffer = Vec::with_capacity(raw.len());
        encoder.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    fn unzip(compressed: &[u8]) -> io::Result<Vec<u8>> {
        let mut decoder = GzDecoder::new(compressed);
        let mut buffer = Vec::with_capacity(compressed.len());
        decoder.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    impl SmartModuleWasmCompressed {
        /// returns the gzip-compressed WASM module bytes
        pub fn to_gzip(&mut self) -> io::Result<()> {
            if let Self::Raw(raw) = self {
                *self = Self::Gzip(zip(raw.as_ref())?);
            }
            Ok(())
        }

        /// returns the raw WASM module bytes
        pub fn to_raw(&mut self) -> io::Result<()> {
            if let Self::Gzip(gzipped) = self {
                *self = Self::Raw(unzip(gzipped)?);
            }
            Ok(())
        }

        /// get the raw bytes of the WASM module
        pub fn get_raw(&self) -> io::Result<Cow<[u8]>> {
            Ok(match self {
                Self::Raw(raw) => Cow::Borrowed(raw),
                Self::Gzip(gzipped) => Cow::Owned(unzip(gzipped.as_ref())?),
            })
        }
    }

    impl Default for SmartModuleWasmCompressed {
        fn default() -> Self {
            Self::Raw(Vec::new())
        }
    }

    impl Debug for SmartModuleWasmCompressed {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Raw(bytes) => f
                    .debug_tuple("Raw")
                    .field(&format!("{} bytes", bytes.len()))
                    .finish(),
                Self::Gzip(bytes) => f
                    .debug_tuple("Gzip")
                    .field(&format!("{} bytes", bytes.len()))
                    .finish(),
            }
        }
    }

    #[cfg(test)]
    mod tests {

        use super::*;

        #[test]
        fn test_encode_smartmodulekind() {
            let mut dest = Vec::new();
            let value: SmartModuleKind = SmartModuleKind::Filter;
            value.encode(&mut dest, 0).expect("should encode");
            assert_eq!(dest.len(), 1);
            assert_eq!(dest[0], 0x00);
        }

        #[test]
        fn test_decode_smartmodulekind() {
            let bytes = vec![0x01];
            let mut value: SmartModuleKind = Default::default();
            value
                .decode(&mut std::io::Cursor::new(bytes), 0)
                .expect("should decode");
            assert!(matches!(value, SmartModuleKind::Map));
        }

        #[test]
        fn test_encode_smartmodulewasm() {
            let mut dest = Vec::new();
            let value: SmartModuleWasmCompressed =
                SmartModuleWasmCompressed::Raw(vec![0xde, 0xad, 0xbe, 0xef]);
            value.encode(&mut dest, 0).expect("should encode");
            println!("{:02x?}", &dest);
            assert_eq!(dest.len(), 9);
            assert_eq!(dest[0], 0x00);
            assert_eq!(dest[1], 0x00);
            assert_eq!(dest[2], 0x00);
            assert_eq!(dest[3], 0x00);
            assert_eq!(dest[4], 0x04);
            assert_eq!(dest[5], 0xde);
            assert_eq!(dest[6], 0xad);
            assert_eq!(dest[7], 0xbe);
            assert_eq!(dest[8], 0xef);
        }

        #[test]
        fn test_decode_smartmodulewasm() {
            let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef];
            let mut value: SmartModuleWasmCompressed = Default::default();
            value
                .decode(&mut std::io::Cursor::new(bytes), 0)
                .expect("should decode");
            let inner = match value {
                SmartModuleWasmCompressed::Raw(inner) => inner,
                #[allow(unreachable_patterns)]
                _ => panic!("should decode to SmartModuleWasm::Raw"),
            };
            assert_eq!(inner.len(), 4);
            assert_eq!(inner[0], 0xde);
            assert_eq!(inner[1], 0xad);
            assert_eq!(inner[2], 0xbe);
            assert_eq!(inner[3], 0xef);
        }

        #[test]
        fn test_gzip_smartmoduleinvocationwasm() {
            let bytes = vec![0xde, 0xad, 0xbe, 0xef];
            let value: SmartModuleInvocationWasm =
                SmartModuleInvocationWasm::adhoc_from_bytes(&bytes).expect("should encode");
            if let SmartModuleInvocationWasm::AdHoc(compressed_bytes) = value {
                let decompressed_bytes = unzip(&compressed_bytes).expect("should decompress");
                assert_eq!(decompressed_bytes, bytes);
            } else {
                panic!("not adhoc")
            }
        }
    }
}

mod encoding {
    use std::fmt::{self, Display};
    use std::collections::BTreeMap;

    use fluvio_protocol::{Encoder, Decoder};

    use crate::Offset;
    use crate::record::{Record, RecordData};

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
}
