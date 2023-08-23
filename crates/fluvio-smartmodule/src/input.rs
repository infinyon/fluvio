use std::io::Cursor;
use std::time::Duration;
use std::{collections::BTreeMap, fmt::Display};
use std::fmt;
use fluvio_protocol::{Decoder, Encoder, Version};
use fluvio_protocol::record::{Offset, Record};
use fluvio_protocol::types::Timestamp;

use crate::SmartModuleRecord;

/// SmartModule Version with support for Lookback with Age and Timestamps,
/// LTA is the acronym for Lookback, Timestamps, and Age.
/// This version is used for encoding and decoding [`SmartModuleInput`]
pub const SMARTMODULE_TIMESTAMPS_VERSION: Version = 22;

#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleExtraParams {
    inner: BTreeMap<String, String>,
    #[fluvio(min_version = 20)]
    lookback: Option<Lookback>,
}

impl From<BTreeMap<String, String>> for SmartModuleExtraParams {
    fn from(inner: BTreeMap<String, String>) -> SmartModuleExtraParams {
        SmartModuleExtraParams {
            inner,
            ..Default::default()
        }
    }
}

impl SmartModuleExtraParams {
    pub fn new(params: BTreeMap<String, String>, lookback: Option<Lookback>) -> Self {
        Self {
            inner: params,
            lookback,
        }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.inner.get(key)
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.inner.insert(key, value);
    }

    pub fn lookback(&self) -> Option<&Lookback> {
        self.lookback.as_ref()
    }

    pub fn set_lookback(&mut self, lookback: Option<Lookback>) {
        self.lookback = lookback;
    }
}

#[derive(Debug, Default, Clone, Encoder, Decoder, PartialEq, Eq)]
pub struct Lookback {
    pub last: u64,
    #[fluvio(min_version = 21)]
    pub age: Option<Duration>,
}

impl Lookback {
    pub fn last(last: u64) -> Self {
        Self {
            last,
            ..Default::default()
        }
    }

    pub fn age(age: Duration, last: Option<u64>) -> Self {
        Self {
            last: last.unwrap_or_default(),
            age: Some(age),
        }
    }
}

/// A single SmartModule input record
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleInput {
    /// The base offset of this batch of records
    base_offset: Offset,
    /// encoded version of Record
    raw_bytes: Vec<u8>,
    /// This is deprecrated, extra parameters should not be passed, they will be removed in the future
    #[deprecated]
    #[fluvio(max_version = 22)]
    params: SmartModuleExtraParams,
    #[fluvio(min_version = 16, max_version = 22)]
    join_record: Vec<u8>,
    /// The base timestamp of this batch of records
    #[fluvio(min_version = 22)]
    base_timestamp: Timestamp,
}

impl SmartModuleInput {
    pub fn new(raw_bytes: Vec<u8>, base_offset: Offset, base_timestamp: Timestamp) -> Self {
        Self {
            base_offset,
            raw_bytes,
            base_timestamp,
            ..Default::default()
        }
    }

    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    pub fn set_base_offset(&mut self, base_offset: Offset) {
        self.base_offset = base_offset;
    }

    pub fn base_timestamp(&self) -> Timestamp {
        self.base_timestamp
    }

    pub fn set_base_timestamp(&mut self, base_timestamp: Timestamp) {
        self.base_timestamp = base_timestamp;
    }

    pub fn raw_bytes(&self) -> &[u8] {
        &self.raw_bytes
    }

    pub fn into_raw_bytes(self) -> Vec<u8> {
        self.raw_bytes
    }

    pub fn parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.raw_bytes, self.join_record)
    }

    /// Creates an instance of [`Record`] from the raw bytes and ignoring the
    /// base offset and timestamp. This method is used to keep backwards
    /// compatibility with SmartModule engines previous to Version `21`.
    #[deprecated = "use SmartModuleRecord instead. Read more here: https://www.fluvio.io/smartmodules/smdk/smartmodulerecord/."]
    pub fn try_into_records(mut self, version: Version) -> Result<Vec<Record>, std::io::Error> {
        Decoder::decode_from(&mut Cursor::new(&mut self.raw_bytes), version)
    }

    /// Attempts to map the internally encoded records into a vector of
    /// [`SmartModuleRecord`] by decoding the raw bytes and filling up the base
    /// offset and timestamp fields.
    pub fn try_into_smartmodule_records(
        self,
        version: Version,
    ) -> Result<Vec<SmartModuleRecord>, std::io::Error> {
        let base_offset = self.base_offset();
        let base_timestamp = self.base_timestamp();
        let records_input = self.into_raw_bytes();
        let mut records: Vec<Record> = vec![];

        Decoder::decode(&mut records, &mut Cursor::new(records_input), version)?;

        let records = records
            .into_iter()
            .map(|inner_record| SmartModuleRecord {
                inner_record,
                base_offset,
                base_timestamp,
            })
            .collect();

        Ok(records)
    }

    /// Attempts to map the [`Record`] vector and build a `SmartModuleInput`
    /// instance from it.
    pub fn try_from_records(
        records: Vec<Record>,
        version: Version,
    ) -> Result<Self, std::io::Error> {
        let mut raw_bytes = Vec::new();

        records.encode(&mut raw_bytes, version)?;

        Ok(SmartModuleInput {
            raw_bytes,
            ..Default::default()
        })
    }
}

impl Display for SmartModuleInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SmartModuleInput {{ base_offset: {:?}, base_timestamp: {:?}, record_data: {:?}, join_data: {:#?} }}",
            self.base_offset,
            self.base_timestamp,
            self.raw_bytes.len(),
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

/// Input to SmartModule Init
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleInitInput {
    pub params: SmartModuleExtraParams,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_to_sm_input_and_back() {
        //given
        let records = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];

        //when
        #[allow(deprecated)]
        let sm_input: SmartModuleInput =
            SmartModuleInput::try_from_records(records, SMARTMODULE_TIMESTAMPS_VERSION)
                .expect("records to input conversion failed");

        #[allow(deprecated)]
        let records_decoded: Vec<Record> = sm_input
            .try_into_records(SMARTMODULE_TIMESTAMPS_VERSION)
            .expect("input to records conversion failed");

        //then
        assert_eq!(records_decoded[0].value.as_ref(), b"apple");
        assert_eq!(records_decoded[1].value.as_ref(), b"fruit");
        assert_eq!(records_decoded[2].value.as_ref(), b"banana");
    }

    #[test]
    fn sets_the_provided_value_as_timestamp() {
        let mut sm_input = SmartModuleInput::new(vec![0, 1, 2, 3], 0, 0);

        assert_eq!(sm_input.base_timestamp, 0);
        assert_eq!(sm_input.base_timestamp(), 0);

        sm_input.set_base_timestamp(1234);

        assert_eq!(sm_input.base_timestamp, 1234);
        assert_eq!(sm_input.base_timestamp(), 1234);
    }
}
