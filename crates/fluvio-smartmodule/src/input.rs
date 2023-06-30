use std::time::Duration;
use std::{collections::BTreeMap, fmt::Display};
use std::fmt;
use std::io::Cursor;

use fluvio_protocol::record::Offset;
use fluvio_protocol::{Encoder, Decoder, record::Record};

pub const SMARTMODULE_LOOKBACK_WITH_AGE_AND_BASE_TIMESTAMP: i16 = 21;

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
    #[fluvio(max_version = SMARTMODULE_BASE_TIMESTMAP)]
    params: SmartModuleExtraParams,
    #[fluvio(min_version = 16, max_version = SMARTMODULE_BASE_TIMESTMAP)]
    join_record: Vec<u8>,
    #[fluvio(min_version = SMARTMODULE_BASE_TIMESTMAP)]
    base_timestamp: i64,
}

impl SmartModuleInput {
    pub fn new(raw_bytes: Vec<u8>, base_offset: Offset, base_timestamp: i64) -> Self {
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

    pub fn base_timestamp(&self) -> i64 {
        self.base_timestamp
    }

    pub fn set_base_timestamp(&mut self, base_timestamp: i64) {
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
}

impl TryFrom<Vec<Record>> for SmartModuleInput {
    type Error = std::io::Error;
    fn try_from(records: Vec<Record>) -> Result<Self, Self::Error> {
        let mut raw_bytes = Vec::new();
        records.encode(
            &mut raw_bytes,
            SMARTMODULE_LOOKBACK_WITH_AGE_AND_BASE_TIMESTAMP,
        )?;
        Ok(SmartModuleInput {
            raw_bytes,
            ..Default::default()
        })
    }
}

impl TryInto<Vec<Record>> for SmartModuleInput {
    type Error = std::io::Error;

    fn try_into(mut self) -> Result<Vec<Record>, Self::Error> {
        Decoder::decode_from(
            &mut Cursor::new(&mut self.raw_bytes),
            SMARTMODULE_LOOKBACK_WITH_AGE_AND_BASE_TIMESTAMP,
        )
    }
}

impl Display for SmartModuleInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SmartModuleInput {{ base_offset: {:?}, record_data: {:?}, join_data: {:#?} }}",
            self.base_offset,
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
        let sm_input: SmartModuleInput = records
            .try_into()
            .expect("records to input conversion failed");

        let records_decoded: Vec<Record> = sm_input
            .try_into()
            .expect("input to records conversion failed");

        //then
        assert_eq!(records_decoded[0].value.as_ref(), b"apple");
        assert_eq!(records_decoded[1].value.as_ref(), b"fruit");
        assert_eq!(records_decoded[2].value.as_ref(), b"banana");
    }
}
