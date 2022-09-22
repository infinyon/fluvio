use std::{collections::BTreeMap, fmt::Display};
use std::fmt;

use fluvio_protocol::record::Offset;
use fluvio_protocol::{Encoder, Decoder, record::Record};

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

    pub fn insert(&mut self, key: String, value: String) {
        self.inner.insert(key, value);
    }
}

/// Old Common data that gets passed as input to every SmartModule WASM module
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleInput {
    /// The base offset of this batch of records
    pub base_offset: Offset,
    /// The records for the SmartModule to process
    pub record_data: Vec<u8>,
    /// This is deprecrated, extra parameters should not be passed, they will be removed in the future
    #[deprecated]
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

/// Input to SmartModule Init
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleInitInput {
    pub params: SmartModuleExtraParams,
}
