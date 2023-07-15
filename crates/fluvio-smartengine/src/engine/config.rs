use std::time::Duration;

use derive_builder::Builder;

use fluvio_protocol::Version;
use fluvio_smartmodule::SMARTMODULE_LTA_VERSION;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;

pub const DEFAULT_SMARTENGINE_VERSION: Version = SMARTMODULE_LTA_VERSION;

/// Initial seed data to passed, this will be send back as part of the output
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SmartModuleInitialData {
    None,
    Aggregate { accumulator: Vec<u8> },
}

impl SmartModuleInitialData {
    pub fn with_aggregate(accumulator: Vec<u8>) -> Self {
        Self::Aggregate { accumulator }
    }
}

impl Default for SmartModuleInitialData {
    fn default() -> Self {
        Self::None
    }
}

/// SmartModule configuration
#[derive(Builder)]
pub struct SmartModuleConfig {
    #[builder(default, setter(strip_option))]
    pub(crate) initial_data: SmartModuleInitialData,
    #[builder(default)]
    pub(crate) params: SmartModuleExtraParams,
    // this will be deprecated in the future
    #[builder(default, setter(into, strip_option))]
    pub(crate) version: Option<i16>,
    #[builder(default)]
    pub(crate) lookback: Option<Lookback>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookback {
    Last(u64),
    Age { age: Duration, last: u64 },
}

impl SmartModuleConfigBuilder {
    /// add initial parameters
    pub fn param(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        let new = self;
        let mut params = new.params.take().unwrap_or_default();
        params.insert(key.into(), value.into());
        new.params = Some(params);
        new
    }
}

impl SmartModuleConfig {
    pub fn builder() -> SmartModuleConfigBuilder {
        SmartModuleConfigBuilder::default()
    }

    pub(crate) fn version(&self) -> i16 {
        self.version.unwrap_or(DEFAULT_SMARTENGINE_VERSION)
    }

    pub fn set_lookback(&mut self, lookback: Option<Lookback>) {
        self.lookback = lookback;
    }
}

#[cfg(feature = "transformation")]
impl From<crate::transformation::TransformationStep> for SmartModuleConfig {
    fn from(step: crate::transformation::TransformationStep) -> Self {
        Self {
            initial_data: SmartModuleInitialData::None,
            params: step
                .with
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<std::collections::BTreeMap<String, String>>()
                .into(),
            version: None,
            lookback: step.lookback.map(|l| l.into()),
        }
    }
}

#[cfg(feature = "transformation")]
impl From<crate::transformation::Lookback> for Lookback {
    fn from(value: crate::transformation::Lookback) -> Self {
        match value.age {
            Some(age) => Self::Age {
                age,
                last: value.last,
            },
            None => Self::Last(value.last),
        }
    }
}

impl From<&fluvio_smartmodule::dataplane::smartmodule::Lookback> for Lookback {
    fn from(value: &fluvio_smartmodule::dataplane::smartmodule::Lookback) -> Self {
        match value.age {
            Some(age) => Self::Age {
                age,
                last: value.last,
            },
            None => Self::Last(value.last),
        }
    }
}
