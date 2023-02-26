use std::fmt::{self, Debug};
use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};
use crate::config::*;
use crate::metrics::SmartModuleChainMetrics;

#[derive(Clone)]
pub struct SmartEngine();

#[allow(clippy::new_without_default)]
impl SmartEngine {
    pub fn new() -> Self {
        todo!()
    }
}

impl Debug for SmartEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleEngine")
    }
}

/// Building SmartModule
#[derive(Default)]
pub struct SmartModuleChainBuilder {
    smart_modules: Vec<(SmartModuleConfig, Vec<u8>)>,
}

impl SmartModuleChainBuilder {
    /// Add SmartModule with a single transform and init
    pub fn add_smart_module(&mut self, config: SmartModuleConfig, bytes: Vec<u8>) {
        self.smart_modules.push((config, bytes))
    }

    /// stop adding smartmodule and return SmartModuleChain that can be executed
    pub fn initialize(self, engine: &SmartEngine) -> Result<SmartModuleChainInstance> {
        todo!()
    }
}

impl<T: Into<Vec<u8>>> From<(SmartModuleConfig, T)> for SmartModuleChainBuilder {
    fn from(pair: (SmartModuleConfig, T)) -> Self {
        let mut result = Self::default();
        result.add_smart_module(pair.0, pair.1.into());
        result
    }
}

/// SmartModule Chain Instance that can be executed
pub struct SmartModuleChainInstance {}

impl Debug for SmartModuleChainInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleChainInstance")
    }
}

impl SmartModuleChainInstance {
    /// A single record is processed thru all smartmodules in the chain.
    /// The output of one smartmodule is the input of the next smartmodule.
    /// A single record may result in multiple records.
    /// The output of the last smartmodule is added to the output of the chain.
    pub fn process(
        &mut self,
        input: SmartModuleInput,
        metric: &SmartModuleChainMetrics,
    ) -> Result<SmartModuleOutput> {
        todo!()
    }
}
