pub mod metrics;
/// SmartModule configuration
mod config;
pub use config::{
    SmartModuleConfig, SmartModuleConfigBuilder, SmartModuleConfigBuilderError,
    SmartModuleInitialData,
};
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};

use self::metrics::SmartModuleChainMetrics;
/// The common traits to be implemented by different WASM engines
mod common;
mod error;

#[cfg(test)]
mod fixture;

pub type WasmSlice = (i32, i32, u32);
pub type Version = i16;

#[derive(Clone)]
pub struct SmartEngine {
    pub(crate) inner: SmartEngineImp,
}

#[allow(clippy::new_without_default)]
impl SmartEngine {
    pub fn new() -> Self {
        Self {
            inner: SmartEngineImp::new(),
        }
    }
}

impl std::fmt::Debug for SmartEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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

    pub fn initialize(self, engine: &SmartEngine) -> anyhow::Result<SmartModuleChainInstance> {
        initialize_imp(self, engine)
    }
}

impl<T: Into<Vec<u8>>> From<(SmartModuleConfig, T)> for SmartModuleChainBuilder {
    fn from(pair: (SmartModuleConfig, T)) -> Self {
        let mut result = Self::default();
        result.add_smart_module(pair.0, pair.1.into());
        result
    }
}

pub struct SmartModuleChainInstance {
    pub(crate) inner: SmartModuleChainInstanceImp,
}

impl std::fmt::Debug for SmartModuleChainInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
    ) -> anyhow::Result<SmartModuleOutput> {
        self.inner.process(input, metric)
    }
}

cfg_if::cfg_if! {
    // TODO: turn on this check when ready
    // #[cfg(all(feature = "wasmedge_engine", feature = "wasmtime_engine"))]
    // compile_error!(
    //     "Both `wasmedge_engine` and `wasmtime_engine` features are enabled, but \
    //     only one WASM runtime is supported at a time"
    // );


    if #[cfg(feature = "wasmtime-engine")] {
        mod wasmtime;
        use self::wasmtime::{SmartEngineImp, initialize_imp, SmartModuleChainInstanceImp};
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "wasmedge-engine")] {
        mod wasmedge;
        mod test_use {
            pub use super::wasmedge::{SmartEngine, SmartModuleChainBuilder, SmartModuleChainInstance};
        }
    }
}