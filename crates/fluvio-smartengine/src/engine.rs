use std::fmt::{self, Debug};

use anyhow::Result;
use derive_builder::Builder;
use tracing::debug;
use wasmtime::{Engine, Module};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput,
};

use crate::init::SmartModuleInit;
use crate::instance::{SmartModuleInstance, SmartModuleInstanceContext};

use crate::metrics::SmartModuleChainMetrics;
use crate::state::WasmState;
use crate::transforms::create_transform;

const DEFAULT_SMARTENGINE_VERSION: i16 = 17;

#[derive(Clone)]
pub struct SmartEngine(Engine);

#[allow(clippy::new_without_default)]
impl SmartEngine {
    pub fn new() -> Self {
        let mut config = wasmtime::Config::default();
        config.consume_fuel(true);
        Self(Engine::new(&config).expect("Config is static"))
    }

    pub(crate) fn new_state(&self) -> WasmState {
        WasmState::new(&self.0)
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
        let mut instances = Vec::with_capacity(self.smart_modules.len());
        let mut state = engine.new_state();
        for (config, bytes) in self.smart_modules {
            let module = Module::new(&engine.0, bytes)?;
            let version = config.version();
            let ctx = SmartModuleInstanceContext::instantiate(
                &mut state,
                module,
                config.params,
                version,
            )?;
            let init = SmartModuleInit::try_instantiate(&ctx, &mut state)?;
            let transform = create_transform(&ctx, config.initial_data, &mut state)?;
            let mut instance = SmartModuleInstance::new(ctx, init, transform);
            instance.init(&mut state)?;
            instances.push(instance);
        }
        Ok(SmartModuleChainInstance {
            store: state,
            instances,
        })
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
pub struct SmartModuleChainInstance {
    store: WasmState,
    instances: Vec<SmartModuleInstance>,
}

impl Debug for SmartModuleChainInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleChainInstance")
    }
}

impl SmartModuleChainInstance {
    #[cfg(test)]
    pub(crate) fn instances(&self) -> &Vec<SmartModuleInstance> {
        &self.instances
    }

    /// A single record is processed thru all smartmodules in the chain.
    /// The output of one smartmodule is the input of the next smartmodule.
    /// A single record may result in multiple records.
    /// The output of the last smartmodule is added to the output of the chain.
    pub fn process(
        &mut self,
        input: SmartModuleInput,
        metric: &SmartModuleChainMetrics,
    ) -> Result<SmartModuleOutput> {
        let raw_len = input.raw_bytes().len();
        debug!(raw_len, "sm raw input");
        metric.add_bytes_in(raw_len as u64);

        let base_offset = input.base_offset();

        if let Some((last, instances)) = self.instances.split_last_mut() {
            let mut next_input = input;

            for instance in instances {
                // pass raw inputs to transform instance
                // each raw input may result in multiple records
                self.store.top_up_fuel();
                let output = instance.process(next_input, &mut self.store)?;
                let fuel_used = self.store.get_used_fuel();
                debug!(fuel_used, "fuel used");
                metric.add_fuel_used(fuel_used);

                if output.error.is_some() {
                    // encountered error, we stop processing and return partial output
                    return Ok(output);
                } else {
                    next_input = output.successes.try_into()?;
                    next_input.set_base_offset(base_offset);
                }
            }

            self.store.top_up_fuel();
            let output = last.process(next_input, &mut self.store)?;
            let fuel_used = self.store.get_used_fuel();
            debug!(fuel_used, "fuel used");
            metric.add_fuel_used(fuel_used);
            let records_out = output.successes.len();
            metric.add_records_out(records_out as u64);
            debug!(records_out, "sm records out");
            Ok(output)
        } else {
            Ok(SmartModuleOutput::new(input.try_into()?))
        }
    }
}

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
    initial_data: SmartModuleInitialData,
    #[builder(default)]
    params: SmartModuleExtraParams,
    // this will be deprecated in the future
    #[builder(default, setter(into, strip_option))]
    version: Option<i16>,
}

impl SmartModuleConfigBuilder {
    /// add initial parameters
    pub fn param(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        let mut new = self;
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
        }
    }
}

#[cfg(test)]
mod test {

    use super::SmartModuleConfig;

    #[test]
    fn test_param() {
        let config = SmartModuleConfig::builder()
            .param("key", "apple")
            .build()
            .unwrap();

        assert_eq!(config.params.get("key"), Some(&"apple".to_string()));
    }
}

#[cfg(test)]
mod chaining_test {

    use std::convert::TryFrom;

    use fluvio_smartmodule::{dataplane::smartmodule::SmartModuleInput, Record};

    use crate::{
        SmartEngine, SmartModuleChainBuilder, SmartModuleConfig, SmartModuleInitialData,
        metrics::SmartModuleChainMetrics,
    };

    const SM_FILTER_INIT: &str = "fluvio_smartmodule_filter_init";
    const SM_MAP: &str = "fluvio_smartmodule_map";

    use crate::fixture::read_wasm_module;

    #[ignore]
    #[test]
    fn test_chain_filter_map() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        let metrics = SmartModuleChainMetrics::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .param("key", "a")
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_INIT),
        );

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_MAP),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");
        assert_eq!(chain.instances().len(), 2);

        let input = vec![Record::new("hello world")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 0); // no records passed

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"APPLE");
        assert_eq!(output.successes[1].value.as_ref(), b"BANANA");
        assert!(metrics.fuel_used() > 0);
        chain.store.top_up_fuel();
        assert_eq!(chain.store.get_used_fuel(), 0);
    }

    const SM_AGGEGRATE: &str = "fluvio_smartmodule_aggregate";

    #[ignore]
    #[test]
    fn test_chain_filter_aggregate() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        let metrics = SmartModuleChainMetrics::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .param("key", "a")
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_INIT),
        );

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .initial_data(SmartModuleInitialData::with_aggregate(
                    "zero".to_string().as_bytes().to_vec(),
                ))
                .build()
                .unwrap(),
            read_wasm_module(SM_AGGEGRATE),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");
        assert_eq!(chain.instances().len(), 2);

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value().to_string(), "zeroapple");
        assert_eq!(output.successes[1].value().to_string(), "zeroapplebanana");

        let input = vec![Record::new("nothing")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 0); // one record passed

        let input = vec![Record::new("elephant")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(
            output.successes[0].value().to_string(),
            "zeroapplebananaelephant"
        );
    }

    #[test]
    fn test_empty_chain() {
        //given
        let engine = SmartEngine::new();
        let chain_builder = SmartModuleChainBuilder::default();
        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        assert_eq!(chain.store.get_used_fuel(), 0);

        let record = vec![Record::new("input")];
        let input = SmartModuleInput::try_from(record).expect("valid input record");
        let metrics = SmartModuleChainMetrics::default();
        //when
        let output = chain.process(input, &metrics).expect("process failed");

        //then
        assert_eq!(output.successes.len(), 1);
        assert_eq!(output.successes[0].value().to_string(), "input");
    }
}
