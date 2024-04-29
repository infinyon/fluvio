use std::fmt::{self, Debug};
use std::future::Future;

use anyhow::Result;
use fluvio_smartmodule::Record;
use tracing::debug;
use wasmtime::{Engine, Module};

use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};

use crate::SmartModuleConfig;
use crate::engine::config::{Lookback, DEFAULT_SMARTENGINE_VERSION};

use super::init::SmartModuleInit;
use super::instance::{SmartModuleInstance, SmartModuleInstanceContext};

use super::limiter::StoreResourceLimiter;
use super::look_back::SmartModuleLookBack;
use super::metrics::SmartModuleChainMetrics;
use super::state::WasmState;
use super::transforms::create_transform;

// 1 GB
const DEFAULT_STORE_MEMORY_LIMIT: usize = 1_000_000_000;

#[derive(Clone)]
pub struct SmartEngine(Engine);

#[allow(clippy::new_without_default)]
impl SmartEngine {
    pub fn new() -> Self {
        let mut config = wasmtime::Config::default();
        config.consume_fuel(true);
        Self(Engine::new(&config).expect("Config is static"))
    }

    pub(crate) fn new_state(&self, store_limiter: StoreResourceLimiter) -> WasmState {
        WasmState::new(&self.0, store_limiter)
    }
}

impl Debug for SmartEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleEngine")
    }
}

/// Building SmartModule
pub struct SmartModuleChainBuilder {
    smart_modules: Vec<(SmartModuleConfig, Vec<u8>)>,
    store_limiter: StoreResourceLimiter,
}

impl SmartModuleChainBuilder {
    /// Add SmartModule with a single transform and init
    pub fn add_smart_module(&mut self, config: SmartModuleConfig, bytes: Vec<u8>) {
        self.smart_modules.push((config, bytes))
    }

    pub fn set_store_memory_limit(&mut self, max_memory_bytes: usize) {
        self.store_limiter.set_memory_size(max_memory_bytes);
    }

    /// stop adding smartmodule and return SmartModuleChain that can be executed
    pub fn initialize(self, engine: &SmartEngine) -> Result<SmartModuleChainInstance> {
        let mut instances = Vec::with_capacity(self.smart_modules.len());
        let mut state = engine.new_state(self.store_limiter);
        for (config, bytes) in self.smart_modules {
            let module = Module::new(&engine.0, bytes)?;
            let version = config.version();
            let ctx = SmartModuleInstanceContext::instantiate(
                &mut state,
                module,
                config.params,
                version,
                config.lookback,
            )?;
            let init = SmartModuleInit::try_instantiate(&ctx, &mut state)?;
            let look_back = SmartModuleLookBack::try_instantiate(&ctx, &mut state)?;
            let transform = create_transform(&ctx, config.initial_data, &mut state)?;
            let mut instance = SmartModuleInstance::new(ctx, init, look_back, transform, version);

            instance.call_init(&mut state)?;
            instances.push(instance);
        }

        Ok(SmartModuleChainInstance {
            store: state,
            instances,
        })
    }
}

impl Default for SmartModuleChainBuilder {
    fn default() -> Self {
        let mut store_limiter = StoreResourceLimiter::default();
        store_limiter.set_memory_size(DEFAULT_STORE_MEMORY_LIMIT);
        Self {
            smart_modules: Default::default(),
            store_limiter,
        }
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
        let base_timestamp = input.base_timestamp();

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

                if let Some(ref smerr) = output.error {
                    // encountered error, we stop processing and return partial output
                    tracing::error!(err=?smerr);
                    return Ok(output);
                } else {
                    next_input =
                        SmartModuleInput::try_from_records(output.successes, instance.version())?;
                    next_input.set_base_offset(base_offset);
                    next_input.set_base_timestamp(base_timestamp);
                }
            }

            self.store.top_up_fuel();
            let output = last.process(next_input, &mut self.store)?;
            if let Some(ref smerr) = output.error {
                tracing::error!(err=?smerr);
            }
            let fuel_used = self.store.get_used_fuel();
            debug!(fuel_used, "fuel used");
            metric.add_fuel_used(fuel_used);
            let records_out = output.successes.len();
            metric.add_records_out(records_out as u64);
            debug!(records_out, "sm records out");
            Ok(output)
        } else {
            #[allow(deprecated)]
            let records = input.try_into_records(DEFAULT_SMARTENGINE_VERSION)?;

            Ok(SmartModuleOutput::new(records))
        }
    }

    pub async fn look_back<F, R>(
        &mut self,
        read_fn: F,
        metrics: &SmartModuleChainMetrics,
    ) -> Result<()>
    where
        R: Future<Output = Result<Vec<Record>>>,
        F: Fn(Lookback) -> R,
    {
        debug!("look_back on chain with {} instances", self.instances.len());

        for instance in self.instances.iter_mut() {
            if let Some(lookback) = instance.lookback() {
                debug!("look_back on instance");
                let records: Vec<Record> = read_fn(lookback).await?;
                let input: SmartModuleInput =
                    SmartModuleInput::try_from_records(records, instance.version())?;

                metrics.add_bytes_in(input.raw_bytes().len() as u64);
                self.store.top_up_fuel();

                let result = instance.call_look_back(input, &mut self.store);
                let fuel_used = self.store.get_used_fuel();

                debug!(fuel_used, "fuel used");
                metrics.add_fuel_used(fuel_used);
                result?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use crate::SmartModuleConfig;

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

    use fluvio_protocol::record::Record;
    use fluvio_protocol::link::smartmodule::SmartModuleLookbackRuntimeError;
    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

    use crate::engine::error::EngineError;
    use crate::engine::config::{Lookback, DEFAULT_SMARTENGINE_VERSION};

    use super::super::{
        SmartEngine, SmartModuleChainBuilder, SmartModuleConfig, SmartModuleInitialData,
        metrics::SmartModuleChainMetrics,
    };

    const SM_FILTER_INIT: &str = "fluvio_smartmodule_filter_init";
    const SM_MAP: &str = "fluvio_smartmodule_map";
    const SM_FILTER_LOOK_BACK: &str = "fluvio_smartmodule_filter_lookback";

    use super::super::fixture::read_wasm_module;

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
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
                &metrics,
            )
            .expect("process");
        assert_eq!(output.successes.len(), 0); // no records passed

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
                &metrics,
            )
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
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
                &metrics,
            )
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value().to_string(), "zeroapple");
        assert_eq!(output.successes[1].value().to_string(), "zeroapplebanana");

        let input = vec![Record::new("nothing")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
                &metrics,
            )
            .expect("process");
        assert_eq!(output.successes.len(), 0); // one record passed

        let input = vec![Record::new("elephant")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
                &metrics,
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(
            output.successes[0].value().to_string(),
            "zeroapplebananaelephant"
        );
    }

    #[ignore]
    #[test]
    fn test_chain_filter_look_back() {
        //given
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        let metrics = SmartModuleChainMetrics::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .lookback(Some(Lookback::Last(1)))
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_LOOK_BACK),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        // when
        fluvio_future::task::run_block_on(chain.look_back(
            |lookback| {
                assert_eq!(lookback, Lookback::Last(1));
                async { Ok(vec![Record::new("2")]) }
            },
            &metrics,
        ))
        .expect("chain look_back");

        // then
        let input = vec![Record::new("1"), Record::new("2"), Record::new("3")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
                &metrics,
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(output.successes[0].value().to_string(), "3");
        assert!(metrics.fuel_used() > 0);
        assert_eq!(metrics.invocation_count(), 2);
    }

    #[ignore]
    #[test]
    fn test_chain_filter_look_back_error_propagated() {
        //given
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        let metrics = SmartModuleChainMetrics::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .lookback(Some(Lookback::Last(1)))
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_LOOK_BACK),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        // when
        let res = fluvio_future::task::run_block_on(chain.look_back(
            |lookback| {
                assert_eq!(lookback, Lookback::Last(1));
                async { Ok(vec![Record::new("wrong str")]) }
            },
            &metrics,
        ));

        // then
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err()
                .downcast::<SmartModuleLookbackRuntimeError>()
                .expect("downcasted"),
            SmartModuleLookbackRuntimeError {
                hint: "invalid digit found in string".to_string(),
                offset: 0,
                record_key: None,
                record_value: "wrong str".to_string().into()
            }
        );
        assert!(metrics.fuel_used() > 0);
        assert_eq!(metrics.invocation_count(), 1);
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
        let input = SmartModuleInput::try_from_records(record, DEFAULT_SMARTENGINE_VERSION)
            .expect("valid input record");
        let metrics = SmartModuleChainMetrics::default();
        //when
        let output = chain.process(input, &metrics).expect("process failed");

        //then
        assert_eq!(output.successes.len(), 1);
        assert_eq!(output.successes[0].value().to_string(), "input");
    }

    #[ignore]
    #[test]
    fn test_unsufficient_memory_to_instantiate() {
        //given
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        let max_memory = 1_000; // 1 kb

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .lookback(Some(Lookback::Last(1)))
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_LOOK_BACK),
        );
        chain_builder.set_store_memory_limit(max_memory);

        // when
        let res = chain_builder.initialize(&engine);

        // then
        assert!(res.is_err());
        let err = res
            .unwrap_err()
            .downcast::<EngineError>()
            .expect("EngineError expected");
        assert!(matches!(
            err,
            EngineError::StoreMemoryExceeded {
                current: _,
                requested: _,
                max
            }
            if max == max_memory
        ))
    }

    #[ignore]
    #[test]
    fn test_look_back_unsufficient_memory() {
        //given
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        let metrics = SmartModuleChainMetrics::default();
        let max_memory = 1_000_000 * 2; // 2mb

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .lookback(Some(Lookback::Last(1000)))
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_LOOK_BACK),
        );
        chain_builder.set_store_memory_limit(max_memory);

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        // when
        let res = fluvio_future::task::run_block_on(chain.look_back(
            |_| {
                let res = (0..1000).map(|_| Record::new([0u8; 1_000])).collect();
                async { Ok(res) }
            },
            &metrics,
        ));

        // then
        assert!(res.is_err());
        let err = res
            .unwrap_err()
            .downcast::<EngineError>()
            .expect("EngineError expected");
        assert!(matches!(
            err,
            EngineError::StoreMemoryExceeded {
                current: _,
                requested: _,
                max
            }
            if max == max_memory
        ))
    }

    #[ignore]
    #[test]
    fn test_process_unsufficient_memory() {
        //given
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        let metrics = SmartModuleChainMetrics::default();
        let max_memory = 1_000_000 * 2; // 2mb

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_FILTER_LOOK_BACK),
        );
        chain_builder.set_store_memory_limit(max_memory);

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        // when
        let input: Vec<Record> = (0..1000).map(|_| Record::new([0u8; 1_000])).collect();
        let res = chain.process(
            SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION).expect("input"),
            &metrics,
        );

        // then
        assert!(res.is_err());
        let err = res
            .unwrap_err()
            .downcast::<EngineError>()
            .expect("EngineError expected");
        assert!(matches!(
            err,
            EngineError::StoreMemoryExceeded {
                current: _,
                requested: _,
                max
            }
            if max == max_memory
        ))
    }
}
