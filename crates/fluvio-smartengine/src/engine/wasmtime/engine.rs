use anyhow::Result;
use tracing::debug;
use wasmtime::{Engine, Module};
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};

use crate::metrics::SmartModuleChainMetrics;
use crate::engine::SmartModuleChainBuilder;
use crate::engine::common::create_transform;
use super::instance::{WasmtimeContext, WasmtimeFn, WasmtimeInstance};
use super::state::WasmState;

type SmartModuleInit = crate::engine::common::SmartModuleInit<WasmtimeFn>;
type SmartModuleInstance = crate::engine::common::SmartModuleInstance<WasmtimeInstance, WasmtimeFn>;

#[derive(Clone)]
pub struct SmartEngineImp(Engine);

#[allow(clippy::new_without_default)]
impl SmartEngineImp {
    pub fn new() -> Self {
        let mut config = wasmtime::Config::default();
        config.consume_fuel(true);
        Self(Engine::new(&config).expect("Config is static"))
    }

    pub(crate) fn new_state(&self) -> WasmState {
        WasmState::new(&self.0)
    }
}

/// stop adding smartmodule and return SmartModuleChain that can be executed
pub fn initialize_imp(
    builder: SmartModuleChainBuilder,
    engine: &SmartEngineImp,
) -> Result<SmartModuleChainInstanceImp> {
    let mut instances = Vec::with_capacity(builder.smart_modules.len());
    let state = engine.new_state();
    let mut ctx = WasmtimeContext { state };
    for (config, bytes) in builder.smart_modules {
        let module = Module::new(&engine.0, bytes)?;
        let version = config.version();

        let mut instance =
            WasmtimeInstance::instantiate(&mut ctx.state, module, config.params, version)?;

        let init = SmartModuleInit::try_instantiate(&mut instance, &mut ctx)?;

        let transform = create_transform(&mut instance, &mut ctx, config.initial_data)?;
        let mut instance = SmartModuleInstance::new(instance, init, transform);
        instance.init(&mut ctx)?;
        instances.push(instance);
    }
    Ok(SmartModuleChainInstanceImp { ctx, instances })
}

/// SmartModule Chain Instance that can be executed
pub struct SmartModuleChainInstanceImp {
    ctx: WasmtimeContext,
    instances: Vec<SmartModuleInstance>,
}

impl SmartModuleChainInstanceImp {
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
                self.ctx.state.top_up_fuel();
                let output = instance.process(next_input, &mut self.ctx)?;
                let fuel_used = self.ctx.state.get_used_fuel();
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

            self.ctx.state.top_up_fuel();
            let output = last.process(next_input, &mut self.ctx)?;
            let fuel_used = self.ctx.state.get_used_fuel();
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

    use std::convert::TryFrom;

    use fluvio_smartmodule::{dataplane::smartmodule::SmartModuleInput, Record};

    use crate::engine::{
        metrics::SmartModuleChainMetrics, SmartEngine, SmartModuleChainBuilder, SmartModuleConfig,
        SmartModuleInitialData,
    };

    const SM_FILTER_INIT: &str = "fluvio_smartmodule_filter_init";
    const SM_MAP: &str = "fluvio_smartmodule_map";

    use crate::engine::fixture::read_wasm_module;

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
            .expect("failed to build chain")
            .inner;
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
        chain.ctx.state.top_up_fuel();
        assert_eq!(chain.ctx.state.get_used_fuel(), 0);
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
            .expect("failed to build chain")
            .inner;
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
            .expect("failed to build chain")
            .inner;

        assert_eq!(chain.ctx.state.get_used_fuel(), 0);

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
