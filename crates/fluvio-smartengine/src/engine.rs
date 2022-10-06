use std::ops::{Deref, DerefMut};
use std::fmt::{self, Debug};

use anyhow::{Error, Result};
use derive_builder::Builder;
use wasmtime::{Engine, Module, IntoFunc, Store, Instance, AsContextMut};

use fluvio_protocol::record::Offset;
use fluvio_smartmodule::Record;

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput,
};

use crate::init::SmartModuleInit;
use crate::instance::{SmartModuleInstance, SmartModuleInstanceContext};
use crate::transforms::create_transform;

const DEFAULT_SMARTENGINE_VERSION: i16 = 17;

#[derive(Clone)]
pub struct SmartEngine(Engine);

#[allow(clippy::new_without_default)]
impl SmartEngine {
    pub fn new() -> Self {
        Self(Engine::default())
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "wasi")] {

        pub(crate) type State = wasmtime_wasi::WasiCtx;
        impl SmartEngine {
            /// create new chain builder
            pub fn builder(&self) -> SmartModuleChainBuilder {
                let wasi = wasmtime_wasi::WasiCtxBuilder::new()
                    .inherit_stderr()
                    .inherit_stdout()
                    .build();
                SmartModuleChainBuilder {
                    store: Store::new(&self.0, wasi),
                    instances: vec![],
                }
            }
        }
    } else  {
        pub(crate) type State = ();
        impl SmartEngine {
            /// create new chain builder
            pub fn builder(&self) -> SmartModuleChainBuilder {
                SmartModuleChainBuilder {
                    store: Store::new(&self.0,()),
                    instances: vec![],
                }
            }
        }
    }
}

impl Debug for SmartEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleEngine")
    }
}

pub type WasmState = Store<State>;

/// Building SmartModule
pub struct SmartModuleChainBuilder {
    store: Store<State>,
    instances: Vec<SmartModuleInstance>,
}

impl Deref for SmartModuleChainBuilder {
    type Target = Store<State>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl DerefMut for SmartModuleChainBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "wasi")] {
        impl SmartModuleChainBuilder {
            pub(crate) fn instantiate<Params, Args>(
                &mut self,
                module: &Module,
                host_fn: impl IntoFunc<State, Params, Args>,
            ) -> Result<Instance, Error> {
                let mut linker = wasmtime::Linker::new(self.store.engine());
                wasmtime_wasi::add_to_linker(&mut linker, |c| c)?;
                let copy_records_fn_import = module
                    .imports()
                    .find(|import| import.name().eq("copy_records"))
                    .ok_or_else(|| Error::msg("At least one import is required"))?;
                linker.func_wrap(
                    copy_records_fn_import.module(),
                    copy_records_fn_import.name(),
                    host_fn,
                )?;
                linker.instantiate(self.as_context_mut(), module)
            }
        }
    } else  {
        impl SmartModuleChainBuilder {

            pub(crate) fn instantiate<Params, Args>(
                &mut self,
                module: &Module,
                host_fn: impl IntoFunc<State, Params, Args>,
            ) -> Result<Instance, Error> {

                use wasmtime::Func;

                let func = Func::wrap(self.as_context_mut(), host_fn);
                Instance::new(self.as_context_mut(), module, &[func.into()])
            }



        }
    }
}

impl SmartModuleChainBuilder {
    #[cfg(test)]
    pub(crate) fn instances(&self) -> &Vec<SmartModuleInstance> {
        &self.instances
    }

    /// Add SmartModule with a single transform and init
    pub fn add_smart_module(
        &mut self,
        config: SmartModuleConfig,
        bytes: impl AsRef<[u8]>,
    ) -> Result<()> {
        let module = Module::new(self.store.engine(), bytes)?;

        let version = config.version();
        let params = config.params;
        let initial_data = config.initial_data;
        let ctx = SmartModuleInstanceContext::instantiate(module, self, params, version)?;
        let init = SmartModuleInit::try_instantiate(&ctx, &mut self.as_context_mut())?;
        let transform = create_transform(&ctx, initial_data, &mut self.as_context_mut())?;
        self.instances
            .push(SmartModuleInstance::new(ctx, init, transform));
        Ok(())
    }

    /// stop adding smart module and return SmartModuleChain that can be executed
    pub fn initialize(mut self) -> Result<SmartModuleChainInstance> {
        for instance in self.instances.iter_mut() {
            instance.init(&mut self.store)?;
        }
        Ok(SmartModuleChainInstance {
            store: self.store,
            instances: self.instances,
        })
    }
}

/// SmartModule Chain Instance that can be executed
pub struct SmartModuleChainInstance {
    store: Store<State>,
    instances: Vec<SmartModuleInstance>,
}

impl Debug for SmartModuleChainInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleChainInstance")
    }
}

impl Deref for SmartModuleChainInstance {
    type Target = Store<State>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl DerefMut for SmartModuleChainInstance {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

impl SmartModuleChainInstance {
    #[cfg(test)]
    pub(crate) fn instances(&self) -> &Vec<SmartModuleInstance> {
        &self.instances
    }

    /// A single record is processed thru all smart modules in the chain.
    /// The output of one smart module is the input of the next smart module.
    /// A single record may result in multiple records.
    /// The output of the last smart module is added to the output of the chain.
    pub fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        enum SmartModuleStepInput {
            Raw(Vec<u8>),
            Records(Vec<Record>),
        }

        impl From<Vec<u8>> for SmartModuleStepInput {
            fn from(input: Vec<u8>) -> Self {
                SmartModuleStepInput::Raw(input)
            }
        }

        impl From<Vec<Record>> for SmartModuleStepInput {
            fn from(input: Vec<Record>) -> Self {
                SmartModuleStepInput::Records(input)
            }
        }

        impl SmartModuleStepInput {
            fn into_input(self, base_offset: Offset) -> Result<SmartModuleInput> {
                match self {
                    SmartModuleStepInput::Raw(input) => {
                        Ok(SmartModuleInput::new(input, base_offset))
                    }
                    SmartModuleStepInput::Records(records) => {
                        let mut input: SmartModuleInput = records.try_into()?;
                        input.set_base_offset(base_offset);
                        Ok(input)
                    }
                }
            }

            fn try_into_output(self) -> Result<SmartModuleOutput> {
                match self {
                    SmartModuleStepInput::Raw(_) => Err(Error::msg("Unexpected raw input")),
                    SmartModuleStepInput::Records(records) => Ok(SmartModuleOutput::new(records)),
                }
            }
        }

        let base_offset = input.base_offset();

        let mut next_input: SmartModuleStepInput = input.into_raw_bytes().into();

        for instance in self.instances.iter_mut() {
            // pass raw inputs to transform instance
            // each raw input may result in multiple records
            //println!("raw records: {}", next_input.len());
            let step_input = next_input.into_input(base_offset)?;
            let output = instance.process(step_input, &mut self.store)?;

            if output.error.is_some() {
                // encountered error, we stop processing and return partial output
                return Ok(output);
            } else {
                next_input = output.successes.into();
            }
        }

        next_input.try_into_output()
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

impl SmartModuleConfig {}

impl SmartModuleConfig {
    pub fn builder() -> SmartModuleConfigBuilder {
        SmartModuleConfigBuilder::default()
    }

    pub(crate) fn version(&self) -> i16 {
        self.version.unwrap_or(DEFAULT_SMARTENGINE_VERSION)
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

    use std::{convert::TryFrom};

    use fluvio_smartmodule::{
        dataplane::smartmodule::{SmartModuleInput},
        Record,
    };

    use crate::{SmartEngine, SmartModuleConfig, SmartModuleInitialData};

    const SM_FILTER_INIT: &str = "fluvio_smartmodule_filter_init";
    const SM_MAP: &str = "fluvio_smartmodule_map";

    use crate::fixture::read_wasm_module;

    #[ignore]
    #[test]
    fn test_chain_filter_map() {
        let engine = SmartEngine::new();
        let mut chain_builder = engine.builder();

        chain_builder
            .add_smart_module(
                SmartModuleConfig::builder()
                    .param("key", "a")
                    .build()
                    .unwrap(),
                read_wasm_module(SM_FILTER_INIT),
            )
            .expect("failed to create filter");

        chain_builder
            .add_smart_module(
                SmartModuleConfig::builder().build().unwrap(),
                read_wasm_module(SM_MAP),
            )
            .expect("failed to create map");

        let mut chain = chain_builder.initialize().expect("failed to build chain");
        assert_eq!(chain.instances().len(), 2);

        let input = vec![Record::new("hello world")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"))
            .expect("process");
        assert_eq!(output.successes.len(), 0); // no records passed

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"))
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"APPLE");
        assert_eq!(output.successes[1].value.as_ref(), b"BANANA");
    }

    const SM_AGGEGRATE: &str = "fluvio_smartmodule_aggregate";

    #[ignore]
    #[test]
    fn test_chain_filter_aggregate() {
        let engine = SmartEngine::new();
        let mut chain_builder = engine.builder();

        chain_builder
            .add_smart_module(
                SmartModuleConfig::builder()
                    .param("key", "a")
                    .build()
                    .unwrap(),
                read_wasm_module(SM_FILTER_INIT),
            )
            .expect("failed to create filter");

        chain_builder
            .add_smart_module(
                SmartModuleConfig::builder()
                    .initial_data(SmartModuleInitialData::with_aggregate(
                        "zero".to_string().as_bytes().to_vec(),
                    ))
                    .build()
                    .unwrap(),
                read_wasm_module(SM_AGGEGRATE),
            )
            .expect("failed to create aggegrate");

        let mut chain = chain_builder.initialize().expect("failed to build chain");
        assert_eq!(chain.instances().len(), 2);

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"))
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value().to_string(), "zeroapple");
        assert_eq!(output.successes[1].value().to_string(), "zeroapplebanana");

        let input = vec![Record::new("nothing")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"))
            .expect("process");
        assert_eq!(output.successes.len(), 0); // one record passed

        let input = vec![Record::new("elephant")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"))
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(
            output.successes[0].value().to_string(),
            "zeroapplebananaelephant"
        );
    }
}
