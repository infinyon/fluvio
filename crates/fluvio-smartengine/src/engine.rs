use std::ops::{Deref, DerefMut};
use std::fmt::{self, Debug};

use anyhow::{Error, Result};
use wasmtime::{Engine, Module, IntoFunc, Store, Instance};

use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleExtraParams};

use crate::instance::{SmartModuleInstance, SmartModuleInstanceContext, SmartModuleTransform};

#[derive(Clone)]
pub struct SmartEngine(Engine);

impl SmartEngine {
    pub fn new() -> Self {
        Self(Engine::default())
    }

    /*
    #[tracing::instrument(skip(self, bytes))]
    pub fn create_new_context(
        &self,
        bytes: impl AsRef<[u8]>,
    ) -> Result<Box<dyn SmartModuleInstance>> {
        let module = Module::new(&self.0, bytes)?;

        todo!("create new context")


        let smartmodule_instance: Box<dyn SmartModuleInstance> = match &smart_payload.kind {
            SmartModuleKind::Filter => {
                Box::new(smartmodule.create_filter(smart_payload.params, version)?)
            }
            SmartModuleKind::FilterMap => {
                Box::new(smartmodule.create_filter_map(smart_payload.params, version)?)
            }
            SmartModuleKind::Map => {
                Box::new(smartmodule.create_map(smart_payload.params, version)?)
            }
            SmartModuleKind::ArrayMap => {
                Box::new(smartmodule.create_array_map(smart_payload.params, version)?)
            }
            SmartModuleKind::Join(_) => {
                Box::new(smartmodule.create_join(smart_payload.params, version)?)
            }
            SmartModuleKind::JoinStream {
                topic: _,
                derivedstream: _,
            } => Box::new(smartmodule.create_join_stream(smart_payload.params, version)?),
            SmartModuleKind::Aggregate { accumulator } => Box::new(smartmodule.create_aggregate(
                smart_payload.params,
                accumulator.clone(),
                version,
            )?),
            SmartModuleKind::Generic(context) => {
                smartmodule.create_generic_smartmodule(smart_payload.params, context, version)?
            }
        };

    }
    */
}

cfg_if::cfg_if! {
    if #[cfg(feature = "wasi")] {

        pub(crate) type State = wasmtime_wasi::WasiCtx;
        impl SmartEngine {
            fn new_chain(&self) -> SmartModuleChain {
                let wasi = wasmtime_wasi::WasiCtxBuilder::new()
                    .inherit_stderr()
                    .inherit_stdout()
                    .build();
                SmartModuleChain {
                    store: Store::new(&self.0, wasi),
                    modules: vec![],
                }
            }
        }
    } else  {
        pub(crate) type State = ();
        impl SmartEngine {
            fn new_chain(&self) -> SmartModuleChain {
                SmartModuleChain {
                    store: Store::new(&self.0)
                    modules: vec![],
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

/// Chain of SmartModule which can be execute
pub struct SmartModuleChain {
    store: Store<State>,
    modules: Vec<Box<SmartModuleInstance<dyn SmartModuleTransform>>>,
}

impl Deref for SmartModuleChain {
    type Target = Store<State>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl DerefMut for SmartModuleChain {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

/*
impl AsContextMut for SmartModuleChain {
    fn as_context_mut(&mut self) -> &mut State {
        self.store.context_mut()
    }
}
*/

cfg_if::cfg_if! {
    if #[cfg(feature = "wasi")] {
        impl SmartModuleChain {
            pub(crate) fn instantiate<Params, Args>(
                &mut self,
                module: &Module,
                host_fn: impl IntoFunc<State, Params, Args>,
            ) -> Result<Instance, Error> {
                let mut linker = wasmtime::Linker::new(&self.store.engine());
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
                linker.instantiate(self.store, module)
            }
        }
    } else  {
        impl SmartModuleChain {

            pub(crate) fn instantiate<Params, Args>(
                &mut self,
                module: &Module,
                host_fn: impl IntoFunc<State, Params, Args>,
            ) -> Result<Instance, Error> {
                let func = Func::wrap(&mut *self.0, host_fn);
                Instance::new(self.0, module, &[func.into()])
            }



        }
    }
}

impl SmartModuleChain {
    /// add new smart module to chain
    pub fn add_smart_module(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
        bytes: impl AsRef<[u8]>,
    ) -> Result<()> {
        let module = Module::new(&self.store.engine(), bytes)?;

        let instance = SmartModuleInstanceContext::instantiate(module, &self, params, version)?;

        Ok(())
    }
}

/*
pub struct SmartModuleWithEngine {
    pub(crate) module: Module,
    pub(crate) engine: SmartEngine,
}

/
impl SmartModuleWithEngine {
    fn create_filter(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleFilter, error::Error> {
        let filter = SmartModuleFilter::new(self, params, version)?;
        Ok(filter)
    }

    fn create_map(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleMap, error::Error> {
        let map = SmartModuleMap::new(self, params, version)?;
        Ok(map)
    }

    fn create_filter_map(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleFilterMap, error::Error> {
        let filter_map = SmartModuleFilterMap::new(self, params, version)?;
        Ok(filter_map)
    }

    fn create_array_map(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleArrayMap, error::Error> {
        let map = SmartModuleArrayMap::new(self, params, version)?;
        Ok(map)
    }

    fn create_join(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleJoin, error::Error> {
        let join = SmartModuleJoin::new(self, params, version)?;
        Ok(join)
    }

    fn create_join_stream(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleJoinStream, error::Error> {
        let join = SmartModuleJoinStream::new(self, params, version)?;
        Ok(join)
    }

    fn create_aggregate(
        &self,
        params: SmartModuleExtraParams,
        accumulator: Vec<u8>,
        version: i16,
    ) -> Result<SmartModuleAggregate, error::Error> {
        let aggregate = SmartModuleAggregate::new(self, params, accumulator, version)?;
        Ok(aggregate)
    }

    /// Create smartmodule without knowing its type. This function will try to initialize the smartmodule as each one of
    /// the available smartmodules until there is success or all the kinds of smartmodules is tried.
    fn create_generic_smartmodule(
        &self,
        params: SmartModuleExtraParams,
        context: &SmartModuleContextData,
        version: i16,
    ) -> Result<Box<dyn SmartModuleInstance>, error::Error> {
        match self.create_filter(params.clone(), version) {
            Ok(filter) => return Ok(Box::new(filter)),
            Err(error::Error::NotNamedExport(_, _)) => {}
            Err(any_other_error) => return Err(any_other_error),
        }

        match self.create_map(params.clone(), version) {
            Ok(map) => return Ok(Box::new(map)),
            Err(error::Error::NotNamedExport(_, _)) => {}
            Err(any_other_error) => return Err(any_other_error),
        }

        match self.create_filter_map(params.clone(), version) {
            Ok(filter_map) => return Ok(Box::new(filter_map)),
            Err(error::Error::NotNamedExport(_, _)) => {}
            Err(any_other_error) => return Err(any_other_error),
        }

        match self.create_array_map(params.clone(), version) {
            Ok(array_map) => return Ok(Box::new(array_map)),
            Err(error::Error::NotNamedExport(_, _)) => {}
            Err(any_other_error) => return Err(any_other_error),
        }

        let accumulator = match context {
            SmartModuleContextData::Aggregate { accumulator } => accumulator.clone(),
            _ => vec![],
        };
        match self.create_aggregate(params.clone(), accumulator, version) {
            Ok(aggregate) => return Ok(Box::new(aggregate)),
            Err(error::Error::NotNamedExport(_, _)) => {}
            Err(any_other_error) => return Err(any_other_error),
        }

        match self.create_join(params.clone(), version) {
            Ok(join) => return Ok(Box::new(join)),
            Err(error::Error::NotNamedExport(_, _)) => {}
            Err(any_other_error) => return Err(any_other_error),
        }

        match self.create_join_stream(params, version) {
            Ok(join_stream) => return Ok(Box::new(join_stream)),
            Err(error::Error::NotNamedExport(_, _)) => {}
            Err(any_other_error) => return Err(any_other_error),
        }

        Err(error::Error::NotValidExports)
    }
}
*/

#[cfg(test)]
mod test {
    use std::path::{PathBuf, Path};

    use super::{DEFAULT_SMARTENGINE_VERSION, SmartEngine};
    const FLUVIO_WASM_FILTER: &str = "fluvio_wasm_filter";
    const FLUVIO_WASM_MAP: &str = "fluvio_wasm_map_double";
    const FLUVIO_WASM_ARRAY_MAP: &str = "fluvio_wasm_array_map_array";
    const FLUVIO_WASM_FILTER_MAP: &str = "fluvio_wasm_filter_map";
    const FLUVIO_WASM_AGGREGATE: &str = "fluvio_wasm_aggregate";
    const FLUVIO_WASM_JOIN: &str = "fluvio_wasm_join";

    fn read_wasm_module(module_name: &str) -> Vec<u8> {
        let spu_dir = std::env::var("CARGO_MANIFEST_DIR").expect("target");
        let wasm_path = PathBuf::from(spu_dir)
            .parent()
            .expect("parent")
            .join(format!(
                "fluvio-smartmodule/examples/target/wasm32-unknown-unknown/release/{}.wasm",
                module_name
            ));
        read_module_from_path(wasm_path)
    }

    fn read_module_from_path(filter_path: impl AsRef<Path>) -> Vec<u8> {
        let path = filter_path.as_ref();
        std::fs::read(path).unwrap_or_else(|_| panic!("Unable to read file {}", path.display()))
    }

    #[ignore]
    #[test]
    fn create_filter() {
        let filter = read_wasm_module(FLUVIO_WASM_FILTER);
        let engine = SmartEngine::default()
            .create_module_from_binary(&filter)
            .expect("Failed to create filter");

        engine
            .create_filter(Default::default(), DEFAULT_SMARTENGINE_VERSION)
            .expect("failed to create filter");

        // generic
        engine
            .create_generic_smartmodule(
                Default::default(),
                &Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create generic smartmodule");
    }

    #[ignore]
    #[test]
    fn create_map() {
        let filter = read_wasm_module(FLUVIO_WASM_MAP);
        let engine = SmartEngine::default()
            .create_module_from_binary(&filter)
            .expect("Failed to create map");

        engine
            .create_map(Default::default(), DEFAULT_SMARTENGINE_VERSION)
            .expect("failed to create map");

        // generic
        engine
            .create_generic_smartmodule(
                Default::default(),
                &Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create generic smartmodule");
    }

    #[ignore]
    #[test]
    fn create_filter_map() {
        let filter = read_wasm_module(FLUVIO_WASM_FILTER_MAP);
        let engine = SmartEngine::default()
            .create_module_from_binary(&filter)
            .expect("Failed to create filter map");

        engine
            .create_filter_map(Default::default(), DEFAULT_SMARTENGINE_VERSION)
            .expect("failed to create filter map");

        // generic
        engine
            .create_generic_smartmodule(
                Default::default(),
                &Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create generic smartmodule");
    }

    #[ignore]
    #[test]
    fn create_array_map() {
        let arraymap = read_wasm_module(FLUVIO_WASM_ARRAY_MAP);
        let engine = SmartEngine::default()
            .create_module_from_binary(&arraymap)
            .expect("Failed to create arraymap");

        engine
            .create_array_map(Default::default(), DEFAULT_SMARTENGINE_VERSION)
            .expect("failed to create arraymap");

        // generic
        engine
            .create_generic_smartmodule(
                Default::default(),
                &Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create generic smartmodule");
    }

    #[ignore]
    #[test]
    fn create_aggregate() {
        let agg = read_wasm_module(FLUVIO_WASM_AGGREGATE);
        let engine = SmartEngine::default()
            .create_module_from_binary(&agg)
            .expect("Failed to create aggregate");

        engine
            .create_aggregate(
                Default::default(),
                Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create aggregate");

        // generic
        engine
            .create_generic_smartmodule(
                Default::default(),
                &Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create generic smartmodule");
    }

    #[ignore]
    #[test]
    fn create_join() {
        let join = read_wasm_module(FLUVIO_WASM_JOIN);
        let engine = SmartEngine::default()
            .create_module_from_binary(&join)
            .expect("Failed to create join");

        engine
            .create_join(Default::default(), DEFAULT_SMARTENGINE_VERSION)
            .expect("failed to create join");

        // generic
        engine
            .create_generic_smartmodule(
                Default::default(),
                &Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create generic smartmodule");
    }

    #[ignore]
    #[test]
    fn invalid_wasm_data() {
        //we try to create a map smartmodule with a filter smartmodule

        let filter = read_wasm_module(FLUVIO_WASM_FILTER);
        let engine = SmartEngine::default()
            .create_module_from_binary(&filter)
            .expect("Failed to create join");

        engine
            .create_map(Default::default(), DEFAULT_SMARTENGINE_VERSION)
            .map(|_| "SmartModuleMap")
            .expect_err("SmartModule Map was created with a filter module");

        // generic creation should work!
        engine
            .create_generic_smartmodule(
                Default::default(),
                &Default::default(),
                DEFAULT_SMARTENGINE_VERSION,
            )
            .expect("failed to create generic smartmodule");
    }
}
