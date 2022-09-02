use std::ops::{Deref, DerefMut};
use std::fmt::{self, Debug};

use anyhow::{Error, Result};
use fluvio_protocol::link::smartmodule::SmartModuleRuntimeError;
use fluvio_protocol::record::Batch;
use fluvio_smartmodule::Record;
use tracing::instrument;
use wasmtime::{Engine, Module, IntoFunc, Store, Instance, AsContextMut};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput,
};

use crate::file_batch::FileBatchIterator;
use crate::instance::{SmartModuleInstance, SmartModuleInstanceContext};
use crate::transforms::create_transform;

const DEFAULT_SMARTENGINE_VERSION: i16 = 17;

#[derive(Clone)]
pub struct SmartEngine(Engine);

impl SmartEngine {
    pub fn new() -> Self {
        Self(Engine::default())
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "wasi")] {

        pub(crate) type State = wasmtime_wasi::WasiCtx;
        impl SmartEngine {
            pub fn new_chain(&self) -> SmartModuleChain {
                let wasi = wasmtime_wasi::WasiCtxBuilder::new()
                    .inherit_stderr()
                    .inherit_stdout()
                    .build();
                SmartModuleChain {
                    store: Store::new(&self.0, wasi),
                    instances: vec![],
                }
            }
        }
    } else  {
        pub(crate) type State = ();
        impl SmartEngine {
            pub fn new_chain(&self) -> SmartModuleChain {
                SmartModuleChain {
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

/// Chain of SmartModule which can be execute
pub struct SmartModuleChain {
    store: Store<State>,
    instances: Vec<SmartModuleInstance>,
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
                linker.instantiate(self.as_context_mut(), module)
            }
        }
    } else  {
        impl SmartModuleChain {

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

impl SmartModuleChain {
    #[cfg(test)]
    pub(crate) fn instances(self) -> Vec<SmartModuleInstance> {
        self.instances
    }

    /// add new smart module to chain
    pub fn add_smart_module(
        &mut self,
        params: SmartModuleExtraParams,
        maybe_version: Option<i16>,
        bytes: impl AsRef<[u8]>,
    ) -> Result<()> {
        let module = Module::new(&self.store.engine(), bytes)?;

        let version = maybe_version.unwrap_or(DEFAULT_SMARTENGINE_VERSION);
        let ctx = SmartModuleInstanceContext::instantiate(module, self, params, version)?;
        let transform = create_transform(&ctx, &mut self.as_context_mut())?;
        self.instances
            .push(SmartModuleInstance::new(ctx, transform));
        Ok(())
    }

    /// process a record
    pub fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        // only perform a single transform now
        let first_instance = self.instances.first_mut();
        if let Some(instance) = first_instance {
            instance.process(input, &mut self.store)
        } else {
            Err(Error::msg("No transform found"))
        }
    }

    #[instrument(skip(self))]
    pub fn invoke_constructor(&mut self) -> Result<(), Error> {
        // only perform a single transform now
        let first_instance = self.instances.first_mut();
        if let Some(instance) = first_instance {
            instance.invoke_constructor(&mut self.store)
        } else {
            Err(Error::msg("No transform found"))
        }
    }

    #[instrument(skip(self, iter, max_bytes, join_last_record))]
    pub fn process_batch(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
        join_last_record: Option<&Record>,
    ) -> Result<(Batch, Option<SmartModuleRuntimeError>), Error> {
        let first_instance = self.instances.first_mut();
        if let Some(instance) = first_instance {
            instance.process_batch(&mut self.store, iter, max_bytes, join_last_record)
        } else {
            Err(Error::msg("No transform found"))
        }
    }
}
