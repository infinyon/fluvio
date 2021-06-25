use std::path::Path;
use std::sync::RwLock;

use anyhow::Result;
use tracing::instrument;
use wasmtime::{Engine, Module, Store};
use crate::smart_stream::filter::SmartFilter;

pub mod filter;
mod memory;

pub struct SmartStreamEngine(Engine);

impl SmartStreamEngine {
    pub fn new() -> Self {
        Self(Engine::default())
    }

    #[allow(unused)]
    #[instrument(skip(self, path))]
    pub fn create_module_from_path(&self, path: impl AsRef<Path>) -> Result<SmartStreamModule> {
        SmartStreamModule::from_path(&self.0, path)
    }

    #[instrument(skip(self, binary))]
    pub fn create_module_from_binary(&self, binary: &[u8]) -> Result<SmartStreamModule> {
        SmartStreamModule::from_binary(&self.0, binary)
    }
}

pub struct SmartStreamModule(RwLock<SmartStreamModuleInner>);

impl SmartStreamModule {
    #[allow(unused)]
    fn from_path(engine: &Engine, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self(RwLock::new(SmartStreamModuleInner::create_from_path(
            engine, path,
        )?)))
    }

    fn from_binary(engine: &Engine, binary: &[u8]) -> Result<Self> {
        Ok(Self(RwLock::new(
            SmartStreamModuleInner::create_from_binary(engine, binary)?,
        )))
    }

    pub fn create_filter(&self) -> Result<SmartFilter> {
        let write_inner = self.0.write().unwrap();
        write_inner.create_filter()
    }
}

unsafe impl Send for SmartStreamModuleInner {}
unsafe impl Sync for SmartStreamModuleInner {}

pub struct SmartStreamModuleInner {
    module: Module,
    store: Store,
}

impl SmartStreamModuleInner {
    #[allow(unused)]
    pub fn create_from_path(engine: &Engine, path: impl AsRef<Path>) -> Result<Self> {
        let store = Store::new(&engine);
        let module = Module::from_file(store.engine(), path)?;
        Ok(Self { module, store })
    }

    pub fn create_from_binary(engine: &Engine, binary: &[u8]) -> Result<Self> {
        let store = Store::new(&engine);
        let module = Module::from_binary(store.engine(), binary)?;
        Ok(Self { module, store })
    }
}
