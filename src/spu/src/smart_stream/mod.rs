use std::path::Path;
use std::sync::{RwLock, Mutex};

use anyhow::{Result, Error};
use tracing::{debug, instrument};
use wasmtime::{Engine, Module, Store, Instance, Memory};
use crate::smart_stream::filter::SmartFilter;
use crate::smart_stream::aggregate::SmartAggregate;

mod memory;
pub mod filter;
pub mod aggregate;
pub mod file_batch;

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

    pub fn create_aggregator(&self) -> Result<SmartAggregate> {
        let write_inner = self.0.write().unwrap();
        write_inner.create_aggregator()
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

pub struct SmartStreamInstance(Instance);

impl SmartStreamInstance {
    fn new(instance: Instance) -> Self {
        Self(instance)
    }

    pub fn copy_memory_to(&self, bytes: &[u8]) -> Result<isize, Error> {
        use self::memory::copy_memory_to_instance;
        copy_memory_to_instance(bytes, &self.0)
    }
}

#[derive(Clone)]
pub struct RecordsMemory {
    ptr: i32,
    len: i32,
    memory: Memory,
}

impl RecordsMemory {
    fn copy_memory_from(&self) -> Vec<u8> {
        // TODO see if we can replace with Memory::read
        unsafe {
            if let Some(data) = self
                .memory
                .data_unchecked()
                .get(self.ptr as u32 as usize..)
                .and_then(|arr| arr.get(..self.len as u32 as usize))
            {
                debug!(wasm_mem_len = self.len, "copying from wasm");
                let mut bytes = vec![0u8; self.len as u32 as usize];
                bytes.copy_from_slice(data);
                bytes
            } else {
                vec![]
            }
        }
    }
}

pub struct RecordsCallBack(Mutex<Option<RecordsMemory>>);

impl RecordsCallBack {
    fn new() -> Self {
        Self(Mutex::new(None))
    }

    fn set(&self, records: RecordsMemory) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.replace(records);
    }

    fn clear(&self) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.take();
    }

    fn get(&self) -> Option<RecordsMemory> {
        let reader = self.0.lock().unwrap();
        reader.clone()
    }
}
