use std::sync::Mutex;
use anyhow::Result;
use wasmtime::{Memory, Store, Engine, Module};
use crate::smart_stream::filter::SmartStreamFilter;
use crate::smart_stream::map::SmartStreamMap;

mod memory;
pub mod filter;
pub mod map;
pub mod file_batch;

#[derive(Default)]
pub struct SmartStreamEngine(pub(crate) Engine);

impl SmartStreamEngine {
    pub fn create_module_from_binary(&self, bytes: &[u8]) -> Result<SmartStreamModule> {
        let module = Module::from_binary(&self.0, bytes)?;
        Ok(SmartStreamModule(module))
    }
}

pub struct SmartStreamModule(pub(crate) Module);

impl SmartStreamModule {
    pub fn create_filter(&self, engine: &SmartStreamEngine) -> Result<SmartStreamFilter> {
        let filter = SmartStreamFilter::new(&engine, self)?;
        Ok(filter)
    }

    pub fn create_map(&self, engine: &SmartStreamEngine) -> Result<SmartStreamMap> {
        let map = SmartStreamMap::new(&engine, self)?;
        Ok(map)
    }
}

pub enum SmartStream {
    Filter(SmartStreamFilter),
    Map(SmartStreamMap),
}

#[derive(Clone)]
pub struct RecordsMemory {
    ptr: i32,
    len: i32,
    memory: Memory,
}

impl RecordsMemory {
    fn copy_memory_from(&self, store: &mut Store<()>) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; self.len as u32 as usize];
        self.memory.read(store, self.ptr as usize, &mut bytes)?;
        Ok(bytes)
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
