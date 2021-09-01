use std::sync::{Arc, Mutex};
use tracing::debug;
use anyhow::Result;
use wasmtime::{Memory, Store, Engine, Module, Func, Caller, Extern, Trap, Instance};
use crate::smartstream::filter::SmartStreamFilter;
use crate::smartstream::map::SmartStreamMap;
use crate::smartstream::aggregate::SmartStreamAggregate;
use dataplane::core::{Encoder, Decoder};

mod memory;
pub mod filter;
pub mod map;
pub mod aggregate;
pub mod file_batch;

pub type WasmSlice = (i32, i32);

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
        let filter = SmartStreamFilter::new(engine, self)?;
        Ok(filter)
    }

    pub fn create_map(&self, engine: &SmartStreamEngine) -> Result<SmartStreamMap> {
        let map = SmartStreamMap::new(engine, self)?;
        Ok(map)
    }

    pub fn create_aggregate(
        &self,
        engine: &SmartStreamEngine,
        accumulator: Vec<u8>,
    ) -> Result<SmartStreamAggregate> {
        let aggregate = SmartStreamAggregate::new(engine, self, accumulator)?;
        Ok(aggregate)
    }
}

pub struct SmartStreamBase {
    store: Store<()>,
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
}

impl SmartStreamBase {
    pub fn new(engine: &SmartStreamEngine, module: &SmartStreamModule) -> Result<Self> {
        let mut store = Store::new(&engine.0, ());
        let cb = Arc::new(RecordsCallBack::new());
        let records_cb = cb.clone();
        let copy_records = Func::wrap(
            &mut store,
            move |mut caller: Caller<'_, ()>, ptr: i32, len: i32| {
                debug!(len, "callback from wasm filter");
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return Err(Trap::new("failed to find host memory")),
                };

                let records = RecordsMemory { ptr, len, memory };
                cb.set(records);
                Ok(())
            },
        );

        let instance = Instance::new(&mut store, &module.0, &[copy_records.into()])?;
        Ok(Self {
            store,
            instance,
            records_cb,
        })
    }

    pub fn write_input<E: Encoder>(&mut self, input: &E) -> Result<WasmSlice> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, 0)?;
        let array_ptr =
            self::memory::copy_memory_to_instance(&mut self.store, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32))
    }

    pub fn read_output<D: Decoder + Default>(&mut self) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(&mut self.store).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), 0)?;
        Ok(output)
    }
}

pub enum SmartStream {
    Filter(SmartStreamFilter),
    Map(SmartStreamMap),
    Aggregate(SmartStreamAggregate),
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
