use anyhow::{anyhow, Error, Result};
use std::sync::Mutex;
use wasmedge_sdk::{types::Val, Engine, Instance, Memory};

const ALLOC_FN: &str = "alloc";
const MEMORY: &str = "memory";
// const ARRAY_SUM_FN: &str = "array_sum";
// const UPPER_FN: &str = "upper";
// const DEALLOC_FN: &str = "dealloc";

/// Copy a byte array into an instance's linear memory
/// and return the offset relative to the module's memory.
pub(crate) fn copy_memory_to_instance(
    engine: &impl Engine,
    instance: &Instance,
    bytes: &[u8],
) -> Result<i32, Error> {
    // Get the "memory" export of the module.
    // If the module does not export it, just panic,
    // since we are not going to be able to copy the data.
    let mut memory = instance
        .memory(MEMORY)
        .ok_or_else(|| anyhow!("Missing memory"))?;

    // The module is not using any bindgen libraries,
    // so it should export its own alloc function.
    //
    // Get the guest's exported alloc function, and call it with the
    // length of the byte array we are trying to copy.
    // The result is an offset relative to the module's linear memory,
    // which is used to copy the bytes into the module's memory.
    // Then, return the offset.
    let alloc = instance
        .func(ALLOC_FN)
        .ok_or_else(|| anyhow!("missing alloc"))?;

    let alloc_result = alloc.call(engine, [Val::I32(bytes.len() as i32).into()])?;

    let guest_ptr_offset = alloc_result[0].to_i32();

    memory.write(bytes, guest_ptr_offset as u32)?;

    Ok(guest_ptr_offset)
}

// TODO: revise later to see whether Clone is necessary
#[derive(Clone)]
pub(crate) struct RecordsMemory {
    pub ptr: u32,
    pub len: u32,
    pub memory: Memory,
}

impl RecordsMemory {
    pub(crate) fn copy_memory_from(&self) -> Result<Vec<u8>> {
        let bytes = self.memory.read(self.ptr, self.len)?;
        Ok(bytes)
    }
}

pub struct RecordsCallBack(Mutex<Option<RecordsMemory>>);

impl RecordsCallBack {
    pub(crate) fn new() -> Self {
        Self(Mutex::new(None))
    }

    pub(crate) fn set(&self, records: RecordsMemory) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.replace(records);
    }

    pub(crate) fn clear(&self) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.take();
    }

    pub(crate) fn get(&self) -> Option<RecordsMemory> {
        let reader = self.0.lock().unwrap();
        reader.clone()
    }
}
