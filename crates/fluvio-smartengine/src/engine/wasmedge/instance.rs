use wasmedge_sdk::Memory;
use anyhow::Result;
use std::sync::Mutex;

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
