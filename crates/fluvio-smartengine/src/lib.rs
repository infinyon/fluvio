pub type WasmSlice = (i32, i32, u32);

#[cfg(feature = "engine")]
pub mod engine;

pub mod metadata;
