pub(crate) mod error;
pub(crate) mod memory;

pub(crate) mod transforms;
pub(crate) mod init;
mod engine;
pub use engine::*;
pub mod instance;

pub mod metrics;

pub type WasmSlice = (i32, i32, u32);
pub type Version = i16;

#[cfg(test)]
mod fixture;
