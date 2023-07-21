pub mod metrics;
/// SmartModule configuration
mod config;
pub use config::{
    SmartModuleConfig, SmartModuleConfigBuilder, SmartModuleConfigBuilderError,
    SmartModuleInitialData, Lookback,
};
mod error;
pub use error::EngineError;

#[cfg(test)]
mod fixture;

pub type WasmSlice = (i32, i32, u32);
pub type Version = i16;

mod wasmtime;
pub use self::wasmtime::{SmartEngine, SmartModuleChainBuilder, SmartModuleChainInstance};
