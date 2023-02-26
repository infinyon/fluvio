// TODO: turn on this check when ready
// #[cfg(all(feature = "wasmedge_engine", feature = "wasmtime_engine"))]
// compile_error!(
//     "Both `wasmedge_engine` and `wasmtime_engine` features are enabled, but \
//     only one WASM runtime is supported at a time"
// );

cfg_if::cfg_if! {
    if #[cfg(feature = "engine")] {
        pub(crate) mod memory;
        pub(crate) mod transforms;
        pub(crate) mod init;
        pub(crate) mod error;
        pub mod metrics;
        mod state;
        mod config;
        mod engine;
        pub use config::{SmartModuleConfig, SmartModuleConfigBuilder, SmartModuleConfigBuilderError, SmartModuleInitialData};
        pub mod instance;

        // TODO: move wasmtime engine to a module; or maybe add some common abstraction?
        #[cfg(feature = "wasmtime-engine")]
        pub use engine::{SmartEngine, SmartModuleChainBuilder, SmartModuleChainInstance};

        #[cfg(feature = "wasmedge-engine")]
        mod wasmedge_engine;
        #[cfg(feature = "wasmedge-engine")]
        mod test_use {
            pub use super::wasmedge_engine::{SmartEngine, SmartModuleChainBuilder, SmartModuleChainInstance};
        }
    }
}
#[cfg(feature = "transformation")]
pub mod transformation;

pub type WasmSlice = (i32, i32, u32);
pub type Version = i16;

#[cfg(test)]
mod fixture;
