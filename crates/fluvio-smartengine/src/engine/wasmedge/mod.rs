/// Implementations of the public API
mod imp;
pub use imp::{SmartEngineImp, initialize_imp, SmartModuleChainInstanceImp};
/// Implementations of the traits in `common` for the WasmEdge engine
mod instance;
#[cfg(test)]
pub use instance::WasmEdgeFn as WasmFnImp;
mod memory;
