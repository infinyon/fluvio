/// Implementations of the public API
mod imp;
pub use imp::{SmartEngineImp, initialize_imp, SmartModuleChainInstanceImp};
/// Implementations of the traits in `common` for the WasmEdge engine
pub(crate) mod instance;
mod memory;
