use dataplane::Isolation;

use super::stream_fetch::SmartStreamPayload;

pub struct Pipeline {
    pub(crate) isolation: Isolation,
    pub sources: Vec<Source>,
    pub sm: Option<SmartStreamConfig>,
}

/// Data Source.
/// Each data source can have SmartStram
pub struct Source {
    pub name: String,
    pub topic: String,
    pub partiton: i32,
    pub sm: Option<SmartStreamConfig>,
}

pub struct SmartStreamConfig {
    pub(crate) wasm_module: Option<SmartStreamPayload>,
}
