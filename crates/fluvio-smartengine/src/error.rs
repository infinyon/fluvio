#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("Failed to get valid exports for {0}")]
    NotNamedExport(&'static str),
    #[error("Function Ty failed conversion {0}: {1}")]
    TypeConversion(&'static str, anyhow::Error),
    #[error("No smartmodule found")]
    UnknownSmartModule,
    #[error("Failed to instantiate: {0}")]
    Instantiate(anyhow::Error)
}
