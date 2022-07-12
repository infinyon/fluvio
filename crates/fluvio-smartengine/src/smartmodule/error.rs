#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to get valid exports for {0}.")]
    NamedExport(&'static str),
    #[error("Failed to get valid exports for any kind of smartmodule.")]
    AnyExports,
    #[error("Failed to instantiate: {0}")]
    Instantiate(anyhow::Error),
}
