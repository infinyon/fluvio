mod client;
mod error;
mod wstream;
mod config;

pub mod config_map;
pub mod secret;
pub mod pod;
pub mod service;
pub mod stateful;

#[cfg(feature = "k8")]
pub mod fixture;

pub use self::client::ApplyResult;
pub use self::client::K8Client;
pub use self::client::as_token_stream_result;
pub use self::config::K8AuthHelper;
pub use self::client::TokenStreamResult;
pub use self::error::ClientError;

