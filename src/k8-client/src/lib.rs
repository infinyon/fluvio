mod client;
mod error;
//mod wstream;
mod config;
mod stream;

pub mod config_map;
pub mod secret;
pub mod pod;
pub mod service;
pub mod stateful;

#[cfg(feature = "k8")]
pub mod fixture;


pub use self::client::K8Client;
pub use self::config::K8HttpClientBuilder;
pub use self::error::ClientError;

