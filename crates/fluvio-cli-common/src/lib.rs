pub mod charts;
pub mod cluster;
pub mod error;
pub mod http;
pub mod install;
pub mod profile;

#[cfg(feature = "file-records")]
pub mod user_input;

pub const DEFAULT_NAMESPACE: &str = "default";

// Environment vars for Channels
pub const FLUVIO_RELEASE_CHANNEL: &str = "FLUVIO_RELEASE_CHANNEL";
pub const FLUVIO_EXTENSIONS_DIR: &str = "FLUVIO_EXTENSIONS_DIR";
pub const FLUVIO_IMAGE_TAG_STRATEGY: &str = "FLUVIO_IMAGE_TAG_STRATEGY";
pub const FLUVIO_ALWAYS_CHECK_UPDATES: &str = "FLUVIO_ALWAYS_CHECK_UPDATES";
