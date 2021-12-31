pub mod http;
pub mod install;
pub mod error;

// Environment vars for Channels
pub const FLUVIO_RELEASE_CHANNEL: &str = "FLUVIO_RELEASE_CHANNEL";
pub const FLUVIO_EXTENSIONS_DIR: &str = "FLUVIO_EXTENSIONS_DIR";
pub const FLUVIO_IMAGE_TAG_STRATEGY: &str = "FLUVIO_IMAGE_TAG_STRATEGY";
