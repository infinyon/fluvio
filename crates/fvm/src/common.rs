/// Home Directory for Fluvio
pub const FLUVIO_HOME_DIR: &str = ".fluvio";

/// Fluvio Binary Name
pub const FLUVIO_BINARY_NAME: &str = "fluvio";

/// Home Directory for the Fluvio Version Manager (FVM) CLI
pub const FVM_HOME_DIR: &str = ".fvm";

/// FVM Binary Name
pub const FVM_BINARY_NAME: &str = "fvm";

/// FVM Packages Set Directory Name
pub const FVM_PACKAGES_SET_DIR: &str = "pkgset";

/// The URL of the Infinyon Hub's FVM API
pub const INFINYON_HUB_URL: &str = "https://hub.infinyon.cloud";

/// The URI of the Infinyon Hub's FVM API Package Set Endpoint
pub const INFINYON_HUB_FVM_PKGSET_API_URI: &str = "/hub/v1/fvm/pkgset";

/// The Target Architecture of the current build (e.g. "aarch64-apple-darwin")
pub const TARGET: &str = env!("TARGET");
