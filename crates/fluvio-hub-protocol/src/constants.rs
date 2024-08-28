use const_format::concatcp;

pub const CLI_CONFIG_HUB: &str = "hub";

/// HUB API URL chunks
pub const HUB_API_V: &str = "hub/v0";
pub const HUB_API_ACT: &str = concatcp!(HUB_API_V, "/action");
pub const HUB_API_HUBID: &str = concatcp!(HUB_API_V, "/hubid");

// sm specific api
pub const HUB_API_SM: &str = concatcp!(HUB_API_V, "/pkg/pub");
pub const HUB_API_LIST: &str = concatcp!(HUB_API_V, "/list");
pub const HUB_API_LIST_META: &str = concatcp!(HUB_API_V, "/list_with_meta");

// connector specific api
pub const HUB_API_CONN_PKG: &str = concatcp!(HUB_API_V, "/connector/pkg");
pub const HUB_API_CONN_LIST: &str = concatcp!(HUB_API_V, "/connector/list");

// sdf specific api
pub const HUB_API_SDF: &str = concatcp!(HUB_API_V, "/sdf");
pub const HUB_API_SDF_PKG: &str = concatcp!(HUB_API_V, "/sdf/pkg");
pub const HUB_API_SDF_LIST: &str = concatcp!(HUB_API_V, "/sdf/list");
pub const HUB_API_SDF_LIST_META: &str = concatcp!(HUB_API_V, "/sdf/list_with_meta");
pub const HUB_API_SDF_PKG_PUB: &str = concatcp!(HUB_API_V, "/sdf/pkg/pub/pkg");
pub const HUB_API_SDF_DATAFLOW_PUB: &str = concatcp!(HUB_API_V, "/sdf/pkg/pub/dataflow");

pub const HUB_MANIFEST_BLOB: &str = "manifest.tar.gz";
pub const HUB_PACKAGE_EXT: &str = "ipkg";
pub const HUB_PACKAGE_META: &str = "package-meta.yaml";
pub const HUB_PACKAGE_META_CLEAN: &str = "package-meta-clean.yaml";
pub const HUB_PACKAGE_VERSION: &str = "0.3";
pub const HUB_REMOTE: &str = "https://hub.infinyon.cloud";
pub const HUB_SIGNFILE_BASE: &str = "signature";

pub const DEF_CARGO_TOML_PATH: &str = "Cargo.toml";
pub const DEF_HUB_INIT_DIR: &str = ".hub";
pub const DEF_HUB_PKG_META: &str = concatcp!(DEF_HUB_INIT_DIR, "/", HUB_PACKAGE_META); // .hub/package-meta.yaml

// This is required in sdf hub package_meta manifests
pub const SDF_PKG_KIND: &str = "sdf-kind";
pub const SDF_PKG_KIND_DATAFLOW: &str = "dataflow";
pub const SDF_PKG_KIND_PACKAGE: &str = "pkg";
