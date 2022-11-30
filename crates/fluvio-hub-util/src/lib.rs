mod errors;
mod hubaccess;
mod package;
mod packagemeta;
mod utils;
mod infinyon_tok;

use const_format::concatcp;

pub use hubaccess::*;
pub mod keymgmt;
pub use package::*;
pub use packagemeta::*;
pub use errors::HubUtilError;
pub use utils::*;
pub use infinyon_tok::read_infinyon_token;
pub use surf as http;

pub const CLI_CONFIG_HUB: &str = "hub";

pub const HUB_PACKAGE_VERSION: &str = "0.2";
pub const HUB_PACKAGE_META: &str = "package-meta.yaml";
pub const HUB_REMOTE: &str = "https://hub.infinyon.cloud";

pub const DEF_CARGO_TOML_PATH: &str = "Cargo.toml";
pub const DEF_HUB_INIT_DIR: &str = "hub";
pub const DEF_HUB_PKG_META: &str = concatcp!(DEF_HUB_INIT_DIR, "/", HUB_PACKAGE_META); // hub/package-meta.yaml
pub const HUB_PACKAGE_EXT: &str = "ipkg";

const HUB_PACKAGE_META_CLEAN: &str = "package-meta-clean.yaml";
const HUB_MANIFEST_BLOB: &str = "manifest.tar.gz";
const HUB_SIGNFILE_BASE: &str = "signature";

// HUB API URL chunks
pub const HUB_API_V: &str = "hub/v0";
pub const HUB_API_SM: &str = concatcp!(HUB_API_V, "/pkg/pub");
pub const HUB_API_ACT: &str = concatcp!(HUB_API_V, "/action");
pub const HUB_API_HUBID: &str = concatcp!(HUB_API_V, "/hubid");
pub const HUB_API_LIST: &str = concatcp!(HUB_API_V, "/list");
