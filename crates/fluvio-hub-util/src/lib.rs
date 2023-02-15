mod hubaccess;
mod package;
mod package_meta_ext;
mod utils;

pub mod keymgmt;

use const_format::concatcp;

pub use hubaccess::*;
pub use package::*;
pub use package_meta_ext::*;
pub use utils::*;
pub use surf as http;

pub use fluvio_hub_protocol::*;
pub use fluvio_hub_protocol::constants::*;

// HUB API URL chunks
pub const HUB_API_V: &str = "hub/v0";
pub const HUB_API_SM: &str = concatcp!(HUB_API_V, "/pkg/pub");
pub const HUB_API_ACT: &str = concatcp!(HUB_API_V, "/action");
pub const HUB_API_HUBID: &str = concatcp!(HUB_API_V, "/hubid");
pub const HUB_API_LIST: &str = concatcp!(HUB_API_V, "/list");
