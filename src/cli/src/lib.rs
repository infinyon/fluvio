mod http;
mod error;
mod root_cli;
mod install;
mod profile;

use self::error::{Result, CliError};
use fluvio_extension_common as common;
pub use root_cli::Root;

const VERSION: &str = include_str!("VERSION");
