pub mod cluster;
pub mod group;
pub mod spu;
mod error;

use self::error::{ ClusterCmdError, Result};
use fluvio_extension_common as common;