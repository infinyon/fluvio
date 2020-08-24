//! Functionality for installing, managing, and deleting Fluvio clusters.

mod helm;
mod install;
mod error;

pub use install::ClusterInstaller;
pub use install::KubeOptions;

pub use error::ClusterError;

const VERSION: &str = include_str!("VERSION");
