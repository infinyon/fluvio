//! Functionality for installing, managing, and deleting Fluvio clusters.
//!
//! The primary use of this crate is to install Fluvio clusters on
//! Kubernetes using a [`ClusterInstaller`], which provides a fluid
//! interface for cluster specification.
//!
//! # Example
//!
//! To install a basic Fluvio cluster, just do the following:
//!
//! ```no_run
//! use fluvio_cluster::ClusterInstaller;
//! let installer = ClusterInstaller::new().build().unwrap();
//! fluvio_future::task::run_block_on(installer.install_fluvio()).unwrap();
//! ```
//!
//! [`ClusterInstaller`]: ./struct.ClusterInstaller.html

mod install;
mod error;
mod check;
mod uninstall;
mod local;

/// extensions
#[cfg(feature = "cmd_extension")]
pub mod extension;

use fluvio_helm as helm;

pub use install::ClusterInstaller;
pub use install::ClusterInstallerBuilder;
pub use error::ClusterError;
pub use check::ClusterChecker;
pub use check::CheckError;
pub use uninstall::ClusterUninstaller;
pub use local::LocalClusterInstaller;

const VERSION: &str = include_str!("VERSION");
