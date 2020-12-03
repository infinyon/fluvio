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

mod check;
mod start;
mod delete;
mod local;
mod error;

/// extensions
#[cfg(feature = "cli")]
pub mod cli;

use fluvio_helm as helm;

pub use start::ClusterInstaller;
pub use start::ClusterInstallerBuilder;
pub use error::{ClusterError, K8InstallError, LocalInstallError, UninstallError};
pub use helm::HelmError;
pub use check::CheckResults;
pub use check::ClusterChecker;
pub use check::UnrecoverableCheck;
pub use delete::ClusterUninstaller;
pub use local::LocalClusterInstaller;

const VERSION: &str = include_str!("VERSION");

/// The result of a successful startup of a Fluvio cluster
///
/// A `StartStatus` carries additional information about the startup
/// process beyond the simple fact that the startup succeeded. It
/// contains the address of the Streaming Controller (SC) of the new
/// cluster as well as the results of any pre-startup checks that
/// were run (if any).
pub struct StartStatus {
    address: String,
    pub(crate) checks: Option<CheckResults>,
}

impl StartStatus {
    /// The address where the newly-started Fluvio cluster lives
    pub fn address(&self) -> &str {
        &self.address
    }
}
