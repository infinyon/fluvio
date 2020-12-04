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

#![warn(missing_docs)]

mod check;
mod start;
mod delete;
mod error;

/// extensions
#[cfg(feature = "cli")]
pub mod cli;

use fluvio_helm as helm;

pub use start::k8::ClusterInstaller;
pub use start::k8::ClusterInstallerBuilder;
pub use start::local::LocalClusterInstaller;
pub use error::{ClusterError, K8InstallError, LocalInstallError, UninstallError};
pub use helm::HelmError;
pub use check::{ClusterChecker, CheckStatus, CheckStatuses};
pub use check::{RecoverableCheck, UnrecoverableCheck};
pub use delete::ClusterUninstaller;

const VERSION: &str = include_str!("VERSION");
pub(crate) const DEFAULT_NAMESPACE: &str = "default";
pub(crate) const DEFAULT_HELM_VERSION: &str = "3.3.4";
pub(crate) const DEFAULT_CHART_SYS_REPO: &str = "fluvio-sys";
pub(crate) const DEFAULT_CHART_APP_REPO: &str = "fluvio";

/// The result of a successful startup of a Fluvio cluster
///
/// A `StartStatus` carries additional information about the startup
/// process beyond the simple fact that the startup succeeded. It
/// contains the address of the Streaming Controller (SC) of the new
/// cluster as well as the results of any pre-startup checks that
/// were run (if any).
pub struct StartStatus {
    address: String,
    #[allow(unused)]
    pub(crate) checks: Option<CheckStatuses>,
}

impl StartStatus {
    /// The address where the newly-started Fluvio cluster lives
    pub fn address(&self) -> &str {
        &self.address
    }
}
