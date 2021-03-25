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
//! ```
//! use fluvio_cluster::{ClusterInstaller, ClusterConfig, ClusterError};
//! # async fn example() -> Result<(), ClusterError> {
//! let config = ClusterConfig::builder("0.7.0-alpha.1").build()?;
//! let installer = ClusterInstaller::from_config(config)?;
//! installer.install_fluvio().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`ClusterInstaller`]: ./struct.ClusterInstaller.html

#![warn(missing_docs)]
#![deny(broken_intra_doc_links)]
#![allow(clippy::upper_case_acronyms)]

use std::path::PathBuf;

mod check;
mod start;
mod delete;
mod error;
mod sys;

/// extensions
#[cfg(feature = "cli")]
pub mod cli;

use fluvio_helm as helm;

pub use start::k8::{ClusterInstaller, ClusterConfig, ClusterConfigBuilder};
pub use start::local::{LocalInstaller, LocalConfig, LocalConfigBuilder};
pub use error::{ClusterError, K8InstallError, LocalInstallError, UninstallError, SysInstallError};
pub use helm::HelmError;
pub use check::{ClusterChecker, CheckStatus, CheckStatuses, CheckResult, CheckResults};
pub use check::{RecoverableCheck, UnrecoverableCheck, CheckFailed, CheckSuggestion};
pub use delete::ClusterUninstaller;
pub use sys::{SysConfig, SysConfigBuilder, SysInstaller};
pub use fluvio::config as fluvio_config;

pub(crate) const DEFAULT_NAMESPACE: &str = "default";
pub(crate) const DEFAULT_HELM_VERSION: &str = "3.3.4";
pub(crate) const DEFAULT_CHART_SYS_REPO: &str = "fluvio-sys";
pub(crate) const DEFAULT_CHART_APP_REPO: &str = "fluvio";
pub(crate) const DEFAULT_CHART_REMOTE: &str = "https://charts.fluvio.io";

/// The result of a successful startup of a Fluvio cluster
///
/// A `StartStatus` carries additional information about the startup
/// process beyond the simple fact that the startup succeeded. It
/// contains the address of the Streaming Controller (SC) of the new
/// cluster as well as the results of any pre-startup checks that
/// were run (if any).
/// TODO: In future release, we should return address without port
pub struct StartStatus {
    address: String,
    port: u16,
    #[allow(unused)]
    pub(crate) checks: Option<CheckStatuses>,
}

impl StartStatus {
    /// The address where the newly-started Fluvio cluster lives
    pub fn address(&self) -> &str {
        &self.address
    }

    /// get port
    #[allow(unused)]
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Distinguishes between a Local and Remote helm chart
#[derive(Debug, Clone)]
pub enum ChartLocation {
    /// Local charts must be located at a valid filesystem path.
    Local(PathBuf),
    /// Remote charts will be located at a URL such as `https://...`
    Remote(String),
}
