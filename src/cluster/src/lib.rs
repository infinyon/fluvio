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
//! async_std::task::block_on(installer.install_fluvio()).unwrap();
//! ```
//!
//! [`ClusterInstaller`]: ./struct.ClusterInstaller.html

#![warn(missing_docs)]

mod helm;
mod install;
mod error;
mod releases;

pub use install::ClusterInstaller;
pub use install::ClusterInstallerBuilder;
pub use error::ClusterError;
pub use releases::list_releases;

const VERSION: &str = include_str!("VERSION");
