//! Data structures and domain logic for reading the Fluvio Package Index.
//!
//! This crate is used by the plugin installer and the self-updater. It
//! is capable of reading the index file in the Fluvio Package Registry
//! in order to find the latest release versions of various components.
//!
//! The two main use-cases for this are to allow the CLI to install plugins,
//! e.g. via `fluvio install fluvio-cloud`, and to give the CLI visibility
//! of new releases for itself and plugins.

use serde::{Serialize, Deserialize};

mod tags;
#[cfg(feature = "http_agent")]
mod http;
mod error;
mod target;
mod version;
mod package;
mod package_id;

#[cfg(feature = "http_agent")]
pub use crate::http::HttpAgent;

pub use tags::TagName;
pub use error::{Error, Result};
pub use target::{Target, package_target};
pub use version::PackageVersion;
pub use package::{Package, PackageKind, Release};
pub use package_id::{PackageId, GroupName, PackageName, Registry, WithVersion, MaybeVersion};
use semver::Version;

pub const INDEX_HOST: &str = "https://packages.fluvio.io/";
pub const INDEX_LOCATION: &str = "https://packages.fluvio.io/v1/";
pub const INDEX_CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// The minimum version of a client which must be used in order
    /// to properly access the index. If a client finds itself with a lower
    /// version than this minimum, it must prompt the user for an update before
    /// it can proceed.
    ///
    /// This version number corresponds to the crate version of the
    /// `fluvio-package-index` crate.
    pub minimum_client_version: Version,
}

impl IndexMetadata {
    /// This checks whether this version of the client is compatible with the given index.
    ///
    /// This will return `true` if the `minimum_client_version` of the index is
    /// greater than this version of the `fluvio-package-index` crate.
    pub fn update_required(&self) -> bool {
        let client_version = Version::parse(INDEX_CLIENT_VERSION).unwrap();
        let required_version = &self.minimum_client_version;
        *required_version > client_version
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FluvioIndex {
    /// Metadata about the Fluvio Index itself
    #[serde(alias = "index")]
    pub metadata: IndexMetadata,
}
