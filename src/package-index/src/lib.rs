//! Data structures and domain logic for reading the Fluvio Package Index.
//!
//! This crate is used by the plugin installer and the self-updater. It
//! is capable of reading the index file in the Fluvio Package Registry
//! in order to find the latest release versions of various components.
//!
//! The two main use-cases for this are to allow the CLI to install plugins,
//! e.g. via `fluvio install fluvio-cloud`, and to give the CLI visibility
//! of new releases for itself and plugins.

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

mod error;
mod package_id;

pub use error::{Error, PackageIdError};
pub use package_id::{PackageId, GroupName, PackageName, Registry};

pub type Result<T> = std::result::Result<T, Error>;

pub const INDEX_CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Serialize, Deserialize)]
pub struct FluvioIndex {
    /// Metadata about the Fluvio Index itself
    #[serde(rename = "index")]
    pub metadata: IndexMetadata,
    /// Packages are organized by group, where a group is like a particular
    /// user which publishes packages. For the initial index, there will only
    /// be an official Fluvio group, but in the future there may be more.
    /// This will prevent clashes in the package namespace.
    pub groups: BTreeMap<GroupName, GroupPackages>,
}

impl FluvioIndex {
    pub fn find_package(&self, id: &PackageId) -> Result<&Package> {
        let group = self.group(&id.group)?;
        let package = group.package(&id.name)?;
        Ok(package)
    }

    pub fn find_release(&self, id: &PackageId) -> Result<&Release> {
        let version = id.version.as_ref().ok_or(PackageIdError::MissingVersion)?;
        let group = self.group(&id.group)?;
        let package = group.package(&id.name)?;
        let release = package.find_release(&version)?;
        Ok(release)
    }

    pub fn add_package(&mut self, package: Package) -> Result<()> {
        let group = self.groups.entry(package.group.clone())
            .or_insert_with(|| GroupPackages::empty(package.group.clone()));
        group.add_package(package.name.clone(), package)?;
        Ok(())
    }

    pub fn add_release(&mut self, id: &PackageId, release: Release) -> Result<()> {
        let group = self.group_mut(&id.group)?;
        let package = group.package_mut(&id.name)?;
        package.add_release(release)?;
        Ok(())
    }

    fn empty(metadata: IndexMetadata) -> FluvioIndex {
        FluvioIndex {
            metadata,
            groups: BTreeMap::new(),
        }
    }

    fn group(&self, group: &GroupName) -> Result<&GroupPackages> {
        self.groups.get(group).ok_or_else(|| Error::MissingGroup(group.clone()))
    }

    fn group_mut(&mut self, group: &GroupName) -> Result<&mut GroupPackages> {
        self.groups.get_mut(group).ok_or_else(|| Error::MissingGroup(group.clone()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// The index itself is given a version so that installers and updaters
    /// can print upgrade prompts to users. If an installer is expecting an
    /// index version `1.0.0` and finds an index with a version such as
    /// `2.0.0`, the installer will know to prompt the user to self-update
    /// the installer. The original index is expected to continue to be available
    /// forever, and, critically, new versions of the installer must always
    /// be published to the original index. New versions of the installer may
    /// then point to future versions of the Fluvio Index, which may exhibit
    /// breaking changes.
    pub version: semver::Version,
    /// The location of this registry as a URL
    pub registry: Registry,
    /// The minimum version of a client which must be used in order
    /// to properly access the index. If a client finds itself with a lower
    /// version than this minimum, it must prompt the user for an update before
    /// it can proceed.
    pub minimum_client_version: semver::Version,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupPackages {
    group: GroupName,
    packages: BTreeMap<PackageName, Package>,
}

impl GroupPackages {
    pub fn empty(group: GroupName) -> Self {
        Self {
            group,
            packages: BTreeMap::new(),
        }
    }

    /// Adds the given package with the given name to this group.
    ///
    /// This will reject packages that already exist.
    pub fn add_package(&mut self, name: PackageName, package: Package) -> Result<()> {
        if self.packages.contains_key(&name) {
            return Err(Error::PackageAlreadyExists(format!("{}/{}", self.group, name.as_str())));
        }
        self.packages.insert(name, package);
        Ok(())
    }

    fn package(&self, package: &PackageName) -> Result<&Package> {
        self.packages.get(package).ok_or_else(|| Error::MissingPackage(package.clone()))
    }

    fn package_mut(&mut self, package: &PackageName) -> Result<&mut Package> {
        self.packages.get_mut(package).ok_or_else(|| Error::MissingPackage(package.clone()))
    }
}

/// A `Package` represents a single published item in Fluvio's registry.
///
/// Each time you publish an updated version of a package, that version is
/// known as a `Release`.
///
/// A package has a specified type, and all releases of that package must
/// be the same type.
#[derive(Debug, Serialize, Deserialize)]
pub struct Package {
    /// The unique name of this package
    pub name: PackageName,
    /// The ID of the group that published the package
    pub group: GroupName,
    /// The type of package this is
    pub kind: PackageKind,
    /// The author of this package
    pub author: Option<String>,
    /// The human-readable description of this package
    pub description: Option<String>,
    /// A link to the source code repository of this package
    pub repository: Option<String>,
    /// The instances of this package that have been published
    #[serde(skip_serializing_if = "Vec::is_empty")]
    releases: Vec<Release>,
}

impl Package {
    pub fn new_binary<S1, S2, S3>(id: &PackageId, author: S1, desc: S2, repo: S3) -> Self
        where S1: Into<String>,
              S2: Into<String>,
              S3: Into<String>,
    {
        let author = author.into();
        let description = desc.into();
        let repository = repo.into();
        Package {
            name: id.name.clone(),
            group: id.group.clone(),
            kind: PackageKind::Binary,
            author: Some(author),
            description: Some(description),
            repository: Some(repository),
            releases: vec![],
        }
    }

    fn find_release(&self, version: &semver::Version) -> Result<&Release> {
        self.releases.iter().find(|it| it.version == *version)
            .ok_or_else(|| Error::MissingRelease(version.clone()))
    }

    /// Adds a new release to this package. This will reject a release if a release by the same version exists.
    ///
    /// Version equality is based strictly on the numeric components of a semantic
    /// version. Therefore, if a release with version `0.1.0-alpha` exists, you
    /// cannot add a release with version `0.1.0-beta`, since there is no way to know
    /// which of those is more recent.
    fn add_release(&mut self, new_release: Release) -> Result<()> {
        // This will reject adding a release if the version numbers are the same
        let maybe_conflict = self.releases.iter().find(|it| it.version == new_release.version);
        if let Some(conflict) = maybe_conflict {
            return Err(Error::ReleaseAlreadyExists(conflict.version.clone()));
        }

        self.releases.push(new_release);
        self.releases.sort_by(|a, b| a.version.cmp(&b.version));
        Ok(())
    }
}

/// Packages have a `PackageKind`, which describes the contents being distributed.
///
/// This is used by installers and updaters to determine what the installation
/// strategy should be for a specific type of package. For example, binaries need
/// to be placed into the PATH, but libraries may need to be installed in a
/// platform-specific way.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PackageKind {
    #[serde(rename = "bin")]
    Binary,
}

/// A `Release` is a specific version of a published item in Fluvio's registry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Release {
    /// The version of the package that this release holds
    version: semver::Version,
    /// A permalink where this release may be downloaded
    permalink: url::Url,
    /// The hex-encoded SHA2 hash of the package contents
    checksum: String,
    /// If a release is yanked, no client should ever try to download it.
    /// A yanked package may have its permalink taken down.
    yanked: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_path() -> PathBuf {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/bin/index.json");
        println!("Path: {}", path.display());
        path
    }

    fn sample_index() -> FluvioIndex {
        let release = Release {
            version: semver::Version::parse("1.0.0").unwrap(),
            permalink: url::Url::parse("https://packages.fluvio.io/v1/packages/fluvio-cli/1.0.0/fluvio").unwrap(),
            checksum: "BLAAAAAH".to_string(),
            yanked: false,
        };
        let package_name = "fluvio-cli".parse::<PackageName>().unwrap();
        let group_name = "fluvio".parse::<GroupName>().unwrap();
        let package = Package {
            name: package_name.clone(),
            group: group_name.clone(),
            kind: PackageKind::Binary,
            author: None,
            description: None,
            repository: None,
            releases: vec![release],
        };
        let group_packages = GroupPackages {
            group: group_name.clone(),
            packages: {
                let mut packages = BTreeMap::new();
                packages.insert(package_name, package);
                packages
            }
        };
        let metadata = IndexMetadata {
            version: semver::Version::parse("0.1.0").unwrap(),
            registry: "https://packages.fluvio.io/v1/".parse().unwrap(),
            minimum_client_version: semver::Version::parse("0.6.0-alpha-1").unwrap(),
        };
        let index = FluvioIndex {
            metadata,
            groups: {
                let mut groups = BTreeMap::new();
                groups.insert(group_name, group_packages);
                groups
            }
        };
        index
    }

    fn write_index() {
        let metadata = IndexMetadata {
            version: semver::Version::parse("0.1.0").unwrap(),
            registry: "https://packages.fluvio.io/v1/".parse().unwrap(),
            minimum_client_version: semver::Version::parse("0.6.0-alpha-1").unwrap(),
        };
        let index = FluvioIndex::empty(metadata);
        let index_string = serde_json::to_string_pretty(&index).unwrap();
        std::fs::write(test_path(), &index_string).unwrap();
    }

    fn test_create_index() {
        let index = sample_index();
        let index_string = serde_json::to_string_pretty(&index).unwrap();
        std::fs::write(test_path(), &index_string).unwrap();
    }

    #[test]
    fn test_find_release() {
        let index = sample_index();
        let id = "fluvio/fluvio-cli:1.0.0".parse().unwrap();
        let release = index.find_release(&id).unwrap();
        assert_eq!(release.version, semver::Version::parse("1.0.0").unwrap());
        assert_eq!(release.permalink, url::Url::parse("https://packages.fluvio.io/v1/packages/fluvio-cli/1.0.0/fluvio").unwrap());
    }

    #[test]
    fn test_add_package() {
        let mut starting_index = sample_index();
        let id = "fluvio/fluvio-cloud".parse::<PackageId>().unwrap();
        let package = Package::new_binary(
            &id,
            "Bobson",
            "The fluvio cloud plugin",
            "https://github.com/infinyon/fluvio",
        );
        starting_index.add_package(package);
    }

    #[test]
    fn test_serialize_package() {
        let id = "fluvio/fluvio".parse().unwrap();
        let package = Package::new_binary(&id, "Bob", "A package", "https://github.com");
        let stringified = serde_json::to_string(&package).unwrap();
        assert_eq!(stringified, r#"{"name":"fluvio","group":"fluvio","kind":"bin","author":"Bob","description":"A package","repository":"https://github.com"}"#)
    }

    #[test]
    fn test_add_release() {
        let mut index = sample_index();
        let new_release = Release {
            version: semver::Version::parse("1.1.0").unwrap(),
            permalink: url::Url::parse("https://packages.fluvio.io/v1/packages/fluvio-cli/1.1.0/fluvio").unwrap(),
            checksum: "BLAAAAAH".to_string(),
            yanked: false,
        };
        let pid = "fluvio/fluvio-cli:1.1.0".parse().unwrap();
        index.add_release(&pid, new_release.clone()).unwrap();
        let searched_release = index.find_release(&pid).unwrap();
        assert_eq!(new_release, *searched_release);
    }

    #[test]
    fn test_releases_sorted() {
        let mut index = sample_index();
        let release_2 = Release {
            version: semver::Version::parse("2.0.0").unwrap(),
            permalink: url::Url::parse("https://packages.fluvio.io/v1/packages/fluvio-cli/2.0.0/fluvio").unwrap(),
            checksum: "BLAAAAAH".to_string(),
            yanked: false,
        };
        let pid = "fluvio/fluvio-cli:2.0.0".parse().unwrap();
        index.add_release(&pid, release_2.clone()).unwrap();

        let release_1_1 = Release {
            version: semver::Version::parse("1.1.0").unwrap(),
            permalink: url::Url::parse("https://packages.fluvio.io/v1/packages/fluvio-cli/1.1.0/fluvio").unwrap(),
            checksum: "BLAAAAAH".to_string(),
            yanked: false,
        };
        let id = "fluvio/fluvio-cli:1.1.0".parse().unwrap();
        index.add_release(&id, release_1_1.clone()).unwrap();

        let group_packages = index.group(&id.group).unwrap();
        let package = group_packages.package(&id.name).unwrap();

        let releases = package.releases.clone();
        for windows in releases.windows(2) {
            match &windows {
                &[a, b] => {
                    println!("Is {} < {}?", a.version, b.version);
                    assert!(a.version < b.version);
                }
                _ => panic!("Should have 2 windows"),
            }
        }
    }
}
