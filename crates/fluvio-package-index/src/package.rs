use tracing::debug;
use serde::{Serialize, Deserialize};
use semver::Version;
use crate::{PackageName, GroupName, PackageId, Error, Result, Target, MaybeVersion};

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    releases: Vec<Release>,
}

impl Package {
    pub fn new_binary<S1, S2, S3, V>(id: &PackageId<V>, author: S1, desc: S2, repo: S3) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
        S3: Into<String>,
    {
        let author = author.into();
        let description = desc.into();
        let repository = repo.into();
        Package {
            name: id.name().clone(),
            group: id.group().clone(),
            kind: PackageKind::Binary,
            author: Some(author),
            description: Some(description),
            repository: Some(repository),
            releases: vec![],
        }
    }

    /// Returns a reference to the latest release for this package
    pub fn latest_release(&self) -> Result<&Release> {
        debug!(releases = ?&self.releases, "Finding latest release");
        // Since releases are sorted upon insert, we just need to grab the last one
        self.releases
            .last()
            .ok_or_else(|| Error::NoReleases(self.package_id().to_string()))
    }

    /// Returns a reference to the latest release with this target
    ///
    /// If `prerelease` is false, this will return only the latest release
    /// whose version does not include a prerelease tag.
    pub fn latest_release_for_target(&self, target: &Target, prerelease: bool) -> Result<&Release> {
        self.releases
            .iter()
            .rev()
            .find(|it| {
                // If not in prerelease mode, do not keep prerelease or build meta
                if !prerelease && (!it.version.pre.is_empty() || !it.version.build.is_empty()) {
                    return false;
                }
                it.targets.contains(target)
            })
            .ok_or_else(|| Error::MissingTarget(target.clone()))
    }

    fn package_id(&self) -> PackageId<MaybeVersion> {
        PackageId::new_unversioned(self.name.clone(), self.group.clone())
    }

    /// Adds a new release to this package. This will reject a release if a release by the same version exists.
    pub fn add_release(&mut self, version: Version, target: Target) -> Result<()> {
        // See if there are any releases with the given version
        let maybe_release = self
            .releases
            .iter_mut()
            .find(|it| version_exactly_eq(&it.version, &version));

        match maybe_release {
            // If a release with this version exists, just add the target to it
            Some(release) => release.add_target(target),
            // If a release with this version does not exist, create it
            None => {
                let release = Release::new(version, target);
                self.releases.push(release);
                self.releases.sort_by(|a, b| a.version.cmp(&b.version));
            }
        }

        Ok(())
    }

    pub fn releases_for_target(&self, target: &Target) -> Vec<&Release> {
        self.releases
            .iter()
            .filter(|it| it.targets.contains(target))
            .collect()
    }
}

fn version_exactly_eq(a: &Version, b: &Version) -> bool {
    a.eq(b) && a.build.eq(&b.build)
}

/// Packages have a `PackageKind`, which describes the contents being distributed.
///
/// This is used by installers and updaters to determine what the installation
/// strategy should be for a specific type of package. For example, binaries need
/// to be placed into the PATH, but libraries may need to be installed in a
/// target-specific way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackageKind {
    /// An executable binary package, "bin".
    Binary,
    /// Anything we don't recognize. This is here to prevent breaking changes
    /// if the registry adds new package kinds and not all clients are updated.
    Unknown(String),
}

impl Serialize for PackageKind {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let value = match self {
            Self::Binary => "bin".serialize(serializer)?,
            Self::Unknown(other) => other.serialize(serializer)?,
        };
        Ok(value)
    }
}

impl<'de> Deserialize<'de> for PackageKind {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        let kind = match &*string {
            "bin" => Self::Binary,
            _ => Self::Unknown(string),
        };
        Ok(kind)
    }
}

/// A `Release` is a specific version of a published item in Fluvio's registry.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Release {
    /// The version of the package that this release holds
    pub version: Version,
    /// If a release is yanked, no client should ever try to download it.
    /// A yanked package may have its permalink taken down.
    pub yanked: bool,
    /// The targets that have published releases with this version
    targets: Vec<Target>,
}

impl Release {
    pub fn new(version: Version, target: Target) -> Self {
        Self {
            version,
            yanked: false,
            targets: vec![target],
        }
    }

    /// Adds a target to this release. If that target already exists,
    /// nothing happens
    pub fn add_target(&mut self, target: Target) {
        if !self.target_exists(&target) {
            self.targets.push(target);
        }
    }

    pub fn target_exists(&self, target: &Target) -> bool {
        self.targets.iter().any(|it| it == target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_package() -> Package {
        Package {
            name: "my-package".parse().unwrap(),
            group: "my-group".parse().unwrap(),
            kind: PackageKind::Binary,
            author: None,
            description: None,
            repository: None,
            releases: vec![
                Release {
                    version: Version::parse("0.1.0-alpha.1").unwrap(),
                    yanked: false,
                    targets: vec![Target::X86_64AppleDarwin],
                },
                Release {
                    version: Version::parse("0.1.0").unwrap(),
                    yanked: false,
                    targets: vec![Target::X86_64AppleDarwin],
                },
                Release {
                    version: Version::parse("0.2.0-alpha.1").unwrap(),
                    yanked: false,
                    targets: vec![Target::X86_64AppleDarwin],
                },
                Release {
                    version: Version::parse("0.2.0-alpha.2").unwrap(),
                    yanked: false,
                    targets: vec![Target::X86_64AppleDarwin],
                },
            ],
        }
    }

    #[test]
    fn test_serialize_package() {
        let id: PackageId<MaybeVersion> = "fluvio/fluvio".parse().unwrap();
        let package = Package::new_binary(&id, "Bob", "A package", "https://github.com");
        let stringified = serde_json::to_string(&package).unwrap();
        assert_eq!(
            stringified,
            r#"{"name":"fluvio","group":"fluvio","kind":"bin","author":"Bob","description":"A package","repository":"https://github.com"}"#
        )
    }

    #[test]
    fn test_deserialize_package() {
        let json = r#"{
          "name": "fluvio",
          "group": "fluvio",
          "kind": "bin",
          "author": "Fluvio Contributors",
          "description": "The Fluvio CLI, an all-in-one toolkit for streaming with Fluvio",
          "repository": "https://github.com/infinyon/fluvio",
          "releases": [
            { 
              "version": "0.8.4",
              "yanked": false,
              "targets": ["x86_64-unknown-linux-musl", "x86_64-apple-darwin", "armv7-unknown-linux-gnueabihf"]
            }
          ]
        }"#;
        let package: Package = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "fluvio".parse().unwrap());
        assert_eq!(package.group, "fluvio".parse().unwrap());
        assert_eq!(package.kind, PackageKind::Binary);
        assert_eq!(package.author, Some("Fluvio Contributors".to_string()));
        assert_eq!(
            package.description,
            Some("The Fluvio CLI, an all-in-one toolkit for streaming with Fluvio".to_string())
        );
        assert_eq!(package.releases.len(), 1);

        let release = &package.releases[0];
        assert_eq!(release.version, semver::Version::parse("0.8.4").unwrap());
        assert!(!release.yanked);
        assert_eq!(release.targets.len(), 3);
        assert_eq!(release.targets[0], Target::X86_64UnknownLinuxMusl);
        assert_eq!(release.targets[1], Target::X86_64AppleDarwin);
        assert_eq!(
            release.targets[2],
            "armv7-unknown-linux-gnueabihf".parse().unwrap()
        );
    }

    #[test]
    fn test_deserialize_package_unknown_kind() {
        let json = r#"{
          "name": "fluvio-smartmodule-filter",
          "group": "fluvio",
          "kind": "wasm",
          "releases": []
        }"#;
        let package: Package = serde_json::from_str(json).unwrap();
        assert_eq!(package.name, "fluvio-smartmodule-filter".parse().unwrap());
        assert_eq!(package.group, "fluvio".parse().unwrap());
        assert_eq!(package.kind, PackageKind::Unknown("wasm".to_string()));
        assert!(package.releases.is_empty());
    }

    #[test]
    fn test_get_latest_prerelease() {
        let package = test_package();
        let release = package
            .latest_release_for_target(&Target::X86_64AppleDarwin, true)
            .unwrap();
        assert_eq!(release.version, Version::parse("0.2.0-alpha.2").unwrap());
    }

    #[test]
    fn test_get_latest_release() {
        let package = test_package();
        let release = package
            .latest_release_for_target(&Target::X86_64AppleDarwin, false)
            .unwrap();
        assert_eq!(release.version, Version::parse("0.1.0").unwrap());
    }

    #[test]
    fn test_deserialize_package_kind_bin() {
        let name = "\"bin\"";
        let kind: PackageKind = serde_json::from_str(name).unwrap();
        assert_eq!(kind, PackageKind::Binary);
    }

    #[test]
    fn test_serialize_package_kind_bin() {
        let kind = PackageKind::Binary;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, "\"bin\"");
    }

    #[test]
    fn test_deserialize_package_kind_unknown() {
        let name = "\"wasm\"";
        let kind: PackageKind = serde_json::from_str(name).unwrap();
        assert_eq!(kind, PackageKind::Unknown("wasm".to_string()));
    }
}
