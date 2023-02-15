use std::default::Default;

use serde::{Deserialize, Serialize};
use tracing::{info, error};

use fluvio_controlplane_metadata::smartmodule::FluvioSemVersion;
use fluvio_controlplane_metadata::smartmodule::SmartModulePackageKey;
use fluvio_controlplane_metadata::smartmodule::SmartModuleVisibility;
use fluvio_controlplane_metadata::smartmodule as smpkg;

use crate::{HubError, Result};
use crate::constants::{HUB_PACKAGE_EXT, HUB_PACKAGE_VERSION};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
/// defines hub package metadata
pub struct PackageMeta {
    pub package_format_version: String, // SerVer
    pub name: String,
    pub version: String, // SemVer?, package version
    pub group: String,
    // author: Option<String>,
    pub description: String,
    pub license: String,

    #[serde(default = "PackageMeta::visibility_if_missing")]
    pub visibility: PkgVisibility, // private is default if missing
    pub manifest: Vec<String>, // Files in package, package-meta is implied, signature is omitted
    // repository: optional url
    // repository-commit: optional hash
    pub tags: Option<Vec<PkgTag>>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum PkgVisibility {
    #[default]
    Private,
    Public,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
pub struct PkgTag {
    pub tag: String,
    pub value: String,
}

impl Default for PackageMeta {
    fn default() -> PackageMeta {
        PackageMeta {
            package_format_version: HUB_PACKAGE_VERSION.into(),
            name: "NameOfThePackage".into(),
            version: "0.0".into(),
            group: "NameOfContributingGroup".into(),
            description: "Describe the module here".into(),
            license: "e.g. Apache2".into(),
            visibility: PkgVisibility::Private,
            manifest: Vec::new(),
            tags: None,
        }
    }
}

impl PackageMeta {
    /// Retrieves the fully qualified name for the `PackageMeta`.
    ///
    /// Eg: `infinyon-example-0.0.1`
    pub fn id(&self) -> Result<String> {
        let fluvio_semver = FluvioSemVersion::parse(&self.version)
            .map_err(|err| HubError::SemVerError(err.to_string()))?;
        let package_key = SmartModulePackageKey {
            name: self.name.clone(),
            group: Some(self.group.clone()),
            version: Some(fluvio_semver),
        };

        Ok(package_key.store_id())
    }

    /// Retrives the package name from this package. Eg: `infinyon/example/0.0.1`
    pub fn pkg_name(&self) -> String {
        format!("{}/{}@{}", self.group, self.name, self.version)
    }

    /// Retrives the S3's object name from this package. Eg: `infinyon/example-0.0.1.ipkg`
    pub fn obj_name(&self) -> String {
        format!(
            "{}/{}-{}.{HUB_PACKAGE_EXT}",
            self.group, self.name, self.version
        )
    }

    /// Builds the S3 object path from the provided package name.
    ///
    /// A S3 object path is structure as `{group}/{pkgname}-{pkgver}.tar`, this
    /// can be build from the package name which holds the same data in a
    /// the following format: `{group}/{pkgname}@{pkgver}"`
    pub fn object_path_from_name(pkg_name: &str) -> Result<String> {
        let parts = pkg_name.split('/').collect::<Vec<&str>>();

        if parts.len() != 2 {
            error!("The provided name is not a valid package name: {pkg_name}");
            return Err(HubError::InvalidPackageName(pkg_name.into()));
        }

        let name_version = parts.get(1).unwrap().split('@').collect::<Vec<&str>>();

        if name_version.len() != 2 {
            error!("The provided name is not a valid package name: {pkg_name}");
            return Err(HubError::InvalidPackageName(pkg_name.into()));
        }

        Ok(format!(
            "{}/{}-{}.{HUB_PACKAGE_EXT}",
            parts[0], name_version[0], name_version[1]
        ))
    }

    /// the packagefile name as defined by the package meta data
    pub fn packagefile_name(&self) -> String {
        self.name.clone() + "-" + &self.version + "." + HUB_PACKAGE_EXT
    }

    /// the packagefile name as defined by the package meta data
    pub fn packagefile_name_unsigned(&self) -> String {
        self.name.clone() + "-" + &self.version + ".tar"
    }

    /// used by serde to fill in private field if missing on parse
    /// this helps support old versions of the package format
    pub fn visibility_if_missing() -> PkgVisibility {
        PkgVisibility::Private
    }

    /// Pull package-meta info from smartmodule meta toml
    pub fn update_from_smartmodule_toml(&mut self, fpath: &str) -> Result<()> {
        info!(fpath, "opening smartmodule toml");
        let spkg = smpkg::SmartModuleMetadata::from_toml(fpath)?;
        let spk = &spkg.package;

        packagename_validate(&spk.name)?;

        self.name = spk.name.clone();
        self.group = spk.group.clone();
        self.version = spk.version.to_string();
        self.description = spk.description.clone().unwrap_or_default();
        self.visibility = PkgVisibility::from(&spk.visibility);

        // needed for fluvio sm download
        self.manifest.push(fpath.into());
        Ok(())
    }

    /// simple because it's not the crypto validation
    pub fn naming_check(&self) -> Result<()> {
        let mut advice = String::new();

        advice.push_str(&validate_lowercase(&self.group, "group"));
        advice.push_str(&validate_allowedchars(&self.group, "group"));
        advice.push_str(&validate_notempty(&self.group, "group"));

        advice.push_str(&validate_lowercase(&self.name, "package name"));
        advice.push_str(&validate_allowedchars(&self.name, "package name"));
        advice.push_str(&validate_notempty(&self.name, "package name"));

        if !advice.is_empty() {
            Err(HubError::PackageVerify(advice))
        } else {
            Ok(())
        }
    }
}

impl PkgTag {
    pub fn new(tag: &str, val: &str) -> Self {
        PkgTag {
            tag: tag.to_string(),
            value: val.to_string(),
        }
    }
}
pub fn packagename_validate(pkgname: &str) -> Result<()> {
    let mut advice = String::new();

    advice.push_str(&validate_allowedchars(pkgname, "package name"));

    if !advice.is_empty() {
        Err(HubError::InvalidPackageName(advice))
    } else {
        Ok(())
    }
}

pub fn validate_notempty(val: &str, name: &str) -> String {
    if val.is_empty() {
        format!("{name} is empty\n")
    } else {
        String::new()
    }
}

pub fn validate_lowercase(val: &str, name: &str) -> String {
    if val.to_lowercase() != val {
        format!("{name} {val} should be lowercase\n")
    } else {
        String::new()
    }
}

pub fn validate_allowedchars(val: &str, name: &str) -> String {
    let good_chars = val
        .chars()
        .all(|ch| matches!(ch, 'a'..='z' | '0'..='9' | '-' | '_'));

    if !good_chars {
        format!("{name} {val} should be alphanumeric, '-' or '_'\n")
    } else {
        String::new()
    }
}

pub fn validate_noleading_punct(val: &str, name: &str) -> String {
    if let Some(c) = val.chars().next() {
        if matches!(c, '_' | '-') {
            return format!("{name} {val} no leading punctuation allowed '-' or '_'\n");
        }
    }
    String::new()
}

// from Smartmodule to package vaisiblity
impl From<&SmartModuleVisibility> for PkgVisibility {
    fn from(sm: &SmartModuleVisibility) -> Self {
        match sm {
            SmartModuleVisibility::Public => Self::Public,
            SmartModuleVisibility::Private => Self::Private,
        }
    }
}

#[test]
fn builds_obj_key_from_package_name() {
    let pkg_names = vec![
        "infinyon/example@0.0.1",
        "infinyon/example-sm@0.1.0",
        "infinyon/json-sql@0.0.2",
        "infinyon/test@0.1.0",
        "infinyon/hub-cli@0.1.0",
        "infinyon/test-cli@0.1.0",
        "infinyon/regex@0.0.1",
    ];
    let obj_paths = vec![
        "infinyon/example-0.0.1.ipkg",
        "infinyon/example-sm-0.1.0.ipkg",
        "infinyon/json-sql-0.0.2.ipkg",
        "infinyon/test-0.1.0.ipkg",
        "infinyon/hub-cli-0.1.0.ipkg",
        "infinyon/test-cli-0.1.0.ipkg",
        "infinyon/regex-0.0.1.ipkg",
    ];

    for (idx, name) in pkg_names.iter().enumerate() {
        assert_eq!(
            &PackageMeta::object_path_from_name(name).unwrap(),
            obj_paths.get(idx).unwrap()
        );
    }
}

// this end up mostly for any cli tools printing out the status
impl std::fmt::Display for PkgVisibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let lbl = match self {
            PkgVisibility::Private => "private",
            PkgVisibility::Public => "public",
        };
        write!(f, "{lbl}")
    }
}

#[test]
fn hub_package_id() {
    let pm = PackageMeta {
        group: "infinyon".into(),
        name: "example".into(),
        version: "0.0.1".into(),
        manifest: ["module.wasm".into()].to_vec(),
        ..PackageMeta::default()
    };

    assert_eq!("example-infinyon-0.0.1", pm.id().unwrap());
}

#[test]
#[should_panic(expected = "unexpected character 'T' while parsing major version number")]
fn hub_package_id_complains_invalid_semver() {
    let pm = PackageMeta {
        group: "infinyon".into(),
        name: "example".into(),
        version: "ThisIsNotSemVer".into(),
        manifest: ["module.wasm".into()].to_vec(),
        ..PackageMeta::default()
    };

    assert_eq!("example-infinyon-0.0.1", pm.id().unwrap());
}

#[test]
fn hub_packagefile_name() {
    let pm = PackageMeta {
        group: "infinyon".into(),
        name: "example".into(),
        version: "0.0.1".into(),
        manifest: ["module.wasm".into()].to_vec(),
        ..PackageMeta::default()
    };
    assert_eq!("example-0.0.1.ipkg", pm.packagefile_name());
}

#[test]
fn hub_packagemeta_naming_check() {
    let allow = vec![
        PackageMeta {
            group: "sleigh".into(),
            name: "dasher".into(),
            ..PackageMeta::default()
        },
        PackageMeta {
            group: "sleigh".into(),
            name: "rudolph-nose".into(),
            ..PackageMeta::default()
        },
        PackageMeta {
            group: "sleigh".into(),
            name: "sack_gift1".into(),
            ..PackageMeta::default()
        },
        PackageMeta {
            group: "halloween-pumpkin".into(),
            name: "lantern".into(),
            ..PackageMeta::default()
        },
    ];
    for pm in allow {
        let res = pm.naming_check();
        assert!(res.is_ok(), "Denied an allowed package meta config {pm:?}");
    }

    let deny = vec![
        PackageMeta {
            group: "Coal.com".into(),
            name: "example".into(),
            ..PackageMeta::default()
        },
        PackageMeta {
            group: "halloween".into(),
            name: "tricks@eggs".into(),
            ..PackageMeta::default()
        },
        PackageMeta {
            group: "halloween".into(),
            name: "tricks/paper".into(),
            ..PackageMeta::default()
        },
        PackageMeta {
            group: "".into(),
            name: "thevoid".into(),
            ..PackageMeta::default()
        },
    ];
    for pm in deny {
        let res = pm.naming_check();
        assert!(res.is_err(), "Denied an valid package meta config {pm:?}");
    }
}
