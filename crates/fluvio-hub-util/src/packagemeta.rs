use std::default::Default;
use std::io::Read;
use std::path::Path;
use std::fs;

use fluvio_controlplane_metadata::smartmodule::FluvioSemVersion;
use fluvio_controlplane_metadata::smartmodule::SmartModulePackageKey;
use fluvio_controlplane_metadata::smartmodule::SmartModuleVisibility;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, error};

use fluvio_controlplane_metadata::smartmodule as smpkg;

use crate::HUB_PACKAGE_META;
use crate::HUB_PACKAGE_VERSION;
use crate::HUB_PACKAGE_EXT;
use crate::HubUtilError;
use crate::package_get_topfile;

type Result<T> = std::result::Result<T, HubUtilError>;

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
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum PkgVisibility {
    #[default]
    Private,
    Public,
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
        }
    }
}

impl PackageMeta {
    /// Retrieves the fully qualified name for the `PackageMeta`.
    ///
    /// Eg: `infinyon-example-0.0.1`
    pub fn id(&self) -> Result<String> {
        let fluvio_semver = FluvioSemVersion::parse(&self.version)
            .map_err(|err| HubUtilError::SemVerError(err.to_string()))?;
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
            return Err(HubUtilError::InvalidPackageName(pkg_name.into()));
        }

        let name_version = parts.get(1).unwrap().split('@').collect::<Vec<&str>>();

        if name_version.len() != 2 {
            error!("The provided name is not a valid package name: {pkg_name}");
            return Err(HubUtilError::InvalidPackageName(pkg_name.into()));
        }

        Ok(format!(
            "{}/{}-{}.{HUB_PACKAGE_EXT}",
            parts[0], name_version[0], name_version[1]
        ))
    }

    /// read package-meta file (not a package.tar file, just the meta file)
    pub fn read_from_file(filename: &str) -> Result<Self> {
        let pm_raw: Vec<u8> = fs::read(filename)?;
        let pm_read: PackageMeta = serde_yaml::from_slice(&pm_raw)?;
        debug!(target: "package-meta", "read_from_file {}, {:?}", &filename, &pm_read);
        Ok(pm_read)
    }

    /// provide the manifest list with full paths to manifest files
    pub fn manifest_paths(&self, pkgpath_in: &str) -> Result<Vec<String>> {
        let base_dir = Path::new(pkgpath_in);
        let full_mf_iter = self.manifest.iter().map(|relname| {
            let pb = base_dir.join(relname);
            pb.to_string_lossy().to_string()
        });
        Ok(full_mf_iter.collect())
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

    pub fn write<P: AsRef<Path>>(&self, pmetapath: P) -> Result<()> {
        let serialized = serde_yaml::to_string(&self)?;
        fs::write(pmetapath, serialized.as_bytes())?;
        Ok(())
    }

    /// Pull pacakge-meta info from Cargo.toml,
    /// particularly package name and version
    pub fn update_from_cargo_toml<P: AsRef<Path>>(&mut self, fpath: P) -> Result<()> {
        let ctoml = cargo_toml::Manifest::from_path(fpath)?;
        let cpkg = ctoml
            .package
            .ok_or(HubUtilError::CargoMissingPackageSection)?;

        packagename_validate(&cpkg.name)?;
        self.name = cpkg.name;
        self.version = cpkg.version;

        Ok(())
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
            Err(HubUtilError::PackageVerify(advice))
        } else {
            Ok(())
        }
    }
}

pub fn packagename_validate(pkgname: &str) -> Result<()> {
    let mut advice = String::new();

    advice.push_str(&validate_allowedchars(pkgname, "package name"));

    if !advice.is_empty() {
        Err(HubUtilError::InvalidPackageName(advice))
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

/// certain output files are transformed in name vs their package name
/// eg. a cargo package named example-smartmodule generates
/// a release file of example_smartmodule.wasm
pub fn packagename_transform(pkgname: &str) -> Result<String> {
    packagename_validate(pkgname)?;

    let tfm_pkgname = pkgname.replace('-', "_");

    Ok(tfm_pkgname)
}

/// given a package.tar file get the package-meta data
pub fn package_get_meta(pkgfile: &str) -> Result<PackageMeta> {
    let buf = package_get_topfile(pkgfile, HUB_PACKAGE_META)?;
    let strbuf = std::str::from_utf8(&buf)
        .map_err(|_| HubUtilError::UnableGetPackageMeta(pkgfile.into()))?;
    let pm: PackageMeta = serde_yaml::from_str(strbuf)?;
    Ok(pm)
}

/// Creates an instance of `PackageMeta` from bytes representing a TAR
/// package.
pub fn package_meta_from_bytes(reader: &[u8]) -> Result<PackageMeta> {
    let mut tarfile = tar::Archive::new(reader);
    let pkg_meta = Path::new(HUB_PACKAGE_META);
    let entries = tarfile.entries()?;
    for file in entries {
        if file.is_err() {
            continue;
        }
        let mut f = file?;
        if let Ok(fp) = f.path() {
            if fp == pkg_meta {
                let mut buf = String::new();
                f.read_to_string(&mut buf)?;
                let pm: PackageMeta = serde_yaml::from_str(&buf)?;
                return Ok(pm);
            }
        }
    }

    Err(HubUtilError::UnableGetPackageMeta(
        "Provided bytes doesn't belong to an actual TAR file".into(),
    ))
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
        write!(f, "{}", lbl)
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
fn hub_package_meta_t_read() {
    let testfile = "tests/apackage/".to_owned() + HUB_PACKAGE_META;
    let pm = PackageMeta {
        group: "infinyon".into(),
        name: "example".into(),
        version: "0.0.1".into(),
        manifest: ["tests/apackage/module.wasm".into()].to_vec(),
        ..PackageMeta::default()
    };

    let pm_read = PackageMeta::read_from_file(&testfile).expect("error reading package file");

    assert_eq!(pm, pm_read);
}

#[test]
fn hub_package_meta_t_write_then_read() {
    let testfile: &str = "tests/hub_package_meta_rw_test.yaml";
    let pm = PackageMeta {
        group: "infinyon".into(),
        name: "example".into(),
        version: "0.0.1".into(),
        manifest: ["module.wasm".into()].to_vec(),
        ..PackageMeta::default()
    };

    pm.write(testfile).expect("error writing package file");
    let pm_read = PackageMeta::read_from_file(testfile).expect("error reading package file");
    assert_eq!(pm, pm_read);
}

#[test]
fn hub_package_meta_t_manifest_paths() {
    let base_paths: Vec<&str> = vec!["target/output/module.wasm", "doc.md", "inputs.toml"];
    let in_paths = base_paths.iter().map(|p| p.to_string()).collect();
    let pm = PackageMeta {
        group: "infinyon".into(),
        name: "example".into(),
        version: "0.0.1".into(),
        manifest: in_paths,
        ..PackageMeta::default()
    };

    let expected: Vec<String> = base_paths.iter().map(|p| p.to_string()).collect();
    let actual = pm.manifest_paths("").expect("unexpected err");
    assert_eq!(expected, actual);

    let expected: Vec<String> = base_paths
        .iter()
        .map(|p| String::from("tmp") + "/" + p)
        .collect();
    let actual = pm.manifest_paths("tmp").expect("unexpected err");
    assert_eq!(expected, actual);
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

#[cfg(test)]
mod t_packagemeta_version {

    use crate::HubUtilError;
    use crate::PackageMeta;
    use crate::PkgVisibility;

    fn read_pkgmeta(fname: &str) -> Result<PackageMeta, HubUtilError> {
        let pm = PackageMeta::read_from_file(fname)?;
        Ok(pm)
    }

    /// the current code should be able to load all old versions
    #[test]
    fn backward_compat() {
        let flist = vec![
            "tests/apackage/package-meta.yaml",
            "tests/apackage/package-meta-v0.1.yaml",
            "tests/apackage/package-meta-v0.2-owner.yaml",
            "tests/apackage/package-meta-v0.2-public.yaml",
        ];

        for ver in flist {
            let msg = format!("Failed to read {ver}");
            let _pm = read_pkgmeta(ver).expect(&msg);
        }
    }

    #[test]
    fn visibility_invalid() {
        let visbad = "tests/apackage/package-meta-v0.2-visbad.yaml";
        let res = read_pkgmeta(visbad);
        println!("{:?}", &res);
        assert!(res.is_err());
    }

    #[test]
    fn visibility_defaults_owner_v0_1() {
        // if package meta is missing any visibility field, like older versions will be missing
        // check that they default to private (Owner) visiblity
        let visbad = "tests/apackage/package-meta-v0.1.yaml";
        let res = read_pkgmeta(visbad);
        println!("{:?}", &res);
        assert!(res.is_ok());
        if let Ok(pm) = res {
            assert_eq!(pm.visibility, PkgVisibility::Private);
        }
    }
}
