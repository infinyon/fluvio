use serde::{Deserialize, Serialize};
use std::default::Default;
use std::io::Read;
use std::path::Path;
use std::fs;
use tracing::{debug, info};

use fluvio_controlplane_metadata::smartmodule as smpkg;

use crate::HUB_PACKAGE_META;
use crate::HUB_PACKAGE_VERSION;
use crate::HUB_PACKAGE_EXT;
use crate::HubUtilError;

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
    pub manifest: Vec<String>, // Files in package, package-meta is implied, signature is omitted
                               // repository: optional url
                               // repository-commit: optional hash
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
            manifest: Vec::new(),
        }
    }
}

impl PackageMeta {
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

    pub fn write<P: AsRef<Path>>(&self, pmetapath: P) -> Result<()> {
        let serialized = serde_yaml::to_string(&self)?;
        fs::write(pmetapath, &serialized.as_bytes())?;
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
        self.description = spk.description.clone().unwrap_or_default();
        Ok(())
    }
}

pub fn packagename_validate(pkgname: &str) -> Result<()> {
    let good_chars = pkgname
        .chars()
        .all(|ch| matches!(ch, 'A'..='Z' | 'a'..='z' | '-' | '_'));
    if !good_chars {
        return Err(HubUtilError::InvalidPackageName(pkgname.to_string()));
    }
    Ok(())
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
    let in_pkg = std::fs::File::open(pkgfile)?;
    let pkg_meta = Path::new(HUB_PACKAGE_META);
    let mut ar = tar::Archive::new(in_pkg);
    let entries = ar.entries()?;
    for file in entries {
        if file.is_err() {
            continue;
        }
        let mut f = file?;
        if let Ok(fp) = f.path() {
            dbg!(&fp);
            if fp == pkg_meta {
                let mut buf = String::new();
                f.read_to_string(&mut buf)?;
                let pm: PackageMeta = serde_yaml::from_str(&buf)?;
                return Ok(pm);
            }
        }
    }

    Err(HubUtilError::UnableGetPackageMeta(pkgfile.to_string()))
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

    pm.write(&testfile).expect("error writing package file");
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
