use std::fs;
use std::io::Read;
use std::path::Path;

use chrono::{DateTime, Utc};
use tracing::debug;

use fluvio_hub_protocol::{HubError, PackageMeta, PkgTag};
use fluvio_hub_protocol::constants::{HUB_PACKAGE_META, PKG_TAG_META_PUBLISHED_AT};
use fluvio_hub_protocol::validate_allowedchars;

use crate::package_get_topfile;

type Result<T> = std::result::Result<T, HubError>;

pub trait PackageMetaExt {
    fn read_from_file<P: AsRef<Path>>(filename: P) -> Result<PackageMeta>;
    fn manifest_paths<P: AsRef<Path>>(&self, pkgpath_in: P) -> Result<Vec<String>>;
    fn write<P: AsRef<Path>>(&self, pmetapath: P) -> Result<()>;
    fn update_from_cargo_toml<P: AsRef<Path>>(&mut self, fpath: P) -> Result<()>;
    fn published_at(&self) -> Result<DateTime<Utc>>;
}

impl PackageMetaExt for PackageMeta {
    /// read package-meta file (not a package.tar file, just the meta file)
    fn read_from_file<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let pm_raw: Vec<u8> = fs::read(filename.as_ref())?;
        let pm_read: PackageMeta = serde_yaml::from_slice(&pm_raw)?;
        debug!(target: "package-meta", "read_from_file {}, {:?}", filename.as_ref().to_string_lossy(), &pm_read);
        Ok(pm_read)
    }

    /// provide the manifest list with full paths to manifest files
    fn manifest_paths<P: AsRef<Path>>(&self, pkgpath_in: P) -> Result<Vec<String>> {
        let base_dir = pkgpath_in.as_ref();
        let full_mf_iter = self.manifest.iter().map(|relname| {
            let pb = base_dir.join(relname);
            pb.to_string_lossy().to_string()
        });
        Ok(full_mf_iter.collect())
    }

    fn write<P: AsRef<Path>>(&self, pmetapath: P) -> Result<()> {
        let serialized = serde_yaml::to_string(&self)?;
        fs::write(pmetapath, serialized.as_bytes())?;
        Ok(())
    }

    /// Pull package-meta info from Cargo.toml,
    /// particularly package name and version
    fn update_from_cargo_toml<P: AsRef<Path>>(&mut self, fpath: P) -> Result<()> {
        let ctoml = cargo_toml::Manifest::from_path(fpath)?;
        let cpkg = ctoml.package.ok_or(HubError::CargoMissingPackageSection)?;

        packagename_validate(&cpkg.name)?;
        self.name = cpkg.name;
        self.version = cpkg.version.get()?.to_string();

        Ok(())
    }

    fn published_at(&self) -> Result<DateTime<Utc>> {
        let published_at_pkg_tag = self
            .tags
            .as_ref()
            .ok_or(HubError::General(String::from("No tags available")))?
            .iter()
            .find(|PkgTag { tag, .. }| tag == PKG_TAG_META_PUBLISHED_AT)
            .ok_or(HubError::General(format!(
                "Missing {PKG_TAG_META_PUBLISHED_AT} tag"
            )))?;

        let publisted_at = DateTime::parse_from_rfc2822(published_at_pkg_tag.value.as_str())
            .map_err(|err| HubError::General(format!("Failed to parse publish date. {}", err)))?;

        Ok(publisted_at.to_utc())
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

/// certain output files are transformed in name vs their package name
/// eg. a cargo package named example-smartmodule generates
/// a release file of example_smartmodule.wasm
pub fn packagename_transform(pkgname: &str) -> Result<String> {
    packagename_validate(pkgname)?;

    let tfm_pkgname = pkgname.replace('-', "_");

    Ok(tfm_pkgname)
}

/// given a package.tar file get the package-meta data
pub fn package_get_meta<P: AsRef<Path>>(pkgfile: P) -> Result<PackageMeta> {
    let buf = package_get_topfile(pkgfile.as_ref(), HUB_PACKAGE_META)?;
    let strbuf = std::str::from_utf8(&buf).map_err(|_| {
        HubError::UnableGetPackageMeta(pkgfile.as_ref().to_string_lossy().to_string())
    })?;
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

    Err(HubError::UnableGetPackageMeta(
        "Provided bytes doesn't belong to an actual TAR file".into(),
    ))
}

pub fn package_meta_relative_path<P: AsRef<Path>, T: AsRef<Path>>(
    package_meta_file_path: P,
    path: T,
) -> Option<String> {
    let base_dir = package_meta_file_path.as_ref().parent()?;
    let diff = pathdiff::diff_paths(path, base_dir)?;
    Some(diff.to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use fluvio_hub_protocol::PkgTag;

    use super::*;

    #[test]
    fn test_path_relative_from_for_ancestors() {
        //given
        let base_dir = "/root/sub/file";
        let path = "/root/another.file";

        //when
        let relative =
            package_meta_relative_path(base_dir, path).expect("failed to get relative path");

        //then
        assert_eq!(relative, "../another.file")
    }

    #[test]
    fn test_path_relative_from_for_descentors() {
        //given
        let base_dir = "/root/sub/file";
        let path = "/root/sub/sub1/another.file";

        //when
        let relative =
            package_meta_relative_path(base_dir, path).expect("failed to get relative path");

        //then
        assert_eq!(relative, "sub1/another.file")
    }

    #[test]
    fn builds_obj_key_from_package_name() {
        let pkg_names = [
            "infinyon/example@0.0.1",
            "infinyon/example-sm@0.1.0",
            "infinyon/json-sql@0.0.2",
            "infinyon/test@0.1.0",
            "infinyon/hub-cli@0.1.0",
            "infinyon/test-cli@0.1.0",
            "infinyon/regex@0.0.1",
        ];
        let obj_paths = [
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
            manifest: ["module.wasm".into()].to_vec(),
            tags: Some(vec![fluvio_hub_protocol::PkgTag {
                tag: "arch".to_owned(),
                value: "aarch64-unknown-linux-gnu".to_owned(),
            }]),
            ..PackageMeta::default()
        };

        let pm_read = PackageMeta::read_from_file(testfile).expect("error reading package file");

        assert_eq!(pm, pm_read);
    }

    #[test]
    fn hub_package_write() {
        use fluvio_hub_protocol::PkgTag;

        let pm = PackageMeta {
            group: "infinyon".into(),
            name: "test-write".into(),
            version: "0.0.1".into(),
            manifest: ["tests/apackage/module.wasm".into()].to_vec(),
            ..PackageMeta::default()
        };
        pm.write("tests/apackage/package-yaml-write.yaml.tmp")
            .expect("write fail");

        let tags = vec![PkgTag {
            tag: "target".to_string(),
            value: "aarch64-unknown-linux-musl".to_string(),
        }];
        let pm = PackageMeta {
            group: "infinyon".into(),
            name: "test-write".into(),
            version: "0.0.1".into(),
            manifest: ["tests/apackage/module.wasm".into()].to_vec(),
            tags: Some(tags),
            ..PackageMeta::default()
        };
        pm.write("tests/apackage/package-yaml-write-tags.yaml.tmp")
            .expect("write fail");
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
        use fluvio_hub_protocol::{HubError, PackageMeta, PkgTag, PkgVisibility};

        use crate::PackageMetaExt;

        fn read_pkgmeta(fname: &str) -> Result<PackageMeta, HubError> {
            let pm = PackageMeta::read_from_file(fname)?;
            Ok(pm)
        }

        /// the current code should be able to load all old versions
        #[test]
        fn backward_compat() {
            let flist = [
                "tests/apackage/package-meta.yaml",
                "tests/apackage/package-meta-v0.1.yaml",
                "tests/apackage/package-meta-v0.2-owner.yaml",
                "tests/apackage/package-meta-v0.2-public.yaml",
                "tests/apackage/package-meta-v0.3-notags.yaml",
                "tests/apackage/package-meta-v0.3-notags2.yaml",
                "tests/apackage/package-meta-v0.3-targets.yaml",
            ];

            for ver in flist {
                let msg = format!("Failed to read {ver}");
                let _pm = read_pkgmeta(ver).expect(&msg);
            }
        }

        #[test]
        fn read_tags_targets() {
            const TAGGED_PM: &str = "tests/apackage/package-meta-v0.3-targets.yaml";
            let msg = format!("couldn't read {TAGGED_PM}");
            let pm = read_pkgmeta(TAGGED_PM).expect(&msg);
            let tags = [
                ("arch", "aarch64-apple-darwin"),
                ("arch", "aarch64-unknown-linux-musl"),
            ]
            .iter()
            .fold(Vec::new(), |mut h, (k, v)| {
                let tag = PkgTag::new(k, v);
                h.push(tag);
                h
            });
            assert_eq!(Some(tags), pm.tags);
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

    #[test]
    fn inf_meta_published_at_success() {
        let pm = PackageMeta {
            group: "infinyon".into(),
            name: "example".into(),
            version: "0.0.1".into(),
            manifest: ["module.wasm".into()].to_vec(),
            tags: Some(vec![PkgTag {
                tag: PKG_TAG_META_PUBLISHED_AT.to_string(),
                value: "Tue, 22 Nov 2022 21:24:11 GMT".to_string(),
            }]),
            ..Default::default()
        };
        let published_at = pm.published_at();

        assert!(published_at.is_ok());

        let published_at = published_at.unwrap();
        assert_eq!(published_at.to_string(), "2022-11-22 21:24:11 UTC");
    }

    #[test]
    fn inf_meta_published_at_missing_tag() {
        let pm = PackageMeta {
            group: "infinyon".into(),
            name: "example".into(),
            version: "0.0.1".into(),
            manifest: ["module.wasm".into()].to_vec(),
            tags: Some(vec![PkgTag {
                tag: String::from("foo"),
                value: String::from("bar"),
            }]),
            ..Default::default()
        };
        let published_at = pm.published_at();

        assert!(published_at.is_err());
        assert_eq!(
            published_at.err().unwrap().to_string(),
            format!("General Error: Missing inf::meta::published_at tag")
        );
    }
}
