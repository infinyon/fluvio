use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use cargo_builder::package::PackageInfo;
use clap::Parser;

use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadata;
use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::{
    DEF_HUB_INIT_DIR, DEF_HUB_PKG_META, HubAccess, PackageMeta, PkgVisibility, PackageMetaExt,
    package_meta_relative_path, packagename_validate,
};
use tracing::debug;

use crate::cmd::PackageCmd;

pub const SMARTMODULE_TOML: &str = "SmartModule.toml";

/// Publish SmartModule to SmartModule Hub
#[derive(Debug, Parser)]
pub struct PublishCmd {
    #[clap(flatten)]
    package: PackageCmd,

    package_meta: Option<String>,

    /// don't ask for confirmation of public package publish
    #[clap(long, default_value = "false")]
    public_yes: bool,

    /// do only the pack portion
    #[clap(long, hide_short_help = true)]
    pack: bool,

    /// given a packed file do only the push
    #[clap(long, hide_short_help = true)]
    push: bool,

    #[clap(long, hide_short_help = true)]
    remote: Option<String>,
}

impl PublishCmd {
    pub(crate) fn process(&self) -> Result<()> {
        let access = HubAccess::default_load(&self.remote)?;

        let opt = self.package.as_opt();
        let package_info = PackageInfo::from_options(&opt)?;

        let hubdir = package_info.package_relative_path(DEF_HUB_INIT_DIR);
        if !hubdir.exists() {
            init_package_template(&package_info)?;
        } else if !self.public_yes {
            check_package_meta_visiblity(&package_info)?;
        }

        let package_meta_path = self
            .package_meta
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| hubdir.join(hubutil::HUB_PACKAGE_META));

        match (self.pack, self.push) {
            (false, false) | (true, true) => {
                let pkgdata = package_assemble(&package_meta_path, &access)?;
                package_push(self, &pkgdata, &access)?;
            }

            // --pack only
            (true, false) => {
                package_assemble(&package_meta_path, &access)?;
            }

            // --push only, needs ipkg file
            (false, true) => {
                let pkgfile = &self
                    .package_meta
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("package file required for push"))?;
                package_push(self, pkgfile, &access)?;
            }
        }

        Ok(())
    }
}

pub fn package_assemble<P: AsRef<Path>>(pkgmeta: P, access: &HubAccess) -> Result<String> {
    let pkgname = hubutil::package_assemble_and_sign(
        &pkgmeta,
        access,
        pkgmeta
            .as_ref()
            .parent()
            .ok_or_else(|| anyhow::anyhow!("invalid package meta path"))?,
        None,
    )?;
    println!("Package {pkgname} created");
    Ok(pkgname)
}

pub fn package_push(opts: &PublishCmd, pkgpath: &str, access: &HubAccess) -> Result<()> {
    if !opts.public_yes {
        let pm = hubutil::package_get_meta(pkgpath)?;
        if pm.visibility == PkgVisibility::Public {
            verify_public_or_exit()?;
        }
    }
    if let Err(e) = run_block_on(hubutil::push_package(pkgpath, access)) {
        eprintln!("{e}");
        std::process::exit(1);
    }
    Ok(())
}

pub fn init_package_template(package_info: &PackageInfo) -> Result<()> {
    let sm_toml_path = find_smartmodule_toml(package_info)?;
    let sm_metadata = SmartModuleMetadata::from_toml(&sm_toml_path)?;

    // fill out template w/ defaults
    let mut pm = PackageMeta {
        group: "no-hubid".into(),
        name: "not-found".into(),
        version: "0.0.0".into(),
        manifest: Vec::new(),
        ..PackageMeta::default()
    };

    let package_hub_path = package_info.package_relative_path(hubutil::DEF_HUB_INIT_DIR);
    if package_hub_path.exists() {
        return Err(anyhow::anyhow!("package hub directory exists already"));
    }
    std::fs::create_dir(&package_hub_path)?;
    let package_meta_path = package_hub_path.join(hubutil::HUB_PACKAGE_META);

    pm.update_from(&sm_metadata)?;
    pm.naming_check()?;

    pm.manifest.push(
        package_meta_relative_path(&package_meta_path, &sm_toml_path).ok_or_else(|| {
            anyhow!(
                "unable to find package relative path for {}",
                sm_toml_path.to_string_lossy()
            )
        })?,
    );

    let wasmpath = package_info.target_wasm32_path()?;
    pm.manifest.push(
        package_meta_relative_path(&package_meta_path, &wasmpath).ok_or_else(|| {
            anyhow!(
                "unable to find package relative path for {}",
                wasmpath.to_string_lossy()
            )
        })?,
    );

    println!("Creating package {}", pm.pkg_name());
    pm.write(&package_meta_path)?;

    println!(
        ".. fill out info in {}",
        package_meta_path.to_string_lossy()
    );
    Ok(())
}

fn check_package_meta_visiblity(package_info: &PackageInfo) -> Result<()> {
    let sm_toml_file = find_smartmodule_toml(package_info)?;
    let spkg = SmartModuleMetadata::from_toml(sm_toml_file)?;
    let spkg_vis = PkgVisibility::from(&spkg.package.visibility);

    let package_meta_path = package_info.package_relative_path(DEF_HUB_PKG_META);
    let mut pm = PackageMeta::read_from_file(&package_meta_path)?;
    if spkg_vis == PkgVisibility::Public && spkg_vis != pm.visibility {
        println!("Package visibility changing from private to public!");
        verify_public_or_exit()?;
        // writeout package metadata visibility change
        pm.visibility = PkgVisibility::Public;
        pm.write(package_meta_path)?;
    }
    Ok(())
}

pub(crate) fn find_smartmodule_toml(package_info: &PackageInfo) -> Result<PathBuf> {
    let smartmodule_toml = package_info.package_relative_path(SMARTMODULE_TOML);

    if smartmodule_toml.exists() {
        return Ok(smartmodule_toml);
    }

    Err(anyhow::anyhow!("No \"{}\" file found", SMARTMODULE_TOML))
}

fn verify_public_or_exit() -> Result<()> {
    println!("Are you sure you want to publish this package as public? (y/N)");
    let mut ans = String::new();
    std::io::stdin().read_line(&mut ans)?;
    let ans = ans.trim_end().to_lowercase();
    debug!("ans: {ans}");
    match ans.as_str() {
        "y" | "yes" => {}
        _ => {
            println!("publish stopped");
            std::process::exit(1);
        }
    }
    Ok(())
}

trait PackageMetaSmartModuleExt {
    fn update_from(&mut self, sm_metadata: &SmartModuleMetadata) -> Result<()>;
}

impl PackageMetaSmartModuleExt for PackageMeta {
    fn update_from(&mut self, sm_metadata: &SmartModuleMetadata) -> Result<()> {
        let spk = &sm_metadata.package;

        packagename_validate(&spk.name)?;

        self.name = spk.name.clone();
        self.group = spk.group.clone();
        self.version = spk.version.to_string();
        self.description = spk.description.clone().unwrap_or_default();
        self.visibility = PkgVisibility::from(&spk.visibility);

        Ok(())
    }
}

#[ignore]
#[test]
fn build_sm_toml() {
    use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadata;

    let fpath = "test_Smart.toml";
    let smart_toml = SmartModuleMetadata::default();
    let smart_toml_str = toml::to_string(&smart_toml);
    assert!(smart_toml_str.is_ok());
    std::fs::write(fpath, smart_toml_str.unwrap()).expect("couldn't write testfile");
}

// the smartmodule has template patterns that don't parse unless we apply
// the template...
#[ignore]
#[test]
fn reference_sm_toml() {
    use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadata;

    let fpath = format!("../../smartmodule/cargo_template/{SMARTMODULE_TOML}");
    let smart_toml = SmartModuleMetadata::from_toml(fpath);
    assert!(
        smart_toml.is_ok(),
        "cargo Smart.toml template incompatible {smart_toml:?}"
    );
    let _smart_toml = smart_toml.unwrap();
}
