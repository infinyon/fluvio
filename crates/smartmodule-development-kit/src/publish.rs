use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Parser;

use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadata;
use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::{DEF_HUB_INIT_DIR, DEF_HUB_PKG_META, HubAccess, PackageMeta, PkgVisibility};
use tracing::debug;

pub const SMARTMODULE_TOML: &str = "SmartModule.toml";

/// Publish SmartModule to SmartModule Hub
#[derive(Debug, Parser)]
pub struct PublishCmd {
    pub package_meta: Option<String>,

    /// don't ask for confirmation of public package publish
    #[clap(long, default_value = "false")]
    pub public_yes: bool,

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

        let hubdir = Path::new(DEF_HUB_INIT_DIR);
        if !hubdir.exists() {
            init_package_template()?;
        } else if !self.public_yes {
            check_package_meta_visiblity()?;
        }

        match (self.pack, self.push) {
            (false, false) | (true, true) => {
                let pkgmetapath = self
                    .package_meta
                    .clone()
                    .unwrap_or_else(|| hubutil::DEF_HUB_PKG_META.to_string());
                let pkgdata = package_assemble(&pkgmetapath, &access)?;
                package_push(self, &pkgdata, &access)?;
            }

            // --pack only
            (true, false) => {
                let pkgmetapath = self
                    .package_meta
                    .clone()
                    .unwrap_or_else(|| hubutil::DEF_HUB_PKG_META.to_string());
                package_assemble(&pkgmetapath, &access)?;
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

pub fn package_assemble(pkgmeta: &str, access: &HubAccess) -> Result<String> {
    let pkgname = hubutil::package_assemble_and_sign(pkgmeta, access, None)?;
    println!("Package {} created", pkgname);
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
        eprintln!("{}", e);
        std::process::exit(1);
    }
    Ok(())
}

pub fn init_package_template() -> Result<()> {
    // fill out template w/ defaults
    let pmetapath = hubutil::DEF_HUB_PKG_META;

    let mut pm = PackageMeta {
        group: "no-hubid".into(),
        name: "not-found".into(),
        version: "0.0.0".into(),
        manifest: Vec::new(),
        ..PackageMeta::default()
    };
    let sm_toml_file = find_smartmodule_toml()?;
    pm.update_from_smartmodule_toml(&sm_toml_file.to_string_lossy())?;

    println!("Creating package {}", pm.pkg_name());
    pm.naming_check()?;

    let wasmout = hubutil::packagename_transform(&pm.name)? + ".wasm";
    let wasmpath = format!("target/wasm32-unknown-unknown/release-lto/{wasmout}");
    pm.manifest.push(wasmpath);

    // create directoy name pkgname
    let pkgdir = Path::new(hubutil::DEF_HUB_INIT_DIR);
    if pkgdir.exists() {
        return Err(anyhow::anyhow!("package hub directory exists already"));
    }
    std::fs::create_dir(pkgdir)?;
    pm.write(pmetapath)?;

    println!(".. fill out info in {pmetapath}");
    Ok(())
}

fn check_package_meta_visiblity() -> Result<()> {
    let sm_toml_file = find_smartmodule_toml()?;
    let spkg = SmartModuleMetadata::from_toml(sm_toml_file)?;
    let spkg_vis = PkgVisibility::from(&spkg.package.visibility);
    let mut pm = PackageMeta::read_from_file(DEF_HUB_PKG_META)?;
    if spkg_vis == PkgVisibility::Public && spkg_vis != pm.visibility {
        println!("Package visibility changing from private to public!");
        verify_public_or_exit()?;
        // writeout package metadata visibility change
        pm.visibility = PkgVisibility::Public;
        pm.write(DEF_HUB_PKG_META)?;
    }
    Ok(())
}

pub(crate) fn find_smartmodule_toml() -> Result<PathBuf> {
    let smartmodule_toml = Path::new(SMARTMODULE_TOML);

    if smartmodule_toml.exists() {
        return Ok(smartmodule_toml.to_path_buf());
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

#[ignore]
#[test]
fn build_sm_toml() {
    use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadata;

    let fpath = "test_Smart.toml";
    let smart_toml = SmartModuleMetadata::default();
    let smart_toml_str = toml::to_string(&smart_toml);
    assert!(smart_toml_str.is_ok());
    std::fs::write(fpath, &smart_toml_str.unwrap()).expect("couldn't write testfile");
}

// the smartmodule has template patterns that don't parse unless we apply
// the template...
#[ignore]
#[test]
fn reference_sm_toml() {
    use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadata;

    let fpath = format!("../../smartmodule/cargo_template/{}", SMARTMODULE_TOML);
    let smart_toml = SmartModuleMetadata::from_toml(fpath);
    assert!(
        smart_toml.is_ok(),
        "cargo Smart.toml template incompatible {smart_toml:?}"
    );
    let _smart_toml = smart_toml.unwrap();
}
