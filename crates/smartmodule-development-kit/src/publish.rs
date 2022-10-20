use std::{path::Path};

use anyhow::Result;
use clap::Parser;

use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::{DEF_HUB_INIT_DIR, HubAccess, PackageMeta};

/// Publish SmartModule to SmartModule Hub
#[derive(Debug, Parser)]
pub struct PublishOpt {
    pub package_meta: Option<String>,

    /// do only the pack portion
    #[clap(long)]
    pack: bool,

    /// given a packed file do only the push
    #[clap(long, hide_short_help = true)]
    push: bool,

    #[clap(long, hide_short_help = true)]
    remote: Option<String>,
}
impl PublishOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let access = HubAccess::default_load(&self.remote)?;

        let hubdir = Path::new(DEF_HUB_INIT_DIR);
        if !hubdir.exists() {
            init_package_template()?;
        }

        match (self.pack, self.push) {
            (false, false) | (true, true) => {
                let pkgmetapath = self
                    .package_meta
                    .clone()
                    .unwrap_or_else(|| hubutil::DEF_HUB_PKG_META.to_string());
                let pkgdata = package_assemble(&pkgmetapath, &access)?;
                package_push(&pkgdata, &access)?;
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
                package_push(pkgfile, &access)?;
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

pub fn package_push(pkgpath: &str, access: &HubAccess) -> Result<()> {
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
    pm.update_from_smartmodule_toml(&sm_toml_file)?;

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
    pm.write(&pmetapath)?;

    println!(".. fill out info in {pmetapath}");
    Ok(())
}

fn find_smartmodule_toml() -> Result<String> {
    let prjdir = if let Ok(d) = std::fs::read_dir("./") {
        d
    } else {
        return Err(anyhow::anyhow!("couldn't read in directory"));
    };
    let cargo_toml = Path::new("./Cargo.toml");
    for ent in prjdir {
        let ent = ent?;
        let fp = ent.path();
        if fp == cargo_toml {
            continue;
        }
        if let Some(ext) = fp.extension() {
            if ext != "toml" {
                continue;
            }
            let fpstr = fp.to_string_lossy().to_string();
            return Ok(fpstr);
        }
    }
    Err(anyhow::anyhow!("No smartmodule toml file found"))
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

#[test]
fn reference_sm_toml() {
    use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadata;

    let fpath = "../../smartmodule/cargo_template/Smart.toml";
    let smart_toml = SmartModuleMetadata::from_toml(fpath);
    assert!(
        smart_toml.is_ok(),
        "cargo Smart.toml template incompatible {smart_toml:?}"
    );
    let _smart_toml = smart_toml.unwrap();
}
