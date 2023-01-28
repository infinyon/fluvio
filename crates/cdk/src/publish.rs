//!
//! Command for hub publishing

use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Parser;

use fluvio_connector_package::metadata::ConnectorMetadata;
use fluvio_connector_package::metadata::ConnectorVisibility;
use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::{
    DEF_HUB_INIT_DIR, DEF_HUB_PKG_META, HubAccess, PackageMeta, PkgVisibility, PackageMetaExt,
};
use hubutil::packagename_validate;
use tracing::{debug, info};

pub const CONNECTOR_TOML: &str = "Connector.toml";

/// Publish Connector package to the Hub
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
    if let Err(e) = run_block_on(hubutil::push_package_conn(pkgpath, access)) {
        eprintln!("{e}");
        std::process::exit(1);
    }
    Ok(())
}

// todo: review for Connectors
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
    let pkg_toml_file = find_connector_toml()?;
    pm.update_from_connector_toml(&pkg_toml_file.to_string_lossy())?;

    println!("Creating package {}", pm.pkg_name());
    pm.naming_check()?;

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
    let cmeta_toml_file = find_connector_toml()?;
    let mpkg = ConnectorMetadata::from_toml_file(cmeta_toml_file)?;
    let mpkg_vis = from_connectorvis(&mpkg.package.visibility);
    let mut pm = PackageMeta::read_from_file(DEF_HUB_PKG_META)?;
    if mpkg_vis == PkgVisibility::Public && mpkg_vis != pm.visibility {
        println!("Package visibility changing from private to public!");
        verify_public_or_exit()?;
        // writeout package metadata visibility change
        pm.visibility = PkgVisibility::Public;
        pm.write(DEF_HUB_PKG_META)?;
    }
    Ok(())
}

fn from_connectorvis(cv: &ConnectorVisibility) -> PkgVisibility {
    match cv {
        ConnectorVisibility::Public => PkgVisibility::Public,
        ConnectorVisibility::Private => PkgVisibility::Private,
    }
}

pub(crate) fn find_connector_toml() -> Result<PathBuf> {
    let smartmodule_toml = Path::new(CONNECTOR_TOML);

    if smartmodule_toml.exists() {
        return Ok(smartmodule_toml.to_path_buf());
    }

    Err(anyhow::anyhow!("No \"{}\" file found", CONNECTOR_TOML))
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

trait PackageMetaConnectorExt {
    /// Pull package-meta info from smartmodule meta toml
    fn update_from_connector_toml(&mut self, fpath: &str) -> Result<()>;
}

impl PackageMetaConnectorExt for PackageMeta {
    /// Pull package-meta info from smartmodule meta toml
    fn update_from_connector_toml(&mut self, fpath: &str) -> Result<()> {
        info!(fpath, "opening smartmodule toml");
        let cpkg = ConnectorMetadata::from_toml_file(fpath)?;
        let cpk = &cpkg.package;

        packagename_validate(&cpk.name)?;

        self.name = cpk.name.clone();
        self.group = cpk.group.clone();
        self.version = cpk.version.to_string();
        self.description = cpk.description.clone().unwrap_or_default();
        self.visibility = from_connectorvis(&cpk.visibility);

        // needed for fluvio sm download
        self.manifest.push(fpath.into());
        Ok(())
    }
}
