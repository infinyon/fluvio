//!
//! Command for hub publishing

use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use cargo_builder::package::PackageInfo;
use clap::Parser;

use fluvio_connector_package::metadata::ConnectorMetadata;
use fluvio_connector_package::metadata::ConnectorVisibility;
use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::{DEF_HUB_INIT_DIR, HubAccess, PackageMeta, PkgVisibility, PackageMetaExt};
use hubutil::packagename_validate;
use tracing::{debug, info};

use crate::cmd::PackageCmd;

pub const CONNECTOR_TOML: &str = "Connector.toml";

/// Publish Connector package to the Hub
#[derive(Debug, Parser)]
pub struct PublishCmd {
    #[clap(flatten)]
    package: PackageCmd,

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

    /// provide target platform for the package. Optional. By default the host's one is used.
    #[clap(
        long,
        default_value_t = current_platform::CURRENT_PLATFORM.to_string()
    )]
    target: String,

    #[clap(long, hide_short_help = true)]
    remote: Option<String>,
}

impl PublishCmd {
    pub(crate) fn process(&self) -> Result<()> {
        let access = HubAccess::default_load(&self.remote)?;

        let opt = self.package.as_opt();
        let package_info = PackageInfo::from_options(&opt)?;

        info!(
            "publishing package {} from {}",
            package_info.package_name(),
            package_info.package_path().to_string_lossy()
        );

        let hubdir = package_info.package_relative_path(DEF_HUB_INIT_DIR);
        if !hubdir.exists() {
            init_package_template(&package_info)?;
        } else if !self.public_yes {
            check_package_meta_visiblity(&package_info)?;
        }

        match (self.pack, self.push) {
            (false, false) | (true, true) => {
                let pkgmetapath = self
                    .package_meta
                    .as_ref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| hubdir.join(hubutil::HUB_PACKAGE_META));
                let pkgdata = package_assemble(pkgmetapath, &self.target, &access)?;
                package_push(self, &pkgdata, &access)?;
            }

            // --pack only
            (true, false) => {
                let pkgmetapath = self
                    .package_meta
                    .as_ref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| hubdir.join(hubutil::HUB_PACKAGE_META));
                package_assemble(pkgmetapath, &self.target, &access)?;
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

pub fn package_assemble<P: AsRef<Path>>(
    pkgmeta: P,
    target: &str,
    access: &HubAccess,
) -> Result<String> {
    let pkgname = hubutil::package_assemble_and_sign(
        &pkgmeta,
        access,
        pkgmeta
            .as_ref()
            .parent()
            .ok_or_else(|| anyhow::anyhow!("invalid package meta path"))?,
        Some(target),
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
    if let Err(e) = run_block_on(hubutil::push_package_conn(pkgpath, access, &opts.target)) {
        eprintln!("{e}");
        std::process::exit(1);
    }
    Ok(())
}

// todo: review for Connectors
pub fn init_package_template(package_info: &PackageInfo) -> Result<()> {
    // fill out template w/ defaults

    let mut pm = PackageMeta {
        group: "no-hubid".into(),
        name: "not-found".into(),
        version: "0.0.0".into(),
        manifest: Vec::new(),
        ..PackageMeta::default()
    };
    let pkg_toml_file = find_connector_toml(package_info)?;
    pm.update_from_connector_toml(&pkg_toml_file.to_string_lossy())?;

    println!("Creating package {}", pm.pkg_name());
    pm.naming_check()?;

    // create directoy name pkgname
    let pkgdir = package_info.package_relative_path(hubutil::DEF_HUB_INIT_DIR);
    if pkgdir.exists() {
        return Err(anyhow::anyhow!("package hub directory exists already"));
    }
    std::fs::create_dir(&pkgdir)?;
    let pmetapath = pkgdir.join(hubutil::HUB_PACKAGE_META);
    pm.write(&pmetapath)?;

    println!(".. fill out info in {}", pmetapath.to_string_lossy());
    Ok(())
}

fn check_package_meta_visiblity(package_info: &PackageInfo) -> Result<()> {
    let cmeta_toml_file = find_connector_toml(package_info)?;
    let mpkg = ConnectorMetadata::from_toml_file(cmeta_toml_file)?;
    let mpkg_vis = from_connectorvis(&mpkg.package.visibility);
    let package_meta_path = package_info.package_relative_path(hubutil::DEF_HUB_PKG_META);
    let mut pm = PackageMeta::read_from_file(&package_meta_path).context(format!(
        "unable to read package meta file from {}",
        package_meta_path.to_string_lossy()
    ))?;
    if mpkg_vis == PkgVisibility::Public && mpkg_vis != pm.visibility {
        println!("Package visibility changing from private to public!");
        verify_public_or_exit()?;
        // writeout package metadata visibility change
        pm.visibility = PkgVisibility::Public;
        pm.write(package_meta_path)?;
    }
    Ok(())
}

fn from_connectorvis(cv: &ConnectorVisibility) -> PkgVisibility {
    match cv {
        ConnectorVisibility::Public => PkgVisibility::Public,
        ConnectorVisibility::Private => PkgVisibility::Private,
    }
}

pub(crate) fn find_connector_toml(package_info: &PackageInfo) -> Result<PathBuf> {
    let smartmodule_toml = package_info.package_relative_path(CONNECTOR_TOML);

    if smartmodule_toml.exists() {
        return Ok(smartmodule_toml);
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
