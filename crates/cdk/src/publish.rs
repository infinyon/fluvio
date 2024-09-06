//!
//! Command for hub publishing

use std::env::current_dir;
use std::fs::remove_dir_all;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use cargo_builder::package::PackageInfo;
use cargo_builder::package::PackageOption;
use clap::Parser;

use fluvio_connector_package::metadata::ConnectorMetadata;
use fluvio_connector_package::metadata::ConnectorVisibility;
use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::package_meta_relative_path;
use hubutil::{DEF_HUB_INIT_DIR, HubAccess, PackageMeta, PkgVisibility, PackageMetaExt};
use hubutil::packagename_validate;
use tracing::{debug, info};

use crate::cmd::PackageCmd;
use crate::utils::build::{BuildOpts, build_connector};

pub const CONNECTOR_TOML: &str = "Connector.toml";

/// Publish Connector package to the Hub
#[derive(Debug, Parser)]
pub struct PublishCmd {
    #[clap(flatten)]
    package: PackageCmd,

    pub package_meta: Option<String>,

    /// path to the ipkg file, used when --push is specified
    #[arg(long)]
    ipkg: Option<String>,

    /// don't ask for confirmation of public package publish
    #[arg(long, default_value = "false")]
    pub public_yes: bool,

    #[arg(long, default_value = "false")]
    pub no_build: bool,

    /// publish a build for running in the Infinyon Cloud environment
    ///
    #[arg(long, default_value_t = false)]
    cloud: bool,

    /// do only the pack portion
    #[arg(long, hide_short_help = true)]
    pack: bool,

    /// given a packed file do only the push
    #[arg(long, hide_short_help = true)]
    push: bool,

    #[arg(long, hide_short_help = true)]
    remote: Option<String>,

    /// Relative path to this connector package README
    #[clap(long, default_value = "./README.md")]
    readme: PathBuf,
}

impl PublishCmd {
    pub(crate) fn process(&self) -> Result<()> {
        let access = HubAccess::default_load(&self.remote)?;

        match (self.pack, self.push) {
            (false, false) | (true, true) => {
                let opt = self.package.as_opt();
                let hubdir = self.run_in_cargo_project(&opt)?;
                let pkgmetapath = self.package_meta_path(&hubdir);
                let pkgdata = package_assemble(pkgmetapath, &opt.target, &access)?;
                package_push(self, &pkgdata, &access)?;
                Self::cleanup(&hubdir)?;
            }

            // --pack only
            (true, false) => {
                let opt = self.package.as_opt();
                let hubdir = self.run_in_cargo_project(&opt)?;
                let pkgmetapath = self.package_meta_path(&hubdir);
                package_assemble(pkgmetapath, &opt.target, &access)?;
            }

            // --push only, needs ipkg file or expects to be run in project folder
            (false, true) => {
                let ipkg_path = match self.ipkg.as_ref() {
                    Some(ipkg_path) => ipkg_path.into(),
                    None => self.default_ipkg_file_path()?,
                };

                package_push(self, &ipkg_path, &access)?;
            }
        }

        Ok(())
    }

    fn package_meta_path(&self, hubdir: &Path) -> PathBuf {
        self.package_meta
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| hubdir.join(hubutil::HUB_PACKAGE_META))
    }

    /// This gets run only if the command should be run in the cargo project folder
    /// of the connector
    ///
    /// returns hubdir
    fn run_in_cargo_project(&self, opt: &PackageOption) -> Result<PathBuf> {
        let package_info = PackageInfo::from_options(opt)?;
        let hubdir = package_info.package_relative_path(DEF_HUB_INIT_DIR);

        info!(
            "publishing package {} from {}",
            package_info.package_name(),
            package_info.package_path().to_string_lossy()
        );

        Self::cleanup(&hubdir)?;

        if !self.no_build {
            build_connector(&package_info, BuildOpts::with_release(opt.release.as_str()))?;
        }

        init_package_template(&package_info, &self.readme)?;
        check_package_meta_visiblity(&package_info)?;

        Ok(hubdir)
    }

    fn cleanup(hubdir: &Path) -> Result<()> {
        if hubdir.exists() {
            // Delete the `.hub` directory if already exists
            tracing::warn!("Removing directory at {:?}", hubdir);
            remove_dir_all(hubdir)?;
        }

        Ok(())
    }

    fn default_ipkg_file_path(&self) -> Result<String> {
        let opt = self.package.as_opt();
        let package_info = PackageInfo::from_options(&opt)
            .context("Failed to read package info. Should either specify --ipkg or run in the connector project folder")?;
        let hubdir = package_info.package_relative_path(DEF_HUB_INIT_DIR);

        let package_meta_path = self.package_meta_path(&hubdir);
        let package_meta = PackageMeta::read_from_file(package_meta_path)?;

        let tar_name = package_meta.packagefile_name_unsigned();
        let ipkg_name = Path::new(&tar_name)
            .with_extension("ipkg")
            .display()
            .to_string();

        let ipkg_path = hubdir
            .join(ipkg_name)
            .to_str()
            .context("Invalid ipkg path generated")?
            .to_owned();

        Ok(ipkg_path)
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
    if let Err(e) = run_block_on(hubutil::push_package_conn(
        pkgpath,
        access,
        &opts.package.target,
    )) {
        eprintln!("{e}");
        std::process::exit(1);
    }
    Ok(())
}

/// Creates a full path from the provided relative path in the context
/// of this command execution
fn make_full_path<P: AsRef<Path>>(path: P) -> Result<PathBuf> {
    let mut full_path = current_dir()?;

    full_path.push(path);
    Ok(full_path)
}

pub fn init_package_template(package_info: &PackageInfo, readme_path: &PathBuf) -> Result<()> {
    let connector_toml_path = find_connector_toml(package_info)?;
    let connector_metadata = ConnectorMetadata::from_toml_file(&connector_toml_path)?;
    let mut pm = PackageMeta {
        group: "no-hubid".into(),
        name: "not-found".into(),
        version: "0.0.0".into(),
        manifest: Vec::new(),
        ..PackageMeta::default()
    };
    let readme_path = make_full_path(readme_path)?;
    let package_hub_path = package_info.package_relative_path(hubutil::DEF_HUB_INIT_DIR);
    if package_hub_path.exists() {
        return Err(anyhow::anyhow!("package hub directory exists already"));
    }
    std::fs::create_dir(&package_hub_path)?;
    let package_meta_path = package_hub_path.join(hubutil::HUB_PACKAGE_META);

    pm.update_from(&connector_metadata)?;

    let connector_toml_relative_path =
        package_meta_relative_path(&package_meta_path, &connector_toml_path);
    pm.manifest.push(
        connector_toml_relative_path
            .unwrap_or_else(|| connector_toml_path.to_string_lossy().to_string()), // if failed to get relative path, use absolute
    );

    let binary_path = package_info.target_bin_path()?;
    let binary_relative_path = package_meta_relative_path(&package_meta_path, &binary_path);
    pm.manifest.push(
        binary_relative_path.unwrap_or_else(|| binary_path.to_string_lossy().to_string()), // if failed to get relative path, use absolute
    );

    let readme_md_relative_path = package_meta_relative_path(&package_meta_path, &readme_path);
    pm.manifest.push(
        readme_md_relative_path.unwrap_or_else(|| readme_path.to_string_lossy().to_string()), // if failed to get relative path, use absolute)
    );

    println!("Creating package {}", pm.pkg_name());
    pm.naming_check()?;

    pm.write(&package_meta_path)?;

    println!(
        ".. fill out info in {}",
        package_meta_path.to_string_lossy()
    );
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
    let connector_toml = package_info.package_relative_path(CONNECTOR_TOML);

    if connector_toml.exists() {
        return Ok(connector_toml);
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
    fn update_from(&mut self, connector_metadata: &ConnectorMetadata) -> Result<()>;
}

impl PackageMetaConnectorExt for PackageMeta {
    /// Pull package-meta info from smartmodule meta toml
    fn update_from(&mut self, connector_metadata: &ConnectorMetadata) -> Result<()> {
        let package = &connector_metadata.package;
        packagename_validate(&package.name)?;

        self.name.clone_from(&package.name);
        self.group.clone_from(&package.group);
        self.version = package.version.to_string();
        self.description = package.description.clone().unwrap_or_default();
        self.visibility = from_connectorvis(&package.visibility);

        Ok(())
    }
}
