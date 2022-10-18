use std::ffi::OsString;
use std::fs::read_dir;
use std::fmt::Debug;
use std::{env};
use std::path::{Path, PathBuf};

use clap::Parser;
use anyhow::Result;
use cargo_metadata::{CargoOpt, MetadataCommand};
use convert_case::{Case, Casing};

#[derive(Debug, Parser)]
pub struct PackageOption {
    /// Release profile name
    #[clap(long, default_value = "release-lto")]
    pub release: String,

    /// Optional package/project name
    #[clap(long, short)]
    pub package_name: Option<String>,
}

#[derive(Debug)]
pub struct PackageInfo {
    // The requested package/project name
    pub package: String,
    /// The requested package/project root folder
    pub package_path: PathBuf,
    // Inferred binary output path
    pub output_path: PathBuf,
}
impl PackageInfo {
    /// Finds the closest Cargo.toml in the tree, starting from the current directory
    pub fn get_current_project_path() -> Result<Option<PathBuf>, String> {
        let cwd = env::current_dir()
            .map_err(|e| format!("Failed to get current working directory: {}", e))?;
        let parents = cwd.as_path().ancestors();

        for path in parents {
            if let Some(filename) = read_dir(path)
                .map_err(|e| format!("Failed to read directory: {}", e))?
                .into_iter()
                .map(|p| p.unwrap().file_name())
                .find(|p| p.eq(&OsString::from("Cargo.toml")))
            {
                return Ok(Some(Path::new(path).join(filename)));
            }
        }
        Ok(None)
    }

    /// From the given options, attempt to resolve a specific cargo package and output path
    pub fn from_options(options: &PackageOption) -> Result<PackageInfo, String> {
        let current_project = Self::get_current_project_path()?
            .ok_or("Could not find a Cargo.toml from the current working directory")?;

        // get metadata for the current project
        let metadata = MetadataCommand::new()
            .manifest_path(&current_project)
            .features(CargoOpt::AllFeatures)
            .exec()
            .map_err(|_| {
                format!(
                    "Failed to load cargo project at {}",
                    current_project.display()
                )
            })?;

        let package = if let Some(root_package) = metadata.root_package() {
            // we found a root project already, if the user is expecting something else raise an error
            if let Some(package_name) = &options.package_name {
                if package_name != &root_package.name {
                    return Err(format!(
                        "Current package name ({}) does not match the supplied package name ({}).",
                        root_package.name, package_name
                    ));
                }
            }
            root_package.clone()
        } else if let Some(package_name) = &options.package_name {
            // try to find the requested package in the current workspace
            let project = metadata
                .packages
                .into_iter()
                .find(|p| &p.name == package_name);
            project.ok_or(format!(
                "Could not find a package '{}' in {}",
                package_name,
                current_project.display()
            ))?
        } else {
            return Err(format!("Could not find a default cargo package in {}. Try the `-p` option to specify a project/package.", current_project.display()));
        };

        // format the expected output path
        let output_path = PathBuf::from(format!(
            "{}/{}/{}/{}.wasm",
            metadata.target_directory,
            crate::build::BUILD_TARGET,
            options.release,
            package.name.to_case(Case::Snake)
        ));

        // find the path of the parent folder for this Cargo.toml
        let package_path: PathBuf = package
            .manifest_path
            .parent()
            .ok_or(format!(
                "Could not get parent folder for {}",
                package.manifest_path
            ))?
            .into();

        Ok(PackageInfo {
            package: package.name,
            package_path,
            output_path,
        })
    }

    /// Read the raw bytes from the package 'output_path'
    pub(crate) fn read_bytes(&self) -> Result<Vec<u8>> {
        crate::read_bytes_from_path(&self.output_path)
    }
}
