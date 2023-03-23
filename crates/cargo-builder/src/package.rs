use std::ffi::OsString;
use std::fs::read_dir;
use std::fmt::Debug;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use cargo_metadata::{CargoOpt, MetadataCommand, Package};

#[derive(Debug)]
pub struct PackageOption {
    pub release: String,
    pub package_name: Option<String>,
}

#[derive(Debug)]
pub struct PackageInfo {
    package: Package,
    /// The package/project root folder
    package_path: PathBuf,
    /// The package/project target folder
    target_dir: PathBuf,
    /// Profile used in build, e.g. release, release-lto, etc.
    profile: String,
}
impl PackageInfo {
    /// From the given options, attempt to resolve a specific cargo package and output path
    pub fn from_options(options: &PackageOption) -> anyhow::Result<PackageInfo> {
        let current_project = get_current_project_path()?
            .context("Could not find a Cargo.toml from the current working directory")?;

        // get metadata for the current project
        let metadata = MetadataCommand::new()
            .manifest_path(&current_project)
            .features(CargoOpt::AllFeatures)
            .verbose(true)
            .exec()
            .context(format!(
                "Failed to load cargo project at {}",
                current_project.display()
            ))?;

        let package = if let Some(root_package) = metadata.root_package() {
            // we found a root project already, if the user is expecting something else raise an error
            if let Some(package_name) = &options.package_name {
                if package_name != &root_package.name {
                    return Err(anyhow!(
                        "Current package name ({}) does not match the supplied package name ({}).",
                        root_package.name,
                        package_name
                    ));
                }
            }
            root_package
        } else if let Some(package_name) = &options.package_name {
            // try to find the requested package in the current workspace
            let project = metadata.packages.iter().find(|p| &p.name == package_name);
            project.ok_or_else(|| {
                anyhow!(
                    "Could not find a package '{}' in {}",
                    package_name,
                    current_project.display()
                )
            })?
        } else {
            return Err(anyhow!("Could not find a default cargo package in {}. Try the `-p` option to specify a project/package.", current_project.display()));
        };

        // find the path of the parent folder for this Cargo.toml
        let package_path: PathBuf = package
            .manifest_path
            .parent()
            .ok_or_else(|| anyhow!("Could not get parent folder for {}", package.manifest_path))?
            .into();

        let package = package.clone();
        Ok(PackageInfo {
            package,
            package_path,
            target_dir: metadata.target_directory.into(),
            profile: options.release.clone(),
        })
    }

    pub fn package_name(&self) -> &str {
        self.package.name.as_str()
    }

    pub fn package_path(&self) -> &Path {
        self.package_path.as_path()
    }

    pub fn package_relative_path<P: AsRef<Path>>(&self, child: P) -> PathBuf {
        self.package_path().join(child)
    }

    /// path to package's bin target
    pub fn target_bin_path(&self) -> anyhow::Result<PathBuf> {
        let mut path = self.target_dir.clone();
        path.push(&self.profile);
        path.push(self.target_name()?);
        Ok(path)
    }

    /// path to package's wasm32 target
    pub fn target_wasm32_path(&self) -> anyhow::Result<PathBuf> {
        let mut path = self.target_dir.clone();
        path.push("wasm32-unknown-unknown");
        path.push(&self.profile);
        path.push(self.target_name()?.replace('-', "_"));
        path.set_extension("wasm");
        Ok(path)
    }

    pub fn target_name(&self) -> anyhow::Result<&str> {
        self.package
            .targets
            .get(0)
            .map(|target| target.name.as_str())
            .ok_or_else(|| anyhow!("package does not have any targets"))
    }
}
/// Finds the closest Cargo.toml in the tree, starting from the current directory
pub fn get_current_project_path() -> anyhow::Result<Option<PathBuf>> {
    let cwd = env::current_dir().context("failed to get current working directory")?;
    let parents = cwd.as_path().ancestors();

    for path in parents {
        if let Some(filename) = read_dir(path)
            .context("failed to read directory")?
            .map(|p| p.unwrap().file_name())
            .find(|p| p.eq(&OsString::from("Cargo.toml")))
        {
            return Ok(Some(Path::new(path).join(filename)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_package_info() {
        //given
        let opt = PackageOption {
            release: "release-lto".into(),
            package_name: None,
        };

        //when
        let package_info = PackageInfo::from_options(&opt).unwrap();

        //then
        assert_eq!(package_info.package_name(), "cargo-builder");
        assert!(package_info
            .package_path()
            .ends_with("crates/cargo-builder"));
        assert_eq!(package_info.target_name().unwrap(), "cargo-builder");
        assert!(package_info
            .target_bin_path()
            .unwrap()
            .ends_with("release-lto/cargo-builder"));
        assert!(package_info
            .target_wasm32_path()
            .unwrap()
            .ends_with("wasm32-unknown-unknown/release-lto/cargo_builder.wasm"));
    }
}
