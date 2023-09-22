//! Fluvio Version Manager Install Utilities
//!
//! When FVM is first installed, it needs to be initialized. This is achieved by
//! preparing the workspace which involves creating the `.fvm` directory and
//! providing the `fvm` binary in the `~/.fvm/bin` directory.
//!
//! This is likely to run once and automatically by the installer after
//! downloading the FVM binary.

use std::env::current_exe;
use std::fs::{copy, create_dir};
use std::path::PathBuf;
use std::str::FromStr;

use surf::Client;
use url::Url;

use fluvio_hub_util::fvm::{PackageSet, RustTarget};

use crate::{Error, Result};
use crate::common::{
    INFINYON_HUB_FVM_PKGSET_API_URI, TARGET, FVM_BINARY_NAME, FVM_HOME_DIR, FVM_PACKAGES_SET_DIR,
};

/// Retrieves the path to the `~/.fvm` directory in the host system.
/// This function only builds the path, it doesn't check if the directory exists.
pub fn fvm_path() -> Result<PathBuf> {
    let Some(home_dir) = dirs::home_dir() else {
        return Err(Error::HomeDirNotFound);
    };
    let fvm_path = home_dir.join(FVM_HOME_DIR);

    Ok(fvm_path)
}

/// Checks if the FVM is installed. This is achieved by checking if the
/// binary is present in the `FVM` home directory.
///
/// The returned path is the path to the FVM binary found.
pub fn fvm_bin_path() -> Result<Option<PathBuf>> {
    let fvm_path = fvm_path()?;
    let fvm_binary_path = fvm_path.join("bin").join(FVM_BINARY_NAME);

    if fvm_binary_path.exists() {
        return Ok(Some(fvm_binary_path));
    }

    Ok(None)
}

/// Retrieves the path to the `~/.fvm/pkgset` directory in the host system.
pub fn fvm_pkgset_path() -> Result<PathBuf> {
    let fvm_path = fvm_path()?;
    let fvm_pkget_path = fvm_path.join(FVM_PACKAGES_SET_DIR);

    Ok(fvm_pkget_path)
}

/// Installs FVM in the host system.
///
/// Intallation process consists in creating the FVM home `bin` directory, and
/// copying the FVM binary to the FVM home `bin` directory.
///
/// This is executed on the first installation of FVM, similar to hom `rustup`
/// does when installed.
///
/// Returs the path to the directory where FVM was installed.
pub fn install_fvm() -> Result<PathBuf> {
    // Creates the directory `~/.fvm` if doesn't exists
    let fvm_dir = fvm_path()?;

    if !fvm_dir.exists() {
        create_dir(&fvm_dir).map_err(|err| Error::Setup(err.to_string()))?;
        tracing::debug!(?fvm_dir, "Created FVM home directory");
    }

    // Attempts to create the binary crate
    let fvm_binary_dir = fvm_dir.join("bin");
    create_dir(fvm_binary_dir).map_err(|err| Error::Setup(err.to_string()))?;

    // Copies "this" binary to the FVM binary directory
    let current_binary_path = current_exe().map_err(|err| Error::Setup(err.to_string()))?;
    let fvm_binary_path = fvm_dir.join("bin").join("fvm");

    copy(current_binary_path, fvm_binary_path).map_err(|err| Error::Setup(err.to_string()))?;
    tracing::debug!(?fvm_dir, "Copied the FVM binary to the FVM home directory");

    // Creates the package set directory
    let fvm_pkgset_dir = fvm_dir.join(FVM_PACKAGES_SET_DIR);
    create_dir(fvm_pkgset_dir).map_err(|err| Error::Setup(err.to_string()))?;

    Ok(fvm_dir)
}

/// Installation Task used to install a specific version of Fluvio in via the
/// FVM CLI.
#[derive(Clone, Debug)]
pub struct InstallTask {
    /// The Host's Architecture written in Rust Target Format
    pub arch: RustTarget,
    /// Registry where to find the Fluvio Versions
    pub registry: Url,
    /// Package Set to install
    pub pkgset: String,
    /// Version to install
    pub version: String,
}

impl InstallTask {
    pub fn new(registry: Url, pkgset: String, version: String) -> Self {
        let arch = RustTarget::from_str(TARGET).expect("Platform not supported");

        Self {
            registry,
            pkgset,
            version,
            arch,
        }
    }

    /// Fetches the Pkgset from the Registry
    pub async fn fetch_pkgset(&self) -> Result<PackageSet> {
        let client = Client::new();
        let url = self.make_pkgset_url();
        let mut res = client.get(url).await?;
        let pkgset = res.body_json::<PackageSet>().await?;

        Ok(pkgset)
    }

    /// Fetches the Pkgset from the Registry
    pub async fn fetch_artifact_shasum(&self, artifact: &str) -> Result<String> {
        let client = Client::new();
        let url = self.make_artifact_shasum256_url(artifact)?;
        let mut res = client.get(url).await?;
        let shasum256 = res.body_string().await?;

        Ok(shasum256)
    }

    /// Constructs the [`Url`] to fetch the [`PackageSet`] from the Registry
    fn make_pkgset_url(&self) -> Url {
        let mut registry = self.registry.clone();

        registry.set_path(&format!(
            "{INFINYON_HUB_FVM_PKGSET_API_URI}/{package}/{version}/{arch}",
            package = self.pkgset,
            version = self.version,
            arch = self.arch,
        ));

        registry
    }

    /// Constructs the [`Url`] to fetch the Sha256 for the Specified Artifact
    /// https://packages.fluvio.io/v1/packages/fluvio/fluvio-run/0.10.14/aarch64-apple-darwin/fluvio-run.sha256
    fn make_artifact_shasum256_url(&self, artifact: &str) -> Result<Url> {
        let url = &format!(
            "https://packages.fluvio.io/v1/packages/fluvio/{artifact}/{version}/{arch}/{artifact}.sha256",
            version = self.version,
            arch = self.arch,
        );
        let url = Url::parse(url)?;

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use fluvio_hub_util::fvm::RustTarget;

    use super::InstallTask;

    #[test]
    fn creates_pkgset_url_as_expected() {
        let task = InstallTask {
            arch: RustTarget::Aarch64AppleDarwin,
            pkgset: "default".to_string(),
            version: "0.10.14".to_string(),
            registry: "https://hub-dev.infinyon.cloud".parse().unwrap(),
        };
        let have = task.make_pkgset_url().to_string();
        let want =
            "https://hub-dev.infinyon.cloud/hub/v1/fvm/pkgset/default/0.10.14/aarch64-apple-darwin";

        assert_eq!(have, want);
    }

    #[test]
    fn creates_artifact_shasum_url_as_expected() {
        let task = InstallTask {
            arch: RustTarget::Aarch64AppleDarwin,
            pkgset: "default".to_string(),
            version: "0.10.14".to_string(),
            registry: "https://hub-dev.infinyon.cloud".parse().unwrap(),
        };
        let have = task
            .make_artifact_shasum256_url("fluvio-run")
            .unwrap()
            .to_string();
        let want =
            "https://packages.fluvio.io/v1/packages/fluvio/fluvio-run/0.10.14/aarch64-apple-darwin/fluvio-run.sha256";

        assert_eq!(have, want);
    }
}
