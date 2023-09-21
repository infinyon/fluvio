use std::str::FromStr;

use surf::Client;
use url::Url;

use fluvio_hub_util::fvm::{PackageSet, RustTarget};

use crate::{Error, Result};
use crate::common::{INFINYON_HUB_FVM_PKGSET_API_URI, TARGET};

/// A Fluvio Version Name
pub type Version = String;

/// Installation Task used to install a specific version of Fluvio in via the
/// FVM CLI.
#[derive(Clone, Debug)]
pub struct InstallTask {
    /// The Host's Architecture written in Rust Target Format
    arch: RustTarget,
    /// Registry where to find the Fluvio Versions
    registry: Url,
    /// Package Set to install
    pkgset: String,
    /// Version to install
    version: Version,
}

impl InstallTask {
    pub fn new(registry: Url, pkgset: String, version: Version) -> Self {
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
        let mut res = client
            .get(url)
            .await
            .map_err(|err| Error::RegistryFetch(self.clone(), err))?;
        let pkgset = res
            .body_json::<PackageSet>()
            .await
            .map_err(|err| Error::RegistryFetch(self.clone(), err))?;

        Ok(pkgset)
    }

    /// Fetches the Pkgset from the Registry
    pub async fn fetch_artifact_shasum(&self, artifact: &str) -> Result<String> {
        let client = Client::new();
        let url = self
            .make_artifact_shasum256_url(artifact)
            .map_err(|err| Error::Install(err.to_string()))?;
        let mut res = client
            .get(url)
            .await
            .map_err(|err| Error::Install(err.to_string()))?;
        let shasum256 = res
            .body_string()
            .await
            .map_err(|err| Error::Install(err.to_string()))?;

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
    fn make_artifact_shasum256_url(
        &self,
        artifact: &str,
    ) -> std::result::Result<Url, url::ParseError> {
        let url = &format!(
            "https://packages.fluvio.io/v1/packages/fluvio/{artifact}/{version}/{arch}/{artifact}.sha256",
            version = self.version,
            arch = self.arch,
        );

        Url::parse(url)
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
