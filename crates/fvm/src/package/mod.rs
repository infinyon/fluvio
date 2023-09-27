//! Fluvio Versions Management (a.k.a. Package)

pub mod manifest;

use std::str::FromStr;

use surf::Client;
use thiserror::Error;
use url::Url;

use fluvio_hub_util::fvm::{PackageSet, RustTarget, Channel};

use crate::Result;
use crate::common::{INFINYON_HUB_FVM_PKGSET_API_URI, TARGET};

#[derive(Debug, Error)]
pub enum PackageError {
    #[error("Failed to parse manifest file. {0}")]
    ManifestParse(serde_json::Error),
    #[error("Failed to serialize manifest. {0}")]
    ManifestSerialization(serde_json::Error),
}

/// Installation Task used to install a specific version of Fluvio
#[derive(Clone, Debug)]
pub struct InstallTask {
    /// The Host's Architecture written in Rust Target Format
    pub arch: RustTarget,
    /// Registry where to find the Fluvio Versions
    pub registry: Url,
    /// Package Set to install
    pub pkgset: String,
    /// Version to install
    pub version: Channel,
}

impl InstallTask {
    pub fn new(registry: Url, pkgset: String, version: Channel) -> Self {
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

        tracing::info!("Fetching package set from: {}", url);

        let mut res = client.get(url).await?;
        let pkgset = res.body_json::<PackageSet>().await?;

        Ok(pkgset)
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
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use fluvio_hub_util::fvm::{Channel, RustTarget};

    use super::InstallTask;

    #[test]
    fn creates_pkgset_url_as_expected() {
        let task = InstallTask {
            arch: RustTarget::Aarch64AppleDarwin,
            pkgset: "default".to_string(),
            version: Channel::from_str("0.10.14").expect("Channel parsing failed"),
            registry: "https://hub-dev.infinyon.cloud".parse().unwrap(),
        };
        let have = task.make_pkgset_url().to_string();
        let want =
            "https://hub-dev.infinyon.cloud/hub/v1/fvm/pkgset/default/0.10.14/aarch64-apple-darwin";

        assert_eq!(have, want);
    }
}
