//! Hub FVM API Client

use std::path::PathBuf;
use std::fs::File;
use std::io::{Cursor, copy, Read};

use anyhow::{Error, Result};
use sha2::{Digest, Sha256};
use surf::{get, StatusCode};
use url::Url;

use crate::fvm::{Channel, PackageSet, Artifact};

/// HTTP Client for interacting with the Hub FVM API
pub struct Client {
    api_url: Url,
}

impl Client {
    /// Creates a new [`Client`] with the default Hub API URL
    pub fn new(url: &str) -> Result<Self> {
        let api_url = url.parse::<Url>()?;

        Ok(Self { api_url })
    }

    /// Fetches a [`PackageSet`] from the Hub with the specific [`Channel`]
    pub async fn fetch_package_set(
        &self,
        name: impl AsRef<str>,
        channel: &Channel,
        arch: &str,
    ) -> Result<PackageSet> {
        let url = self.make_fetch_package_set_url(name, channel, arch)?;
        let mut res = get(url).await.map_err(|err| Error::msg(err.to_string()))?;
        let pkg = res.body_json::<PackageSet>().await.map_err(|err| {
            Error::msg(format!(
                "Server responded with status code {}",
                err.status()
            ))
        })?;

        Ok(pkg)
    }

    /// Downloads binaries from the Hub into the specified `target_dir`.
    /// Returns the [`PackageSet`] that was downloaded.
    pub async fn download_package_set(
        &self,
        name: impl AsRef<str>,
        channel: &Channel,
        arch: &str,
        target_dir: PathBuf,
    ) -> Result<PackageSet> {
        let pkgset = self.fetch_package_set(name, channel, arch).await?;

        for artf in pkgset.artifacts.iter() {
            let mut res = surf::get(&artf.download_url)
                .await
                .map_err(|err| Error::msg(err.to_string()))?;

            if res.status() == StatusCode::Ok {
                let out_path = target_dir.join(&artf.name);
                let mut file = File::create(&out_path)?;
                let mut buf = Cursor::new(
                    res.body_bytes()
                        .await
                        .map_err(|err| Error::msg(err.to_string()))?,
                );

                copy(&mut buf, &mut file)?;
                self.checksum_artifact(artf, &file).await?;

                tracing::debug!(
                    "Artifact downloaded: {} at {:?}",
                    artf.name,
                    out_path.display()
                );

                continue;
            }

            tracing::warn!(
                "Failed to download artifact {}@{} from {}",
                artf.name,
                artf.version,
                artf.download_url
            );
        }

        Ok(pkgset)
    }

    /// Verifies downloaded artifacts checksums against the upstream checksums
    async fn checksum_artifact(&self, artf: &Artifact, local: &File) -> Result<()> {
        let local_file_shasum = Self::shasum256(local)?;
        let upstream_shasum = surf::get(&artf.sha256_url)
            .await
            .map_err(|err| Error::msg(err.to_string()))?
            .body_string()
            .await
            .map_err(|err| Error::msg(err.to_string()))?;

        if local_file_shasum != upstream_shasum {
            return Err(Error::msg(format!(
                "Artifact {} didnt matched upstream shasum. {} != {}",
                artf.name, local_file_shasum, upstream_shasum
            )));
        }

        Ok(())
    }

    /// Generates the Sha256 checksum for the specified file
    fn shasum256(file: &File) -> Result<String> {
        let meta = file.metadata()?;
        let mut file = file.clone();
        let mut hasher = Sha256::new();
        let mut buffer = vec![0; meta.len() as usize];

        file.read_exact(&mut buffer)?;
        hasher.update(buffer);

        let output = hasher.finalize();
        Ok(hex::encode(output))
    }

    /// Builds the URL to the Hub API for fetching a [`PackageSet`] using the
    /// [`Client`]'s `api_url`.
    fn make_fetch_package_set_url(
        &self,
        name: impl AsRef<str>,
        channel: &Channel,
        arch: &str,
    ) -> Result<Url> {
        let url = format!(
            "{}hub/v1/fvm/pkgset/{name}/{channel}/{arch}",
            self.api_url,
            name = name.as_ref(),
            channel = channel,
            arch = arch
        );

        Ok(Url::parse(&url)?)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use url::Url;
    use semver::Version;

    use super::{Client, Channel};

    #[test]
    fn creates_a_default_client() {
        let client = Client::new("https://hub.infinyon.cloud").unwrap();

        assert_eq!(
            client.api_url,
            Url::parse("https://hub.infinyon.cloud").unwrap()
        );
    }

    #[test]
    fn builds_url_for_fetching_pkgsets() {
        let client = Client::new("https://hub.infinyon.cloud").unwrap();
        let url = client
            .make_fetch_package_set_url("fluvio", &Channel::Stable, "arm-unknown-linux-gnueabihf")
            .unwrap();

        assert_eq!(url.as_str(), "https://hub.infinyon.cloud/hub/v1/fvm/pkgset/fluvio/stable/arm-unknown-linux-gnueabihf");
    }

    #[test]
    fn builds_url_for_fetching_pkgsets_on_version() {
        let client = Client::new("https://hub.infinyon.cloud").unwrap();
        let url = client
            .make_fetch_package_set_url(
                "fluvio",
                &Channel::Tag(Version::from_str("0.10.14-dev+123345abc").unwrap()),
                "arm-unknown-linux-gnueabihf",
            )
            .unwrap();

        assert_eq!(url.as_str(), "https://hub.infinyon.cloud/hub/v1/fvm/pkgset/fluvio/0.10.14-dev+123345abc/arm-unknown-linux-gnueabihf");
    }
}
