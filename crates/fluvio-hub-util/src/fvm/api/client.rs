//! Hub FVM API Client

use anyhow::{Error, Result};
use url::Url;

use crate::fvm::{Channel, PackageSet, PackageSetRecord};

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
    pub async fn fetch_package_set(&self, channel: &Channel, arch: &str) -> Result<PackageSet> {
        let url = self.make_fetch_package_set_url(channel, arch)?;
        let mut res = surf::get(url)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let pkgset_record = res.body_json::<PackageSetRecord>().await.map_err(|err| {
            tracing::error!(?err, "Failed to parse PackageSet from Hub");
            Error::msg(format!(
                "Server responded with status code {}",
                err.status()
            ))
        })?;

        tracing::info!(?pkgset_record, "Found PackageSet");

        Ok(pkgset_record.into())
    }

    /// Builds the URL to the Hub API for fetching a [`PackageSet`] using the
    /// [`Client`]'s `api_url`.
    fn make_fetch_package_set_url(&self, channel: &Channel, arch: &str) -> Result<Url> {
        let url = format!(
            "{}hub/v1/fvm/pkgset/{channel}?arch={arch}",
            self.api_url,
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
            .make_fetch_package_set_url(&Channel::Stable, "arm-unknown-linux-gnueabihf")
            .unwrap();

        assert_eq!(
            url.as_str(),
            "https://hub.infinyon.cloud/hub/v1/fvm/pkgset/stable?arch=arm-unknown-linux-gnueabihf"
        );
    }

    #[test]
    fn builds_url_for_fetching_pkgsets_on_version() {
        let client = Client::new("https://hub.infinyon.cloud").unwrap();
        let url = client
            .make_fetch_package_set_url(
                &Channel::Tag(Version::from_str("0.10.14-dev+123345abc").unwrap()),
                "arm-unknown-linux-gnueabihf",
            )
            .unwrap();

        assert_eq!(url.as_str(), "https://hub.infinyon.cloud/hub/v1/fvm/pkgset/0.10.14-dev+123345abc?arch=arm-unknown-linux-gnueabihf");
    }
}
