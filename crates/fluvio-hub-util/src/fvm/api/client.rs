//! Hub FVM API Client

use anyhow::Result;
use reqwest;
use url::Url;

use crate::fvm::{Channel, PackageSet};

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
        let res = reqwest::get(url).await?;
        let pkg = res.json().await?;

        tracing::info!(?pkg, "Found PackageSet");

        Ok(pkg)
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
