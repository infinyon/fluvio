//! Hub FVM API Client

use std::env::var;

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::fvm::{Channel, PackageSet, PackageSetRecord};

const INFINYON_CI_CONTEXT: &str = "INFINYON_CI_CONTEXT";
const CONTEXT_QUERY_PARAM: &str = "ctx";

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

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
        use crate::htclient::ResponseExt;

        let url = self.make_fetch_package_set_url(channel, arch)?;
        let res = crate::htclient::get(url)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let res_status = res.status();

        if res_status.is_success() {
            let pkgset_record = res.json::<PackageSetRecord>().map_err(|err| {
                tracing::debug!(?err, "Failed to parse PackageSet from Hub");
                Error::msg("Failed to parse server's response")
            })?;

            tracing::info!(?pkgset_record, "Found PackageSet");
            return Ok(pkgset_record.into());
        }

        let error = res.json::<ApiError>().map_err(|err| {
            tracing::debug!(?err, "Failed to parse API Error from Hub");
            Error::msg(format!("Server responded with status code {}", res_status))
        })?;

        tracing::debug!(?error, "Server responded with not successful status code");

        Err(anyhow::anyhow!(error.message))
    }

    /// Builds the URL to the Hub API for fetching a [`PackageSet`] using the
    /// [`Client`]'s `api_url`.
    fn make_fetch_package_set_url(&self, channel: &Channel, arch: &str) -> Result<Url> {
        let mut url = Url::parse(&format!(
            "{}hub/v1/fvm/pkgset/{channel}",
            self.api_url,
            channel = channel,
        ))?;
        let mut params = url::form_urlencoded::Serializer::new(String::new());

        params.append_pair("arch", arch);

        if let Ok(ctx) = var(INFINYON_CI_CONTEXT) {
            params.append_pair(CONTEXT_QUERY_PARAM, ctx.as_str());
        }

        url.set_query(Some(params.finish().as_str()));

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::env::{set_var, remove_var};

    use url::Url;
    use semver::Version;

    use crate::fvm::api::client::INFINYON_CI_CONTEXT;

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
    fn builds_urls_for_fetching_pkgsets() {
        // Scenario: Using Stable Channel

        let client = Client::new("https://hub.infinyon.cloud").unwrap();
        let url = client
            .make_fetch_package_set_url(&Channel::Stable, "arm-unknown-linux-gnueabihf")
            .unwrap();

        assert_eq!(
            url.as_str(),
            "https://hub.infinyon.cloud/hub/v1/fvm/pkgset/stable?arch=arm-unknown-linux-gnueabihf",
            "failed on Scenario Using Stable Channel"
        );

        // Scenario: Using Tag

        let client = Client::new("https://hub.infinyon.cloud").unwrap();
        let url = client
            .make_fetch_package_set_url(
                &Channel::Tag(Version::from_str("0.10.14-dev+123345abc").unwrap()),
                "arm-unknown-linux-gnueabihf",
            )
            .unwrap();

        assert_eq!(url.as_str(), "https://hub.infinyon.cloud/hub/v1/fvm/pkgset/0.10.14-dev+123345abc?arch=arm-unknown-linux-gnueabihf", "failed on Scenario Using Tag");

        // Scenario: Using Context

        set_var(INFINYON_CI_CONTEXT, "unit_testing");

        let client = Client::new("https://hub.infinyon.cloud").unwrap();
        let url = client
            .make_fetch_package_set_url(
                &Channel::Tag(Version::from_str("0.10.14-dev+123345abc").unwrap()),
                "arm-unknown-linux-gnueabihf",
            )
            .unwrap();

        assert_eq!(url.as_str(), "https://hub.infinyon.cloud/hub/v1/fvm/pkgset/0.10.14-dev+123345abc?arch=arm-unknown-linux-gnueabihf&ctx=unit_testing", "failed on Scenario Using Context");
        remove_var(INFINYON_CI_CONTEXT);
    }
}
