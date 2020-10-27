use http_types::{Request, Response};
use crate::{Result, FluvioIndex, Package, PackageId, Release, Platform, Error};
use crate::error::HttpError;

pub struct HttpAgent {
    base_url: url::Url,
}

impl HttpAgent {
    pub fn new() -> Self {
        Self {
            base_url: url::Url::parse(crate::INDEX_LOCATION).unwrap(),
        }
    }

    pub fn request_index(&self) -> Result<Request> {
        let url = self.base_url.join("index.json")?;
        Ok(Request::get(url))
    }

    pub async fn index_from_response(&self, mut response: Response) -> Result<FluvioIndex> {
        let index: FluvioIndex = response.body_json().await
            .map_err(|inner| HttpError { inner })?;
        Ok(index)
    }

    pub fn request_package(&self, id: &PackageId) -> Result<Request> {
        let url = self.base_url.join(
            &format!("packages/{}/{}/meta.json",
                    id.group,
                    id.name
            ))?;
        Ok(Request::get(url))
    }

    pub async fn package_from_response(&self, mut response: Response) -> Result<Package> {
        let package: Package = response.body_json().await
            .map_err(|inner| HttpError { inner })?;
        Ok(package)
    }

    pub fn request_release_download(&self, id: &PackageId, platform: &Platform) -> Result<Request> {
        let version = id.version.as_ref()
            .ok_or(Error::MissingVersion)?;
        let url = self.base_url.join(
            &format!("packages/{group}/{name}/{version}/{platform}/{name}",
                group = &id.group,
                name = &id.name,
                version = version,
                platform = platform.as_str(),
            )
        )?;

        Ok(Request::get(url))
    }

    pub fn request_release_checksum(&self, id: &PackageId, platform: &Platform) -> Result<Request> {
        let version = id.version.as_ref()
            .ok_or(Error::MissingVersion)?;
        let url = self.base_url.join(
            &format!("packages/{group}/{name}/{version}/{platform}/{name}.sha256",
                group = &id.group,
                name = &id.name,
                version = version,
                platform = platform.as_str(),
            )
        )?;

        Ok(Request::get(url))
    }

    pub async fn release_from_response(&self, mut response: Response) -> Result<Release> {
        let release: Release = response.body_json().await
            .map_err(|inner| HttpError { inner })?;
        Ok(release)
    }
}
