use url::Url;
use http::Request;
use crate::package_id::WithVersion;
use crate::{Result, FluvioIndex, Package, PackageId, Target, TagName};

pub struct HttpAgent {
    base_url: url::Url,
}

impl Default for HttpAgent {
    fn default() -> Self {
        Self {
            base_url: url::Url::parse(crate::INDEX_LOCATION).unwrap(),
        }
    }
}

impl HttpAgent {
    pub fn with_prefix(prefix: &str) -> Result<Self> {
        Ok(Self {
            base_url: Url::parse(crate::INDEX_HOST).unwrap().join(prefix)?,
        })
    }

    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    pub fn request_index(&self) -> Result<Request<()>> {
        let url = self.base_url.join("index.json")?;
        Ok(Request::get(url.as_str()).body(())?)
    }

    pub async fn index_from_response(&self, response: &[u8]) -> Result<FluvioIndex> {
        let index: FluvioIndex = serde_json::from_slice(response)?;
        Ok(index)
    }

    pub fn request_package<T>(&self, id: &PackageId<T>) -> Result<Request<()>> {
        let url =
            self.base_url
                .join(&format!("packages/{}/{}/meta.json", id.group(), id.name()))?;
        Ok(Request::get(url.as_str()).body(())?)
    }

    pub async fn package_from_response(&self, response: &[u8]) -> Result<Package> {
        let package: Package = serde_json::from_slice(response)?;
        Ok(package)
    }

    pub fn request_tag(&self, id: &PackageId<WithVersion>, tag: &TagName) -> Result<Request<()>> {
        let url = self.base_url.join(&format!(
            "packages/{group}/{name}/tags/{tag}",
            group = id.group(),
            name = id.name(),
            tag = tag,
        ))?;

        Ok(Request::get(url.as_str()).body(())?)
    }

    pub fn request_release_download<T>(
        &self,
        id: &PackageId<T>,
        version: &semver::Version,
        target: &Target,
    ) -> Result<Request<()>> {
        let file_name = if target.to_string().contains("windows") {
            format!("{}.exe", id.name())
        } else {
            id.name().to_string()
        };

        let url = self.base_url.join(&format!(
            "packages/{group}/{name}/{version}/{target}/{file_name}",
            group = &id.group(),
            name = &id.name(),
            file_name = file_name,
            version = version,
            target = target.as_str(),
        ))?;

        Ok(Request::get(url.as_str()).body(())?)
    }

    pub fn request_release_checksum<T>(
        &self,
        id: &PackageId<T>,
        version: &semver::Version,
        target: &Target,
    ) -> Result<Request<()>> {
        let file_name = if target.to_string().contains("windows") {
            format!("{}.exe", id.name())
        } else {
            id.name().to_string()
        };
        let url = self.base_url.join(&format!(
            "packages/{group}/{name}/{version}/{target}/{file_name}.sha256",
            group = &id.group(),
            name = &id.name(),
            file_name = file_name,
            version = version,
            target = target.as_str(),
        ))?;

        Ok(Request::get(url.as_str()).body(())?)
    }

    pub async fn tag_version_from_response(
        &self,
        tag: &TagName,
        response: &[u8],
    ) -> Result<semver::Version> {
        let string = String::from_utf8_lossy(response);
        if string.contains("<title>404 Not Found") {
            return Err(crate::Error::TagDoesNotExist(tag.to_string()));
        }
        let version = semver::Version::parse(&string)?;
        Ok(version)
    }
}
