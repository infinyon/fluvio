use std::io::{Error as IoError, ErrorKind};

use crate::bytes::{BufMut, Buf};
use crate::{Encoder, Version, Decoder};
use crate::api::Request;

use super::ErrorCode;

pub const VERSIONS_API_KEY: u16 = 18;
pub const V10_PLATFORM: i16 = 2;

// -----------------------------------
// ApiVersionsRequest
// -----------------------------------

#[derive(Decoder, Encoder, Default, Debug)]
pub struct ApiVersionsRequest {
    #[fluvio(min_version = 1)]
    pub client_version: String,
    #[fluvio(min_version = 1)]
    pub client_os: String,
    #[fluvio(min_version = 1)]
    pub client_arch: String,
}

impl Request for ApiVersionsRequest {
    const API_KEY: u16 = VERSIONS_API_KEY;
    const DEFAULT_API_VERSION: i16 = V10_PLATFORM;
    type Response = ApiVersionsResponse;
}

// -----------------------------------
// ApiVersionsResponse
// -----------------------------------

pub type ApiVersions = Vec<ApiVersionKey>;

#[derive(Decoder, Encoder, Default, Debug, Eq, PartialEq)]
pub struct ApiVersionsResponse {
    pub error_code: ErrorCode,
    pub api_keys: ApiVersions,
    pub platform_version: PlatformVersion,
}

#[derive(Decoder, Encoder, Default, Clone, Debug, Eq, PartialEq)]
pub struct ApiVersionKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PlatformVersion(String);

impl PlatformVersion {
    pub fn new(version: &semver::Version) -> Self {
        Self(version.to_string())
    }

    /// Creates a `semver::Version` object of the inner version
    pub fn to_semver(&self) -> semver::Version {
        // This is safe because of this type's invariant:
        // The only ways to construct it are From<semver::Version>,
        // via the Decoder trait, or by Default. The Decoder impl explicitly
        // checks that the inner string is Semver, and the Default impl
        // directly constructs a Semver and stringifies it. Therefore this
        // unwrapping is safe.
        semver::Version::parse(&self.0)
            .expect("Broken Invariant: PlatformVersion can only be constructed with Semver")
    }
}

impl From<semver::Version> for PlatformVersion {
    fn from(version: semver::Version) -> Self {
        Self(version.to_string())
    }
}

impl Default for PlatformVersion {
    fn default() -> Self {
        Self(semver::Version::new(0, 0, 0).to_string())
    }
}

impl Decoder for PlatformVersion {
    /// Wrap the decoder for the string inside
    fn decode<T>(&mut self, src: &mut T, version: i16) -> Result<(), IoError>
    where
        T: Buf,
    {
        // Decoder the data as a String
        let mut string = String::default();
        string.decode(src, version)?;

        // Before constructing, ensure that what was decoded is valid Semver
        let _version = semver::Version::parse(&string).map_err(|_| {
            IoError::new(
                ErrorKind::InvalidData,
                "PlatformVersion is not valid semver",
            )
        })?;

        // Construct PlatformVersion with semver string
        self.0 = string;
        Ok(())
    }
}

impl Encoder for PlatformVersion {
    fn write_size(&self, version: Version) -> usize {
        self.0.write_size(version)
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        self.0.encode(dest, version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_platform_version() {
        let version = semver::Version::parse("1.2.3-alpha.4+56789").unwrap();
        let version_string = version.to_string();
        let platform_version = PlatformVersion::from(version);

        // Check encoding matches
        let mut version_string_buffer: Vec<u8> = vec![];
        version_string
            .encode(&mut version_string_buffer, 0)
            .unwrap();
        let mut platform_version_buffer: Vec<u8> = vec![];
        platform_version
            .encode(&mut platform_version_buffer, 0)
            .unwrap();
        assert_eq!(version_string_buffer, platform_version_buffer);

        // Check round-trip encode/decode for PlatformVersion
        let mut decoded_platform_version = PlatformVersion::default();
        decoded_platform_version
            .decode(&mut (&*platform_version_buffer), 0)
            .unwrap();
        assert_eq!(platform_version, decoded_platform_version);
    }

    #[test]
    fn test_encode_decode_api_versions_response() {
        fn api_versions() -> ApiVersionsResponse {
            let version = semver::Version::parse("0.1.2-alpha.3+4567").unwrap();
            let platform_version = PlatformVersion::from(version);
            ApiVersionsResponse {
                error_code: ErrorCode::None,
                api_keys: vec![],
                platform_version,
            }
        }

        let api_version = api_versions();
        let mut api_versions_buffer: Vec<u8> = vec![];
        api_version.encode(&mut api_versions_buffer, 0).unwrap();

        let mut decoded_api_version = ApiVersionsResponse::default();
        decoded_api_version
            .decode(&mut (&*api_versions_buffer), 0)
            .unwrap();

        assert_eq!(api_version, decoded_api_version);
    }
}
