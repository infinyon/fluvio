use std::io::{Error as IoError, ErrorKind};
use fluvio_protocol::{Encoder, Version, Decoder};
use fluvio_protocol::bytes::{BufMut, Buf};

use crate::ErrorCode;
use crate::api::Request;
use crate::apis::AdminPublicApiKey;
use crate::derive::{Decode, Encode};

// -----------------------------------
// ApiVersionsRequest
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionsRequest {}

impl Request for ApiVersionsRequest {
    const API_KEY: u16 = AdminPublicApiKey::ApiVersion as u16;
    type Response = ApiVersionsResponse;
}

// -----------------------------------
// ApiVersionsResponse
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionsResponse {
    pub error_code: ErrorCode,
    pub api_keys: Vec<ApiVersionKey>,
    pub platform_version: String,
}

pub type ApiVersions = Vec<ApiVersionKey>;

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug)]
pub struct PlatformVersion(String);

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
        where T: Buf
    {
        // Decode the data as a String
        let mut string = String::default();
        string.decode(src, version)?;

        // Before constructing, ensure that what was decoded is valid Semver
        let _version = semver::Version::parse(&string)
            .map_err(|_| IoError::new(ErrorKind::InvalidData, "PlatformVersion is not valid semver"))?;

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
        where T: BufMut
    {
        self.0.encode(dest, version)
    }
}
