use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fs;
use std::path::{PathBuf, Path};
use thiserror::Error;

//use futures::io::Error;
use tracing::{warn, debug, trace, instrument};
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use toml::de::Error as TomlError;
use http_types::{Response, Request, StatusCode, Url};

use fluvio::FluvioConfig;
use fluvio_types::defaults::CLI_CONFIG_PATH;
use url::ParseError;
use super::http::execute;

const DEFAULT_AGENT_REMOTE: &str = "https://cloud.fluvio.io";

/// An Agent for authenticating with Fluvio Cloud
///
/// The LoginAgent is responsible for the following:
///
/// * Using a username and password to authenticate
///   and receive login credentials from Fluvio Cloud
/// * Saving credentials for later use
/// * Checking whether a saved token is valid or expired
///
#[derive(Debug)]
pub struct LoginAgent {
    /// The remote address to connect to Fluvio Cloud
    remote: String,
    /// Configured path for storing credentials in filesystem
    path: PathBuf,
    /// Active session either holds credentials or not
    session: Option<Credentials>,
}

impl LoginAgent {
    /// Creates a new LoginAgent with default configurations
    ///
    /// This may fail if the LoginAgent is unable to detect
    /// the user's home directory, as it would be unable to
    /// save session credentials in `~/.fluvio/login`.
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            remote: DEFAULT_AGENT_REMOTE.to_string(),
            path: path.into(),
            session: None,
        }
    }

    /// Creates a new LoginAgent using the given path to store login credentials
    pub fn with_default_path() -> Result<Self, IoError> {
        Ok(Self::new(Self::default_file_path()?))
    }

    /// Configure a custom remote
    pub fn with_remote<S: Into<String>>(mut self, remote: S) -> Self {
        let remote = remote.into();
        trace!(
            remote = &*remote,
            "LoginAgent configured with custom remote"
        );
        self.remote = remote;
        self
    }

    /// Return the default path where login is stored.
    ///
    /// Default path is `~/.fluvio/login`.
    ///
    /// This may fail if the home directory cannot be found.
    fn default_file_path() -> Result<PathBuf, IoError> {
        if let Some(mut login_path) = dirs::home_dir() {
            login_path.push(CLI_CONFIG_PATH);
            login_path.push("login");
            Ok(login_path)
        } else {
            Err(IoError::new(
                ErrorKind::InvalidInput,
                "can't get login directory",
            ))
        }
    }

    /// Attempts to use a saved session to download a Fluvio Cloud profile.
    ///
    /// Will fail if there is no saved session, or if the token
    /// in the saved session is expired.
    pub async fn download_profile(&mut self) -> Result<FluvioConfig, CloudError> {
        // Check whether we have credentials in session or on disk
        let creds = match self.session.as_ref() {
            // First, try to get the token from the agent session
            Some(creds) => {
                debug!("Using credentials from session");
                creds
            }
            None => {
                // If that doesn't work, try to get the token from disk
                let loaded_creds = Credentials::try_load(&self.path)?;
                self.session.replace(loaded_creds);
                self.session.as_ref().unwrap()
            }
        };

        let cluster_profile = self.try_download_profile(creds).await?;
        Ok(cluster_profile)
    }

    /// Attempts to download a Fluvio Cloud profile with the given credentials
    #[instrument(
        skip(self, creds),
        fields(
            remote = &*self.remote,
            path = "/api/v1/downloadProfile"
        )
    )]
    async fn try_download_profile(&self, creds: &Credentials) -> Result<FluvioConfig, CloudError> {
        let mut response = download_profile(&self.remote, creds).await?;
        trace!("Response: {:#?}", &response);
        debug!(status = response.status() as u16);

        match response.status() {
            StatusCode::Ok => {
                debug!("Successfully authenticated with token");
                let config: FluvioConfig = response.body_json().await?;
                Ok(config)
            }
            _ => {
                warn!("Failed to download profile");
                Err(CloudError::ProfileDownloadError)
            }
        }
    }

    /// Attempts to use a username and password to login to Fluvio Cloud.
    ///
    /// If this succeeds, the LoginAgent will save the Fluvio Cloud
    /// credentials in a session to be used later.
    #[allow(clippy::unit_arg)]
    #[instrument(
        skip(self, password),
        fields(
            remote = &*self.remote,
            path = "/api/v1/loginUser",
        ),
    )]
    pub async fn authenticate(
        &mut self,
        email: String,
        password: String,
    ) -> Result<(), CloudError> {
        let mut response = login_user(&self.remote, email.clone(), password).await?;

        match response.status() {
            StatusCode::Ok => {
                let creds = response.body_json::<Credentials>().await?;
                self.save_credentials(creds).await?;
                Ok(())
            }
            _ => {
                warn!("Failed to login");
                Err(CloudError::AuthenticationError(email))
            }
        }
    }

    /// Save the given Credentials to disk and in the LoginAgent's session.
    async fn save_credentials(&mut self, creds: Credentials) -> Result<(), CloudError> {
        // Save credentials to disk
        creds.try_save(&self.path)?;
        // Save credentials in agent session
        self.session.replace(creds);
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Credentials {
    id: String,
    token: String,
}

impl Credentials {
    /// Try to load credentials from disk
    fn try_load<P: AsRef<Path>>(path: P) -> Result<Self, CloudError> {
        let file_str = fs::read_to_string(path)
            .map_err(|source| CloudError::UnableToLoadCredentials { source })?;
        let creds: Credentials = toml::from_str(&*file_str)
            .map_err(|source| CloudError::UnableToParseCredentials { source })?;
        Ok(creds)
    }

    /// Try to save credentials to disk
    fn try_save<P: AsRef<Path>>(&self, path: P) -> Result<(), IoError> {
        let parent = path.as_ref().parent().ok_or_else(|| {
            IoError::new(ErrorKind::NotFound, "failed to open credentials folder")
        })?;
        fs::create_dir_all(parent)?;
        // Serializing self can never fail because Credentials: Serialize
        fs::write(path, toml::to_string(self).unwrap().as_bytes())
    }
}

#[derive(Debug, Serialize)]
struct LoginRequest {
    email: String,
    password: String,
}

async fn login_user(host: &str, email: String, password: String) -> Result<Response, CloudError> {
    let url = Url::parse(&format!("{}/api/v1/loginUser", host))?;
    let mut request = Request::post(url);
    let login = LoginRequest { email, password };

    // Always safe to serialize when Self: Serialize
    let body = serde_json::to_string(&login).unwrap();
    request.set_body(body);

    let response = execute(request).await?;
    Ok(response)
}

async fn download_profile(host: &str, creds: &Credentials) -> Result<Response, CloudError> {
    let url = Url::parse(&format!("{}/api/v1/downloadProfile", host))?;
    let mut request = Request::get(url);
    request.append_header("Authorization", &*creds.token);

    let response = execute(request).await?;
    Ok(response)
}

#[derive(Error, Debug)]
pub enum CloudError {
    /// Failed to download profile
    #[error("Failed to download cloud profile")]
    ProfileDownloadError,
    /// Failed to authenticate using the given username
    #[error("Failed to authenticate with username: {0}")]
    AuthenticationError(String),
    /// Failed to open Fluvio Cloud login file
    #[error("Failed to load cloud credentials")]
    UnableToLoadCredentials { source: IoError },
    /// Failed to parse Fluvio Cloud token
    #[error("Failed to parse login token from file")]
    UnableToParseCredentials { source: TomlError },
    /// Failed to make an http request
    #[error("Failed to make HTTP request to Fluvio cloud")]
    HttpError { source: HttpError },
    /// Failed to do some IO.
    #[error("IO error")]
    IoError {
        #[from]
        source: IoError,
    },
    /// Failed to deserialize JSON
    #[error("Failed to read JSON")]
    JsonError {
        #[from]
        source: JsonError,
    },
    /// Failed to parse request URL
    #[error("Failed to parse URL")]
    UrlError {
        #[from]
        source: ParseError,
    },
}

#[derive(Error, Debug)]
#[error("An HTTP error occurred: {inner}")]
pub struct HttpError {
    inner: http_types::Error,
}

impl From<http_types::Error> for CloudError {
    fn from(inner: http_types::Error) -> Self {
        Self::HttpError {
            source: HttpError { inner },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluvio_future::test_async;

    #[test_async]
    async fn test_save_credentials() -> Result<(), IoError> {
        let mut tmp = std::env::temp_dir();
        tmp.push("test_credentials");
        let creds = Credentials {
            id: "Johnny Appleseed".to_string(),
            token: "Token tokenson".to_string(),
        };
        let write_result = creds.try_save(&tmp);
        assert!(write_result.is_ok());

        let result = Credentials::try_load(tmp);
        let loaded_creds = result.unwrap();
        assert_eq!(creds, loaded_creds);
        Ok(())
    }

    #[test]
    fn test_custom_remote() -> Result<(), IoError> {
        let _agent = LoginAgent::with_default_path()?.with_remote("localhost:3030");
        Ok(())
    }
}
