use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fs;
use std::path::{PathBuf, Path};

use tracing::{warn, debug, trace, instrument};
use serde::{Deserialize, Serialize};
use flv_types::defaults::CLI_CONFIG_PATH;
use surf::http_types::StatusCode;
use surf::Error as SurfError;
use serde_json::Error as JsonError;

use serde::export::Formatter;
use futures::io::Error;
use fluvio::config::Cluster;

const DEFAULT_AGENT_REMOTE: &str = "cloud.fluvio.io";

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
    pub async fn download_profile(&mut self) -> Result<Cluster, CloudError> {
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
    async fn try_download_profile(&self, creds: &Credentials) -> Result<Cluster, CloudError> {
        let mut response = surf::get(format!("{}/api/v1/downloadProfile", &self.remote))
            .set_header("Authorization", &*creds.token)
            .await?;
        trace!("Response: {:#?}", &response);
        debug!(status = response.status() as u16);

        match response.status() {
            StatusCode::Ok => {
                debug!("Successfully authenticated with token");
                let cluster: Cluster = response.body_json().await?;
                Ok(cluster)
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
        let login = LoginRequest {
            email: email.clone(),
            password,
        };
        let mut response = surf::post(format!("{}/api/v1/loginUser", &self.remote))
            .body_json(&login)?
            .await?;

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

#[derive(Debug, Serialize)]
struct LoginRequest {
    email: String,
    password: String,
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
        let file_str = fs::read_to_string(path).map_err(CloudError::UnableToLoadCredentials)?;
        let creds: Credentials =
            toml::from_str(&*file_str).map_err(CloudError::UnableToParseCredentials)?;
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

#[derive(Debug)]
pub enum CloudError {
    /// Failed to download profile
    ProfileDownloadError,
    /// Failed to authenticate using the given username
    AuthenticationError(String),
    /// Failed to open Fluvio Cloud login file
    UnableToLoadCredentials(IoError),
    /// Failed to parse Fluvio Cloud token
    UnableToParseCredentials(toml::de::Error),
    /// Failed to make an http request
    HttpError(SurfError),
    /// Failed to do some IO.
    IoError(IoError),
    /// Failed to deserialize JSON
    JsonError(JsonError),
}

impl std::fmt::Display for CloudError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProfileDownloadError => write!(f, "Failed to download profile"),
            Self::AuthenticationError(email) => write!(f, "Failed to login with email {}", email),
            Self::UnableToLoadCredentials(e) => write!(f, "Failed to open login file: {}", e),
            Self::UnableToParseCredentials(e) => {
                write!(f, "Failed to read credentials toml: {}", e)
            }
            Self::HttpError(surf) => write!(f, "Failed to make http request: {}", surf),
            Self::IoError(e) => write!(f, "Io Error: {}", e),
            Self::JsonError(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<SurfError> for CloudError {
    fn from(error: SurfError) -> Self {
        Self::HttpError(error)
    }
}

impl From<IoError> for CloudError {
    fn from(error: Error) -> Self {
        Self::IoError(error)
    }
}

impl From<JsonError> for CloudError {
    fn from(error: JsonError) -> Self {
        Self::JsonError(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flv_future_aio::test_async;

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
