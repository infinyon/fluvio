//
// minimal login token read module that just exposes a
// 'read_infinyon_token' function to read from the current login config
//
use std::env;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::debug;

use fluvio_types::defaults::CLI_CONFIG_PATH;

const INFINYON_CONFIG_PATH_ENV: &str = "INFINYON_CONFIG_PATH";
const DEFAULT_LOGINS_DIR: &str = "logins"; // from logins.rs
const CURRENT_LOGIN_FILE_NAME: &str = "current";

type InfinyonToken = String;

#[derive(thiserror::Error, Debug)]
pub enum InfinyonCredentialError {
    #[error("Read error {0}")]
    Read(String),

    #[error("unable to parse credentials")]
    UnableToParseCredentials,
}

pub fn read_infinyon_token() -> Result<InfinyonToken, InfinyonCredentialError> {
    // the ENV variable should point directly to the applicable profile
    if let Ok(profilepath) = env::var(INFINYON_CONFIG_PATH_ENV) {
        let cred = Credentials::load(Path::new(&profilepath))?;
        debug!("{INFINYON_CONFIG_PATH_ENV} {profilepath} loaded");
        return Ok(cred.token);
    }
    let cfgpath = default_file_path();
    // this will read the indirection file to resolve the profile
    let cred = Credentials::try_load(&cfgpath)?;
    Ok(cred.token)
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Credentials {
    remote: String,
    email: String,
    id: String,
    token: String,
}

impl Credentials {
    /// Try to load credentials from disk
    fn try_load<P: AsRef<Path>>(base_path: P) -> Result<Self, InfinyonCredentialError> {
        let current_login_path = base_path.as_ref().join(CURRENT_LOGIN_FILE_NAME);
        let cfg_path = fs::read_to_string(&current_login_path).map_err(|_| {
            let strpath = current_login_path.to_string_lossy().to_string();
            InfinyonCredentialError::Read(strpath)
        })?;
        let cred_path = base_path.as_ref().join(cfg_path);
        Self::load(&cred_path)
    }

    fn load(cred_path: &Path) -> Result<Self, InfinyonCredentialError> {
        let file_str = fs::read_to_string(cred_path).map_err(|_| {
            let strpath = cred_path.to_string_lossy().to_string();
            InfinyonCredentialError::Read(strpath)
        })?;
        let creds: Credentials = toml::from_str(&file_str)
            .map_err(|_| InfinyonCredentialError::UnableToParseCredentials)?;
        Ok(creds)
    }
}

fn default_file_path() -> String {
    let mut login_path = dirs::home_dir().unwrap_or_default();
    login_path.push(CLI_CONFIG_PATH);
    login_path.push(DEFAULT_LOGINS_DIR);
    login_path.to_string_lossy().to_string()
}

#[cfg(test)]
mod infinyon_tok_tests {
    use super::read_infinyon_token;

    // load default credentials (ignore by default becasuse config is not populated in ci env)
    #[ignore]
    #[test]
    fn read_default() {
        let token = read_infinyon_token();
        assert!(token.is_ok());
        println!("token: {}", token.unwrap());
    }
}
