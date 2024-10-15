//
// minimal login token read module that just exposes a
// 'read_infinyon_token' function to read from the current login config
//
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json;
use tracing::debug;

use fluvio_types::defaults::CLI_CONFIG_PATH;

const INFINYON_CONFIG_PATH_ENV: &str = "INFINYON_CONFIG_PATH";
const DEFAULT_LOGINS_DIR: &str = "logins"; // from logins.rs
const CURRENT_LOGIN_FILE_NAME: &str = "current";

type InfinyonToken = String;
type InfinyonRemote = String;

#[derive(thiserror::Error, Debug)]
pub enum InfinyonCredentialError {
    #[error("{0}")]
    Read(String),

    #[error("unable to parse credentials")]
    UnableToParseCredentials,
}

// multi-org access token output
#[derive(Debug, Serialize, Deserialize)]
pub struct CliAccessTokens {
    pub remote: String,
    pub user_access_token: String,
    pub org_access_tokens: HashMap<String, String>,
}

pub fn read_access_tokens() -> Result<CliAccessTokens, InfinyonCredentialError> {
    const LOGIN_BIN: &str = "fluvio-cloud-v4";

    let mut cmd = std::process::Command::new(LOGIN_BIN);
    cmd.arg("cli-access-tokens");
    match cmd.output() {
        Ok(output) => {
            let cli_access_tokens: CliAccessTokens =
                serde_json::from_slice(&output.stdout).unwrap();
            tracing::trace!("cli access tokens: {:#?}", cli_access_tokens);
            Ok(cli_access_tokens)
        }
        Err(e) => {
            tracing::debug!("failed to execute v4: {}", e);
            Err(InfinyonCredentialError::Read(
                "failed to execute v4".to_owned(),
            ))
        }
    }
}

pub fn read_infinyon_token() -> Result<InfinyonToken, InfinyonCredentialError> {
    match read_access_tokens() {
        Ok(cli_access_tokens) => {
            let tok = cli_access_tokens.get_current_org_token();
            return Ok(tok);
        }
        Err(_e) => {
            // fallback to old token logic
        }
    };
    read_infinyon_token_v3()
}

impl CliAccessTokens {
    pub fn get_current_org_token(&self) -> String {
        let key = self.org_access_tokens.keys().next().unwrap_or_else(|| {
            panic!("no org access token found, please login or switch to an org with 'fluvio cloud org switch'");
        });
        let tok = if let Some(tok) = self.org_access_tokens.get(key) {
            tok.to_owned()
        } else {
            String::new()
        };
        tok
    }
}

// depcreated, will be removed after multi-org is stable
pub fn read_infinyon_token_v3() -> Result<InfinyonToken, InfinyonCredentialError> {
    let cfgpath = default_file_path();
    // this will read the indirection file to resolve the profile
    let cred = Credentials::try_load(cfgpath)?;
    Ok(cred.token)
}

pub fn read_infinyon_token_rem() -> Result<(InfinyonToken, InfinyonRemote), InfinyonCredentialError>
{
    // the ENV variable should point directly to the applicable profile
    if let Ok(profilepath) = env::var(INFINYON_CONFIG_PATH_ENV) {
        let cred = Credentials::load(Path::new(&profilepath))?;
        debug!(
            path = profilepath,
            "profile loaded from INFINYON_CONFIG_PATH_ENV"
        );
        return Ok((cred.token, cred.remote));
    }
    let cfgpath = default_file_path();
    // this will read the indirection file to resolve the profile
    let cred = Credentials::try_load(cfgpath)?;
    Ok((cred.token, cred.remote))
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
        let cfg_path = fs::read_to_string(current_login_path).map_err(|_| {
            InfinyonCredentialError::Read(
                "no access credentials, try 'fluvio cloud login'".to_owned(),
            )
        })?;
        let cred_path = base_path.as_ref().join(cfg_path);
        Self::load(&cred_path)
    }

    fn load(cred_path: &Path) -> Result<Self, InfinyonCredentialError> {
        let file_str = fs::read_to_string(cred_path).map_err(|_| {
            InfinyonCredentialError::Read(
                "no access credentials, try 'fluvio cloud login'".to_owned(),
            )
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
        let res_token = read_infinyon_token();
        assert!(res_token.is_ok(), "{res_token:?}");
        println!("token: {}", res_token.unwrap());
    }
}
