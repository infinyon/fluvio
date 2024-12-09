//
// minimal login token read module that just exposes a
// 'read_infinyon_token' function to read from the current login config
//
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json;

use fluvio_types::defaults::CLI_CONFIG_PATH;

const DEFAULT_LOGINS_DIR: &str = "logins"; // from logins.rs
const CURRENT_LOGIN_FILE_NAME: &str = "current";

type InfinyonToken = String;
type InfinyonRemote = String;

#[derive(Clone, thiserror::Error, Debug)]
pub enum InfinyonCredentialError {
    #[error("no org access token found, please login or switch to an org with 'fluvio cloud org switch'")]
    MissingOrgToken,

    #[error("{0}")]
    Read(String),

    #[error("unable to parse credentials")]
    UnableToParseCredentials,
}

#[derive(Clone)]
pub enum AccessToken {
    V3((InfinyonToken, InfinyonRemote)),
    V4(CliAccessTokens),
}

impl AccessToken {
    pub fn get_token(&self) -> Result<String, InfinyonCredentialError> {
        match self {
            AccessToken::V3((token, _remote)) => Ok(token.clone()),
            AccessToken::V4(token) => Ok(token.get_current_org_token()?),
        }
    }

    pub fn get_remote(&self) -> Result<String, InfinyonCredentialError> {
        match self {
            AccessToken::V3((_token, remote)) => Ok(remote.clone()),
            AccessToken::V4(cli_access_tokens) => Ok(cli_access_tokens.remote.clone()),
        }
    }
}

// multi-org access token output
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CliAccessTokens {
    pub remote: String,
    pub user_access_token: Option<String>,
    pub org_access_tokens: HashMap<String, String>,
}

impl CliAccessTokens {
    pub fn get_current_org_name(&self) -> Result<String, InfinyonCredentialError> {
        let key = self
            .org_access_tokens
            .keys()
            .next()
            .ok_or(InfinyonCredentialError::MissingOrgToken)?;
        Ok(key.to_owned())
    }
    pub fn get_current_org_token(&self) -> Result<String, InfinyonCredentialError> {
        let org = self.get_current_org_name()?;
        let tok = self
            .org_access_tokens
            .get(&org)
            .ok_or(InfinyonCredentialError::MissingOrgToken)?
            .to_owned();
        Ok(tok)
    }
}

/// replaces old read_infinyon_token
pub fn read_access_token() -> Result<AccessToken, InfinyonCredentialError> {
    // read token into cache once
    static TOKEN_CACHE: std::sync::OnceLock<Result<AccessToken, InfinyonCredentialError>> =
        std::sync::OnceLock::new();

    TOKEN_CACHE
        .get_or_init(|| {
            let token = read_access_token_impl();
            match token {
                Ok(AccessToken::V3(_)) => {
                    tracing::debug!("using v3 token");
                }
                Ok(AccessToken::V4(ref cli_access_token)) => {
                    tracing::debug!("using v4 token");
                    println!(
                        "Using org access: {}",
                        cli_access_token.get_current_org_name()?
                    );
                }
                Err(ref err) => {
                    tracing::debug!("failed to read token: {}", err);
                }
            }
            token
        })
        .clone()
}

// replaces old read_infinyon_token
fn read_access_token_impl() -> Result<AccessToken, InfinyonCredentialError> {
    if let Ok(cli_access_tokens) = read_infinyon_token_v4() {
        return Ok(AccessToken::V4(cli_access_tokens));
    }
    let pair = read_infinyon_token_v3()?;
    Ok(AccessToken::V3(pair))
}

pub fn read_infinyon_token_v4() -> Result<CliAccessTokens, InfinyonCredentialError> {
    const CLOUD_BIN: &str = "fluvio-cloud";
    const CLOUD_BIN_V4: &str = "fluvio-cloud-v4";
    let res = read_infinyon_token_v4_cli(CLOUD_BIN_V4);
    if res.is_err() {
        read_infinyon_token_v4_cli(CLOUD_BIN)
    } else {
        res
    }
}

fn read_infinyon_token_v4_cli(cloud_bin: &str) -> Result<CliAccessTokens, InfinyonCredentialError> {
    let mut cmd = std::process::Command::new(cloud_bin);
    cmd.arg("cli-access-tokens");
    cmd.env_remove("RUST_LOG"); // remove RUST_LOG to avoid debug output
    match cmd.output() {
        Ok(output) => {
            let output = String::from_utf8_lossy(&output.stdout);
            let cli_access_tokens: CliAccessTokens =
                serde_json::from_slice(output.as_bytes()).map_err(|e| {
                    tracing::debug!("failed to parse multi-org output: {}\n$ {cloud_bin} cli-access-tokens\n-->>{}<<--", e, output);
                    InfinyonCredentialError::UnableToParseCredentials
                })?;
            tracing::trace!("cli access tokens: {:#?}", cli_access_tokens);
            Ok(cli_access_tokens)
        }
        Err(e) => {
            tracing::debug!("failed to find multi-org login: {}", e);
            Err(InfinyonCredentialError::Read(
                "failed to find multi-org login".to_owned(),
            ))
        }
    }
}

// deprecated, will be removed after multi-org is stable
pub fn read_infinyon_token_v3() -> Result<(InfinyonToken, InfinyonRemote), InfinyonCredentialError>
{
    let cfgpath = default_file_path();
    // this will read the indirection file to resolve the profile
    let cred = Credentials::try_load(cfgpath)?;
    Ok((cred.token, cred.remote))
}

// read remote (older api)
pub fn read_infinyon_token_rem() -> Result<InfinyonRemote, InfinyonCredentialError> {
    let tok = read_access_token()?;
    tok.get_remote()
}

// read token (older api)
pub fn read_infinyon_token() -> Result<InfinyonToken, InfinyonCredentialError> {
    let access = read_access_token()?;
    access.get_token()
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
    use super::CliAccessTokens;
    use serde_json;

    // parse token options
    #[test]
    fn read_token_outputs() {
        let with_uat = r#"
        {
  "remote": "https://infinyon.cloud",
  "user_access_token": "uat_token",
  "org_access_tokens": {
    "inf-billing": "an_org_token"
  }
    }
        "#;

        let cli_access_tokens = serde_json::from_str::<CliAccessTokens>(with_uat);
        assert!(cli_access_tokens.is_ok(), "{:?} ", cli_access_tokens);
        let cli_access_tokens = cli_access_tokens.expect("should succeed");
        let org_token = cli_access_tokens
            .get_current_org_token()
            .expect("retreiving org token");
        assert_eq!(org_token, "an_org_token");
        assert_eq!(
            cli_access_tokens.user_access_token,
            Some("uat_token".to_string())
        );

        let no_uat = r#"
        {
  "remote": "https://infinyon.cloud",
  "org_access_tokens": {
    "inf-billing": "an_org_token"
  }
    }
        "#;
        let cli_access_tokens = serde_json::from_str::<CliAccessTokens>(no_uat);
        assert!(cli_access_tokens.is_ok(), "{:?} ", cli_access_tokens);
        let cli_access_tokens = cli_access_tokens.expect("should succeed");
        let org_token = cli_access_tokens
            .get_current_org_token()
            .expect("retreiving org token");
        assert_eq!(org_token, "an_org_token");
        assert_eq!(cli_access_tokens.user_access_token, None);
    }

    // load default credentials (ignore by default becasuse config is not populated in ci env)
    #[ignore]
    #[test]
    fn read_default() {
        let res_token = read_infinyon_token();
        assert!(res_token.is_ok(), "{res_token:?}");
        println!("token: {}", res_token.unwrap());
    }
}
