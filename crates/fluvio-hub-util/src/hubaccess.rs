use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use surf::http::mime;
use surf::StatusCode;
use tracing::{debug, info};

use fluvio_types::defaults::CLI_CONFIG_PATH;

use crate::errors::Result;
use crate::{HubUtilError, read_infinyon_token};
use crate::keymgmt::Keypair;
use crate::{HUB_API_ACT, HUB_API_HUBID, HUB_REMOTE, CLI_CONFIG_HUB};

// in .fluvio/hub/hcurrent
const ACCESS_FILE_PTR: &str = "hcurrent";
const ACCESS_FILE_DEF: &str = "default"; // default profile name

pub const ACTION_LIST: &str = "list";
pub const ACTION_LIST_WITH_META: &str = "lwm";
pub const ACTION_CREATE_HUBID: &str = "chid";
pub const ACTION_DOWNLOAD: &str = "dl";
pub const ACTION_PUBLISH: &str = "pbl";

const INFINYON_HUB_REMOTE: &str = "INFINYON_HUB_REMOTE";
const FLUVIO_HUB_PROFILE_ENV: &str = "FLUVIO_HUB_PROFILE";

#[derive(Serialize, Deserialize)]
pub struct HubAccess {
    #[serde(skip)]
    pub remote: String, // remote host url (deprecated for config file)
    pub hubid: String,  // hubid associated with the signing key
    pub pkgkey: String, // package signing key (private)
    pub pubkey: String, // package signing key (public)
}

impl HubAccess {
    pub fn new() -> Self {
        HubAccess {
            remote: HUB_REMOTE.to_string(),
            hubid: String::new(),
            pkgkey: String::new(),
            pubkey: String::new(),
        }
    }

    pub fn default_load(remote: &Option<String>) -> Result<Self> {
        let cfgpath = default_cfg_path()?;
        let profileopt = std::env::var(FLUVIO_HUB_PROFILE_ENV).ok();
        HubAccess::load_path(&cfgpath, profileopt, remote)
    }

    pub async fn create_hubid(&self, hubid: &str) -> Result<()> {
        let action_token = self.get_action_auth(ACTION_CREATE_HUBID).await?;
        let msg = MsgHubIdReq {
            hubid: hubid.to_string(),
            pubkey: self.pubkey.clone(),
        };
        let msg_json = serde_json::to_string(&msg)?;
        let host = &self.remote;
        let api_url = format!("{host}/{HUB_API_HUBID}");
        debug!("Sending Action token {action_token}");
        let req = surf::put(api_url)
            .content_type(mime::JSON)
            .body_bytes(msg_json)
            .header("Authorization", &action_token);
        let res = req
            .await
            .map_err(|e| HubUtilError::HubAccess(format!("Failed to connect {e}")))?;
        let status = res.status();
        match status {
            StatusCode::Created => {
                println!("hub: hubid {hubid} created");
            }
            StatusCode::Ok => {
                println!("hub: hubid {hubid} is set");
            }
            StatusCode::Forbidden => {
                let msg = format!("hub: hubid {hubid} already taken");
                return Err(HubUtilError::HubAccess(msg));
            }
            StatusCode::Unauthorized => {
                let msg = "hub: authorization error, try 'fluvio cloud login'".to_string();
                return Err(HubUtilError::HubAccess(msg));
            }
            sc => {
                let msg = format!("hub: hubid creation error {sc}");
                return Err(HubUtilError::HubAccess(msg));
            }
        }
        Ok(())
    }

    pub async fn get_download_token(&self) -> Result<String> {
        self.get_action_auth(ACTION_DOWNLOAD).await
    }

    pub async fn get_list_token(&self) -> Result<String> {
        self.get_action_auth(ACTION_LIST).await
    }

    pub async fn get_list_with_meta_token(&self) -> Result<String> {
        self.get_action_auth(ACTION_LIST_WITH_META).await
    }

    pub async fn get_publish_token(&self) -> Result<String> {
        self.get_action_auth(ACTION_PUBLISH).await
    }

    pub async fn get_action_auth_with_token(
        &self,
        action: &str,
        authn_token: &str,
    ) -> Result<String> {
        self.make_action_token(action, authn_token.into()).await
    }

    async fn get_action_auth(&self, action: &str) -> Result<String> {
        self.make_action_token(action, read_infinyon_token()?).await
    }

    async fn make_action_token(&self, action: &str, authn_token: String) -> Result<String> {
        let host = &self.remote;
        let api_url = format!("{host}/{HUB_API_ACT}");
        let mat = MsgActionToken {
            act: String::from(action),
        };
        let msg_action_token = serde_json::to_string(&mat)
            .map_err(|_e| HubUtilError::HubAccess("Failed access setup".to_string()))?;
        let req = surf::get(api_url)
            .content_type(mime::JSON)
            .body_bytes(msg_action_token)
            .header("Authorization", &authn_token);
        let mut res = req
            .await
            .map_err(|e| HubUtilError::HubAccess(format!("Failed to connect {e}")))?;
        let status_code = res.status();
        match status_code {
            StatusCode::Ok => {
                let action_token = res.body_string().await.map_err(|e| {
                    debug!("err {e} {res:?}");
                    HubUtilError::HubAccess("Failed to parse reply".to_string())
                })?;
                Ok(action_token)
            }
            StatusCode::Unauthorized => Err(HubUtilError::HubAccess(
                "Unauthorized, please log in with 'fluvio cloud login'".into(),
            )),
            _ => {
                let msg = format!("Unknown error: {}", res.status());
                Err(HubUtilError::HubAccess(msg))
            }
        }
    }

    pub fn have_pkgkey(&self) -> bool {
        !self.pkgkey.is_empty()
    }

    // generate a package signing key. It's recommended
    // to save the package key after generating it.
    pub fn gen_pkgkey(&mut self) -> Result<()> {
        let kp = Keypair::new()?;
        let pubkey = kp.public();
        self.pkgkey = kp.to_hex();
        self.pubkey = pubkey.to_hex();
        Ok(())
    }

    pub fn load_path<P: AsRef<Path>>(
        base_path: P,
        profile_in: Option<String>,
        remote_url: &Option<String>,
    ) -> Result<HubAccess> {
        let profile = if let Some(profile) = profile_in {
            profile
        } else {
            let profile_ptr_path = base_path.as_ref().join(ACCESS_FILE_PTR);
            if let Ok(profile) = std::fs::read_to_string(profile_ptr_path.as_path()) {
                profile
            } else {
                info!("Creating initial hub credentials");
                // if the ptr file doesn't exist, then assume we need to create the default config
                let deferr = Err(HubUtilError::HubAccess(
                    "Couldn't create default hubaccess credentials".to_string(),
                ));
                std::fs::create_dir(&base_path)?;
                let mut blank_access = HubAccess::new();
                if blank_access.gen_pkgkey().is_err() {
                    return deferr;
                }
                let def_path = base_path.as_ref().join(ACCESS_FILE_DEF);

                blank_access.write_file(def_path)?;
                if write_ptr_file(&base_path, ACCESS_FILE_DEF).is_err() {
                    return deferr;
                }
                String::from(ACCESS_FILE_DEF)
            }
        };
        let profile_path = base_path.as_ref().join(profile);
        info!("loading hub profile {profile_path:?}");
        let buf = std::fs::read_to_string(&profile_path)?;
        let mut ha: HubAccess = serde_yaml::from_str(&buf).map_err(|e| {
            let spath = profile_path.display();
            debug!("parse error {e}");
            HubUtilError::HubAccess(format!("Could not load from {spath}"))
        })?;

        // generate keys if they don't exist
        if !ha.have_pkgkey() {
            ha.gen_pkgkey()?;
            ha.write_hash(base_path)?;
        }

        ha.remote = if let Some(rurl) = remote_url {
            // remote url from flag
            info!("using remote={rurl}");
            rurl.to_string()
        } else if let Ok(envurl) = std::env::var(INFINYON_HUB_REMOTE) {
            info!("using {INFINYON_HUB_REMOTE}={envurl}");
            envurl
        } else {
            HUB_REMOTE.to_string()
        };

        Ok(ha)
    }

    pub fn write_file<P: AsRef<Path>>(&self, fname: P) -> Result<()> {
        debug!("writing HubAccess data to {}", fname.as_ref().display());
        let buf = serde_yaml::to_string(self)?;
        std::fs::write(fname, &buf)?;
        Ok(())
    }

    pub fn write_hash<P: AsRef<Path>>(&self, base_path: P) -> Result<()> {
        let mut hash = Sha512::new();
        hash.update(&self.hubid);
        hash.update(&self.pubkey);
        let hname = hex::encode(hash.finalize());

        let outpath = base_path.as_ref().join(&hname);
        self.write_file(outpath)?;
        write_ptr_file(base_path, &hname)?;
        Ok(())
    }

    /// return signing keypair
    pub fn keypair(&self) -> Result<Keypair> {
        Keypair::from_hex(&self.pkgkey)
    }
}

fn write_ptr_file<P: AsRef<Path>>(base_path: P, profile: &str) -> Result<()> {
    let ptr_path = base_path.as_ref().join(ACCESS_FILE_PTR);
    std::fs::write(ptr_path, profile)?;
    Ok(())
}

impl Default for HubAccess {
    fn default() -> Self {
        Self::new()
    }
}

/// used by this crate and server to exchange tokens
#[derive(Serialize, Deserialize)]
pub struct MsgActionToken {
    // action (client -> server)
    pub act: String,
}

#[derive(Serialize, Deserialize)]
pub struct MsgHubIdReq {
    pub hubid: String,
    pub pubkey: String,
}

pub fn default_cfg_path() -> Result<PathBuf> {
    let mut hub_cfg_path =
        dirs::home_dir().ok_or_else(|| HubUtilError::HubAccess("no home directory".into()))?;
    hub_cfg_path.push(CLI_CONFIG_PATH); // .fluvio
    hub_cfg_path.push(CLI_CONFIG_HUB);
    Ok(hub_cfg_path)
}
