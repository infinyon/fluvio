use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use surf::http::mime;
use surf::StatusCode;
use tracing::debug;

use crate::errors::Result;
use crate::HubUtilError;
use crate::keymgmt::Keypair;
use crate::{HUB_API_ACT, HUB_API_HUBID, HUB_REMOTE};

// in .fluvio/hub/hcurrent
const ACCESS_FILE_PTR: &str = "hcurrent";
const ACCESS_FILE_DEF: &str = "default"; // default profile name
const ACTION_DOWNLOAD: &str = "dl";
const ACTION_PUBLISH: &str = "pbl";
const ACTION_CREATE_HUBID: &str = "chid";

#[derive(Serialize, Deserialize)]
pub struct HubAccess {
    pub remote: String,      // remote host url
    pub authn_token: String, // cloud auth token
    pub hubid: String,       // hubid associated with the signing key
    pub pkgkey: String,      // package signing key (private)
    pub pubkey: String,      // package signing key (public)
}

impl HubAccess {
    pub fn new() -> Self {
        HubAccess {
            remote: HUB_REMOTE.to_string(),
            authn_token: String::new(),
            hubid: String::new(),
            pkgkey: String::new(),
            pubkey: String::new(),
        }
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
                println!("Hubid {hubid} created!");
            }
            StatusCode::Ok => {
                println!("Hubid {hubid} already set");
            }
            StatusCode::Forbidden => {
                let msg = format!("Hubid {hubid} already taken");
                return Err(HubUtilError::HubAccess(msg));
            }
            StatusCode::Unauthorized => {
                let msg = "Authorization error, try 'fluvio cloud login'".to_string();
                return Err(HubUtilError::HubAccess(msg));
            }
            sc => {
                let msg = format!("Hubid creation error {sc}");
                return Err(HubUtilError::HubAccess(msg));
            }
        }
        Ok(())
    }

    pub async fn get_download_token(&self) -> Result<String> {
        self.get_action_auth(ACTION_DOWNLOAD).await
    }

    pub async fn get_publish_token(&self) -> Result<String> {
        self.get_action_auth(ACTION_PUBLISH).await
    }

    async fn get_action_auth(&self, action: &str) -> Result<String> {
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
            .header("Authorization", &self.authn_token);
        let mut res = req
            .await
            .map_err(|e| HubUtilError::HubAccess(format!("Failed to connect {e}")))?;
        let status_code = res.status();
        match status_code {
            StatusCode::Ok => {
                let action_token = res.body_string().await.map_err(|_e| {
                    debug!("err {_e} {res:?}");
                    HubUtilError::HubAccess("Failed to parse reply".to_string())
                })?;
                Ok(action_token)
            }
            StatusCode::Unauthorized => Err(HubUtilError::HubAccess(
                "Unauthorized, please log in with 'fluvio cloud login'".into(),
            )),
            // surf::http::StatusCode::Forbidden
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
    ) -> Result<HubAccess> {
        let profile = if let Some(profile) = profile_in {
            profile
        } else {
            let profile_ptr_path = base_path.as_ref().join(ACCESS_FILE_PTR);
            if let Ok(profile) = std::fs::read_to_string(&profile_ptr_path.as_path()) {
                profile
            } else {
                // if the ptr file doesn't exist, then assume we need to create the default config
                let deferr = Err(HubUtilError::HubAccess(
                    "Couldn't create default hubaccess credentials".to_string(),
                ));
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
        debug!("profile_path: {profile_path:?}");
        let buf = std::fs::read_to_string(&profile_path)?;
        let mut ha: HubAccess = serde_yaml::from_str(&buf).map_err(|_e| {
            let spath = profile_path.display();
            HubUtilError::HubAccess(format!("Could not load from {spath}"))
        })?;

        // generate keys if they don't exist
        if !ha.have_pkgkey() {
            ha.gen_pkgkey()?;
            ha.write_hash(base_path)?;
        }
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
        hash.update(&self.remote);
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
