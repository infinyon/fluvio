use anyhow::{anyhow, Result};

use serde::{Serialize, Deserialize};
use fluvio_controlplane::remote_cluster::RemoteClusterSpec;
use fluvio_controlplane::remote_cluster::{KeyPair, RemoteClusterType};
use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::link::ErrorCode;

pub mod remote;
pub mod upstream;

/// API call from client to SPU
#[repr(u16)]
#[derive(Encoder, Decoder, Eq, PartialEq, Debug, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum CloudApiKey {
    ApiVersion = 1, // version api key
    Object = 2000,
    Register = 2001,
    Delete = 2002,
    List = 2003,

    Request = 2100,
    ReqRegister = 2101,
    ReqDelete = 2102,
    ReqList = 2103,
}

impl Default for CloudApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}

pub trait CloudRemoteClusterSpec: Encoder + Decoder {}

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CloudStatus {
    pub name: String,
    #[serde(skip)]
    pub error_code: ErrorCode,
    pub error_message: Option<String>,
    pub list: Option<Vec<ListItem>>,
}

impl std::fmt::Display for CloudStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Status<{}>", self.name)
    }
}

impl CloudStatus {
    pub fn new(name: String, error_code: ErrorCode, error_message: Option<String>) -> Self {
        CloudStatus {
            name,
            error_code,
            error_message,
            list: None,
        }
    }

    pub fn new_ok(okmsg: &str) -> Self {
        CloudStatus {
            name: okmsg.to_string(),
            error_code: ErrorCode::None,
            error_message: None,
            list: None,
        }
    }
}

impl fluvio_controlplane_metadata::core::Status for CloudStatus {}

#[derive(Encoder, Decoder, Default, Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct ListItem {
    pub name: String,
    pub remote_type: String,
    pub pairing: String,
    pub status: String,
    pub last: String,
}

pub fn validate_req(rs_type_str: &str) -> Result<RemoteClusterSpec> {
    let remote_type = match rs_type_str {
        "mirror-edge" => RemoteClusterType::MirrorEdge,
        _ => {
            return Err(anyhow!("bad remote cluster type"));
        }
    };

    // key_pair todo
    let key_pair = KeyPair {
        public_key: "".into(),
        private_key: "".into(),
    };
    let spec = RemoteClusterSpec {
        remote_type,
        key_pair,
    };

    Ok(spec)
}
