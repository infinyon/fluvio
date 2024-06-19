use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Clone, PartialEq, Eq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct MirrorSpec {
    pub mirror_type: MirrorType,
    // TODO: we should add auth
}

impl MirrorSpec {
    pub fn type_name(&self) -> &str {
        match &self.mirror_type {
            MirrorType::Remote(_) => "remote",
            MirrorType::Home(_) => "home",
        }
    }
}

impl fmt::Display for MirrorSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Mirror: {:?}", self)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MirrorType {
    #[cfg_attr(feature = "use_serde", serde(rename = "remote"))]
    #[fluvio(tag = 0)]
    Remote(Remote),
    #[cfg_attr(feature = "use_serde", serde(rename = "home"))]
    #[fluvio(tag = 1)]
    Home(Home),
}

impl fmt::Display for MirrorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts = match self {
            MirrorType::Remote(remote) => format!("remote: {}", remote.id),
            MirrorType::Home(home) => format!("home: {}", home.id),
        };
        write!(f, "{}", ts)
    }
}

impl Default for MirrorType {
    fn default() -> Self {
        Self::Remote(Remote::default())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Remote {
    pub id: String,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Home {
    pub id: String,
    pub remote_id: String,
    pub public_endpoint: String,
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub client_tls: Option<ClientTls>,
}

#[derive(Clone, PartialEq, Eq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ClientTls {
    pub domain: String,
    pub ca_cert: String,
    pub client_cert: String,
    pub client_key: String,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientTls: {{ domain: {} }}", self.domain)
    }
}
