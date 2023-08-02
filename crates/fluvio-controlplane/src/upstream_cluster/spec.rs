use core::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Clone, PartialEq, Eq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct UpstreamClusterSpec {
    pub source_id: String,
    pub target: UpstreamTarget,
    pub key_pair: UpstreamKeyPair,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct UpstreamTarget {
    pub endpoint: String,
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub tls: Option<ClientTls>,
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

impl fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ClientTls")
            .field("domain", &self.domain)
            .finish()
    }
}

#[derive(Decoder, Default, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct UpstreamKeyPair {
    pub public_key: String,
}
