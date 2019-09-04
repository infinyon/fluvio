//!
//! # Spu Spec
//!
//! Spu Spec metadata information cached locally.
//!
use std::convert::TryFrom;
use std::io::Error as IoError;
use std::fmt;

use types::socket_helpers::EndPoint as SocketEndPoint;
use types::socket_helpers::EndPointEncryption;
use types::defaults::{SPU_PRIVATE_HOSTNAME, SPU_PRIVATE_PORT};
use types::defaults::{SPU_PUBLIC_HOSTNAME, SPU_PUBLIC_PORT};
use types::SpuId;
use types::socket_helpers::ServerAddress;

use kf_protocol::derive::{Decode, Encode};

use k8_metadata::spu::SpuSpec as K8SpuSpec;
use k8_metadata::spu::SpuType as K8SpuType;
use k8_metadata::spu::EncryptionEnum as K8EncryptionEnum;
use k8_metadata::spu::Endpoint as K8Endpoint;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub struct SpuSpec {
    pub id: SpuId,
    pub spu_type: SpuType,
    pub public_endpoint: Endpoint,
    pub private_endpoint: Endpoint,

    pub rack: Option<String>,
}

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub struct Endpoint {
    pub port: u16,
    pub host: String,
    pub encryption: EncryptionEnum,
}

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub enum EncryptionEnum {
    PLAINTEXT,
    SSL,
}

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub enum SpuType {
    Managed,
    Custom,
}

impl Default for SpuType {
    fn default() -> Self {
        SpuType::Managed
    }
}

impl From<K8SpuType> for SpuType {
    fn from(kv_spu_type: K8SpuType) -> Self {
        match kv_spu_type {
            K8SpuType::Managed => SpuType::Managed,
            K8SpuType::Custom => SpuType::Custom,
        }
    }
}

impl Into<K8SpuType> for SpuType {
    fn into(self) -> K8SpuType {
        match self {
            SpuType::Managed => K8SpuType::Managed,
            SpuType::Custom => K8SpuType::Custom,
        }
    }
}

// prob better way to do this
impl From<&SpuSpec> for SpuSpec {
    fn from(spec: &SpuSpec) -> Self {
        spec.clone()
    }
}

impl From<SpuId> for SpuSpec {
    fn from(spec: SpuId) -> Self {
        Self::new(spec)
    }
}

impl From<K8SpuSpec> for SpuSpec {
    fn from(kv_spec: K8SpuSpec) -> Self {
        // convert spu-type, defaults to Custom for none
        let spu_type = if let Some(kv_spu_type) = kv_spec.spu_type {
            kv_spu_type.into()
        } else {
            SpuType::Custom
        };

        // spu spec
        SpuSpec {
            id: kv_spec.spu_id,
            spu_type: spu_type,
            public_endpoint: Endpoint::new(&kv_spec.public_endpoint),
            private_endpoint: Endpoint::new(&kv_spec.private_endpoint),
            rack: kv_spec.rack.clone(),
        }
    }
}

impl From<SpuSpec> for K8SpuSpec {
    fn from(spec: SpuSpec) -> Self {
        K8SpuSpec {
            spu_id: spec.id,
            spu_type: Some(spec.spu_type.into()),
            public_endpoint: spec.public_endpoint.into(),
            private_endpoint: spec.private_endpoint.into(),
            rack: spec.rack,
        }
    }
}

impl Default for SpuSpec {
    fn default() -> Self {
        SpuSpec {
            id: -1,
            spu_type: SpuType::default(),
            public_endpoint: Endpoint {
                port: SPU_PUBLIC_PORT,
                host: SPU_PUBLIC_HOSTNAME.to_string(),
                encryption: EncryptionEnum::default(),
            },
            private_endpoint: Endpoint {
                port: SPU_PRIVATE_PORT,
                host: SPU_PRIVATE_HOSTNAME.to_string(),
                encryption: EncryptionEnum::default(),
            },
            rack: None,
        }
    }
}

impl SpuSpec {
    /// Given an Spu id generate a new SpuSpec
    pub fn new(id: SpuId) -> Self {
        let mut spec = Self::default();
        spec.id = id;
        spec
    }

    pub fn set_custom(mut self) -> Self {
        self.spu_type = SpuType::Custom;
        self
    }

    /// Return type label in String format
    pub fn type_label(&self) -> String {
        match self.spu_type {
            SpuType::Managed => "managed".to_owned(),
            SpuType::Custom => "custom".to_owned(),
        }
    }

    /// Return custom type: true for custom, false otherwise
    pub fn is_custom(&self) -> bool {
        match self.spu_type {
            SpuType::Managed => false,
            SpuType::Custom => true,
        }
    }

    pub fn private_server_address(&self) -> ServerAddress {
        let private_ep = &self.private_endpoint;
        ServerAddress {
            host: private_ep.host.clone(),
            port: private_ep.port,
        }
    }
}

// -----------------------------------
// Implementation - Endpoint
// -----------------------------------

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl TryFrom<&Endpoint> for SocketEndPoint {
    type Error = IoError;

    fn try_from(endpoint: &Endpoint) -> Result<Self, Self::Error> {
        types::socket_helpers::host_port_to_socket_addr(&endpoint.host, endpoint.port).map(|addr| {
            SocketEndPoint {
                addr,
                encryption: EndPointEncryption::PLAINTEXT,
            }
        })
    }
}

#[allow(dead_code)]
impl TryFrom<&Endpoint> for std::net::SocketAddr {
    type Error = IoError;

    fn try_from(endpoint: &Endpoint) -> Result<Self, Self::Error> {
        types::socket_helpers::host_port_to_socket_addr(&endpoint.host, endpoint.port)
    }
}

impl Into<K8Endpoint> for Endpoint {
    fn into(self) -> K8Endpoint {
        K8Endpoint {
            host: self.host.clone(),
            port: self.port,
            encryption: match self.encryption {
                EncryptionEnum::PLAINTEXT => K8EncryptionEnum::PLAINTEXT,
                EncryptionEnum::SSL => K8EncryptionEnum::SSL,
            },
        }
    }
}

impl Default for Endpoint {
    fn default() -> Self {
        Endpoint {
            host: "127.0.0.1".to_owned(),
            port: 0,
            encryption: EncryptionEnum::default(),
        }
    }
}

impl Endpoint {
    pub fn from_port_host(port: u16, host: &String) -> Self {
        Endpoint {
            port: port,
            host: host.clone(),
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }

    pub fn new(ep: &K8Endpoint) -> Endpoint {
        Endpoint {
            port: ep.port,
            host: ep.host.clone(),
            encryption: match ep.encryption {
                K8EncryptionEnum::PLAINTEXT => EncryptionEnum::PLAINTEXT,
                K8EncryptionEnum::SSL => EncryptionEnum::SSL,
            },
        }
    }
}

// -----------------------------------
// Implementation - EncryptionEnum
// -----------------------------------

impl Default for EncryptionEnum {
    fn default() -> Self {
        EncryptionEnum::PLAINTEXT
    }
}
