//!
//! # Spu Spec
//!
//! Spu Spec metadata information cached locally.
//!
use std::convert::TryFrom;
use std::io::Error as IoError;
use std::fmt;

use flv_util::socket_helpers::EndPoint as SocketEndPoint;
use flv_util::socket_helpers::EndPointEncryption;
use flv_types::defaults::{SPU_PRIVATE_HOSTNAME, SPU_PRIVATE_PORT};
use flv_types::defaults::SPU_PUBLIC_PORT;
use flv_types::SpuId;
use flv_util::socket_helpers::ServerAddress;

use kf_protocol::derive::{Decode, Encode};

use k8_metadata::spu::SpuSpec as K8SpuSpec;
use k8_metadata::spu::SpuType as K8SpuType;
use k8_metadata::spu::EncryptionEnum as K8EncryptionEnum;
use k8_metadata::spu::Endpoint as K8Endpoint;
use k8_metadata::spu::IngressPort as K8IngressPort;
use k8_metadata::spu::IngressAddr as K8IngressAddr;



// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub struct SpuSpec {
    pub id: SpuId,
    pub spu_type: SpuType,
    pub public_endpoint: IngressPort,
    pub private_endpoint: Endpoint,
    pub rack: Option<String>,
}


impl Default for SpuSpec {
    fn default() -> Self {
        SpuSpec {
            id: -1,
            spu_type: SpuType::default(),
            public_endpoint: IngressPort {
                port: SPU_PUBLIC_PORT,
                ..Default::default()
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
            public_endpoint: kv_spec.public_endpoint.into(),
            private_endpoint: kv_spec.private_endpoint.into(),
            rack: kv_spec.rack.clone(),
        }
    }
}


impl Into<K8SpuSpec> for SpuSpec {
    fn into(self) -> K8SpuSpec {
        K8SpuSpec {
            spu_id: self.id,
            spu_type: Some(self.spu_type.into()),
            public_endpoint: self.public_endpoint.into(),
            private_endpoint: self.private_endpoint.into(),
            rack: self.rack,
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





#[derive(Decode, Encode, Default, Debug, Clone, PartialEq)]
pub struct IngressPort {
    pub port: u16,
    pub ingress: Vec<IngressAddr>,
    pub encryption: EncryptionEnum,
}

impl fmt::Display for IngressPort {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        write!(f,"{}:{}",self.host_string(),self.port)
    }

}

impl From<K8IngressPort> for IngressPort {
    fn from(ingress_port: K8IngressPort) -> Self {
        Self {
            port: ingress_port.port,
            ingress: ingress_port.ingress.into_iter().map(|a| a.into()).collect(),
            encryption: ingress_port.encryption.into()
        }
    }
}

impl Into<K8IngressPort> for IngressPort {
    fn into(self) -> K8IngressPort {
        K8IngressPort {
            port: self.port,
            ingress: self.ingress.into_iter().map(|a| a.into()).collect(),
            encryption: self.encryption.into()
        }
    }
}


impl IngressPort {
    pub fn from_port_host(port: u16, host: String) -> Self {
        Self {
            port: port,
            ingress: vec![IngressAddr {
                hostname: Some(host),
                ip: None
            }],
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }

    // return any host whether it is IP or String
    pub fn host(&self) -> Option<String> {
        if self.ingress.len() == 0 {
            None
        } else {
            self.ingress[0].host()
        }
    }

    pub fn host_string(&self) -> String {
        match self.host() {
            Some(host_val) => host_val,
            None => "".to_owned()
        }
    }

}




#[derive(Decode, Encode, Default,Debug, Clone, PartialEq)]
pub struct IngressAddr {
    pub hostname: Option<String>,
    pub ip: Option<String>
}

impl IngressAddr {
    pub fn host(&self) -> Option<String>  {
        if let Some(name) = &self.hostname {
            Some(name.clone())
        } else {
            if let Some(ip) = &self.ip {
                Some(ip.clone())
            } else {
                None
            }
        }
    }
}

impl From<K8IngressAddr> for IngressAddr {
    fn from(addr: K8IngressAddr) -> Self {
        Self {
            hostname: addr.hostname,
            ip: addr.ip
        }
    }
}

impl Into<K8IngressAddr> for IngressAddr {
    fn into(self) -> K8IngressAddr {
        K8IngressAddr {
            hostname: self.hostname,
            ip: self.ip
        }
    }
}

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub struct Endpoint {
    pub port: u16,
    pub host: String,
    pub encryption: EncryptionEnum,
}

impl From<K8Endpoint> for Endpoint {
    fn from(pt: K8Endpoint) -> Self {
        Self {
            port: pt.port,
            host: pt.host,
            encryption: pt.encryption.into()
        }
    }
}

impl Into<K8Endpoint> for Endpoint {
    fn into(self) -> K8Endpoint {
        K8Endpoint {
            port: self.port,
            host: self.host,
            encryption: self.encryption.into()
        }
    }
}



impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}


impl TryFrom<&Endpoint> for SocketEndPoint {
    type Error = IoError;

    fn try_from(endpoint: &Endpoint) -> Result<Self, Self::Error> {
        flv_util::socket_helpers::host_port_to_socket_addr(&endpoint.host, endpoint.port).map(|addr| {
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
        flv_util::socket_helpers::host_port_to_socket_addr(&endpoint.host, endpoint.port)
    }
}

impl Default for Endpoint {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 0,
            encryption: EncryptionEnum::default(),
        }
    }
}

impl Endpoint {
    pub fn from_port_host(port: u16, host: String) -> Self {
        Self {
            port: port,
            host: host,
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }

    pub fn new(ep: K8Endpoint) -> Self {
        Self {
            port: ep.port,
            host: ep.host,
            encryption: match ep.encryption {
                K8EncryptionEnum::PLAINTEXT => EncryptionEnum::PLAINTEXT,
                K8EncryptionEnum::SSL => EncryptionEnum::SSL,
            },
        }
    }
}





#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub enum EncryptionEnum {
    PLAINTEXT,
    SSL,
}


impl Default for EncryptionEnum {
    fn default() -> Self {
        EncryptionEnum::PLAINTEXT
    }
}

impl From<K8EncryptionEnum> for EncryptionEnum {
    fn from(enc: K8EncryptionEnum) -> Self {
        match enc {
            K8EncryptionEnum::PLAINTEXT => Self::PLAINTEXT,
            K8EncryptionEnum::SSL => Self::SSL
        }
    }
}

impl Into<K8EncryptionEnum> for EncryptionEnum {
    fn into(self) -> K8EncryptionEnum {
        match self {
            Self::PLAINTEXT => K8EncryptionEnum::PLAINTEXT,
            Self::SSL => K8EncryptionEnum::SSL

        }
    }
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