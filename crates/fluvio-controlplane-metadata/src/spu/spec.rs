#![allow(clippy::assign_op_pattern)]

//!
//! # Spu Spec
//!
//! Spu Spec metadata information cached locally.
//!
use std::convert::TryFrom;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt;

use flv_util::socket_helpers::EndPoint as SocketEndPoint;
use flv_util::socket_helpers::EndPointEncryption;
use fluvio_types::SpuId;
use flv_util::socket_helpers::ServerAddress;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::bytes::{Buf, BufMut};
use fluvio_protocol::Version;

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SpuSpec {
    #[cfg_attr(feature = "use_serde", serde(rename = "spuId"))]
    pub id: SpuId,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub spu_type: SpuType,
    pub public_endpoint: IngressPort,
    pub private_endpoint: Endpoint,
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub rack: Option<String>,

    #[fluvio(min_version = 1)]
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub public_endpoint_local: Option<Endpoint>,
}

impl fmt::Display for SpuSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "id: {}, type: {}, public: {}",
            self.id, self.spu_type, self.public_endpoint
        )
    }
}

impl From<SpuId> for SpuSpec {
    fn from(spec: SpuId) -> Self {
        Self::new(spec)
    }
}

impl SpuSpec {
    /// Given an Spu id generate a new SpuSpec
    pub fn new(id: SpuId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub fn new_public_addr(id: SpuId, port: u16, host: String) -> Self {
        Self {
            id,
            public_endpoint: IngressPort {
                port,
                ingress: vec![IngressAddr::from_host(host)],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn new_private_addr(id: SpuId, port: u16, host: String) -> Self {
        Self {
            id,
            private_endpoint: Endpoint {
                port,
                host,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn set_custom(mut self) -> Self {
        self.spu_type = SpuType::Custom;
        self
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

    pub fn update(&mut self, other: &Self) {
        if self.rack != other.rack {
            self.rack = other.rack.clone();
        }
        if self.public_endpoint != other.public_endpoint {
            self.public_endpoint = other.public_endpoint.clone();
        }
        if self.private_endpoint != other.private_endpoint {
            self.private_endpoint = other.private_endpoint.clone();
        }
    }
}

/// Custom Spu Spec
/// This is not real spec since when this is stored on metadata store, it will be stored as SPU
#[derive(Decoder, Encoder, Debug, Clone, Default, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct CustomSpuSpec {
    pub id: SpuId,
    pub public_endpoint: IngressPort,
    pub private_endpoint: Endpoint,
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub rack: Option<String>,
}

impl CustomSpuSpec {
    pub const LABEL: &'static str = "CustomSpu";
}

impl From<CustomSpuSpec> for SpuSpec {
    fn from(spec: CustomSpuSpec) -> Self {
        Self {
            id: spec.id,
            public_endpoint: spec.public_endpoint,
            private_endpoint: spec.private_endpoint,
            rack: spec.rack,
            spu_type: SpuType::Custom,
            public_endpoint_local: Default::default(),
        }
    }
}

impl From<SpuSpec> for CustomSpuSpec {
    fn from(spu: SpuSpec) -> Self {
        match spu.spu_type {
            SpuType::Custom => Self {
                id: spu.id,
                public_endpoint: spu.public_endpoint,
                private_endpoint: spu.private_endpoint,
                rack: spu.rack,
            },
            SpuType::Managed => panic!("managed spu type can't be converted into custom"),
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase", default)
)]
pub struct IngressPort {
    pub port: u16,
    pub ingress: Vec<IngressAddr>,
    pub encryption: EncryptionEnum,
}

impl fmt::Display for IngressPort {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host_string(), self.port)
    }
}

impl From<ServerAddress> for IngressPort {
    fn from(addr: ServerAddress) -> Self {
        Self {
            port: addr.port,
            ingress: vec![IngressAddr::from_host(addr.host)],
            ..Default::default()
        }
    }
}

impl IngressPort {
    pub fn from_port_host(port: u16, host: String) -> Self {
        Self {
            port,
            ingress: vec![IngressAddr {
                hostname: Some(host),
                ip: None,
            }],
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }

    // return any host whether it is IP or String
    pub fn host(&self) -> Option<String> {
        if self.ingress.is_empty() {
            None
        } else {
            self.ingress[0].host()
        }
    }

    pub fn host_string(&self) -> String {
        match self.host() {
            Some(host_val) => host_val,
            None => "".to_owned(),
        }
    }

    // convert to host:addr format
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host_string(), self.port)
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IngressAddr {
    pub hostname: Option<String>,
    pub ip: Option<String>,
}

impl IngressAddr {
    pub fn from_host(hostname: String) -> Self {
        Self {
            hostname: Some(hostname),
            ..Default::default()
        }
    }

    pub fn from_ip(ip: String) -> Self {
        Self {
            ip: Some(ip),
            ..Default::default()
        }
    }

    pub fn host(&self) -> Option<String> {
        self.hostname.clone().or_else(|| self.ip.clone())
    }
}

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Endpoint {
    pub port: u16,
    pub host: String,
    pub encryption: EncryptionEnum,
}

impl From<ServerAddress> for Endpoint {
    fn from(addr: ServerAddress) -> Self {
        Self {
            port: addr.port,
            host: addr.host,
            ..Default::default()
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
        flv_util::socket_helpers::host_port_to_socket_addr(&endpoint.host, endpoint.port).map(
            |addr| SocketEndPoint {
                addr,
                encryption: EndPointEncryption::PLAINTEXT,
            },
        )
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
            port,
            host,
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }
}

#[derive(Default, Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EncryptionEnum {
    #[default]
    #[fluvio(tag = 0)]
    PLAINTEXT,
    #[fluvio(tag = 1)]
    SSL,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive()]
pub enum SpuType {
    #[default]
    #[fluvio(tag = 0)]
    Managed,
    #[fluvio(tag = 1)]
    Custom,
}

/// Return type label in String format
impl SpuType {
    pub fn type_label(&self) -> &str {
        match self {
            Self::Managed => "managed",
            Self::Custom => "custom",
        }
    }
}

impl fmt::Display for SpuType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.type_label())
    }
}

#[derive(Debug)]
pub enum CustomSpu {
    Name(String),
    Id(i32),
}

// -----------------------------------
// Implementation - CustomSpu
// -----------------------------------
impl Default for CustomSpu {
    fn default() -> CustomSpu {
        Self::Name("".to_string())
    }
}

impl Encoder for CustomSpu {
    // compute size
    fn write_size(&self, version: Version) -> usize {
        let type_size = (0u8).write_size(version);
        match self {
            Self::Name(name) => type_size + name.write_size(version),
            Self::Id(id) => type_size + id.write_size(version),
        }
    }

    // encode match
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        // ensure buffer is large enough
        if dest.remaining_mut() < self.write_size(version) {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough capacity for custom spu len of {}",
                    self.write_size(version)
                ),
            ));
        }

        match self {
            Self::Name(name) => {
                let typ: u8 = 0;
                typ.encode(dest, version)?;
                name.encode(dest, version)?;
            }
            Self::Id(id) => {
                let typ: u8 = 1;
                typ.encode(dest, version)?;
                id.encode(dest, version)?;
            }
        }

        Ok(())
    }
}

impl Decoder for CustomSpu {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        let mut value: u8 = 0;
        value.decode(src, version)?;
        match value {
            0 => {
                let mut name: String = String::default();
                name.decode(src, version)?;
                *self = Self::Name(name)
            }
            1 => {
                let mut id: i32 = 0;
                id.decode(src, version)?;
                *self = Self::Id(id)
            }
            _ => {
                return Err(IoError::new(
                    ErrorKind::UnexpectedEof,
                    format!("invalid value for Custom Spu: {value}"),
                ))
            }
        }

        Ok(())
    }
}
