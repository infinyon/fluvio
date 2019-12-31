//!
//! # SPU Spec
//!
//! Interface to the SPU metadata spec in K8 key value store
//!
use crate::SPU_API;
use metadata_core::Crd;
use metadata_core::Spec;

use serde::Deserialize;
use serde::Serialize;

use super::SpuStatus;

impl Spec for SpuSpec {
    type Status = SpuStatus;
    fn metadata() -> &'static Crd {
        &SPU_API
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpuSpec {
    pub spu_id: i32,
    pub public_endpoint: IngressPort,
    pub private_endpoint: Endpoint,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub spu_type: Option<SpuType>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum SpuType {
    Managed,
    Custom,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct IngressPort {
    pub port: u16,
    pub ingress: Vec<IngressAddr>,
    pub encryption: EncryptionEnum,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
pub struct IngressAddr {
    pub hostname: Option<String>,
    pub ip: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Endpoint {
    pub port: u16,
    pub host: String,
    pub encryption: EncryptionEnum,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum EncryptionEnum {
    PLAINTEXT,
    SSL,
}

// -----------------------------------
// Implementation - Endpoint
// -----------------------------------

impl Endpoint {
    pub fn new(port: u16, host: String) -> Self {
        Endpoint {
            port,
            host,
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }
}

// -----------------------------------
// Implementation - EncryptionEnum
// -----------------------------------
impl Default for EncryptionEnum {
    fn default() -> EncryptionEnum {
        EncryptionEnum::PLAINTEXT
    }
}
