//!
//! # CRD Definition
//!
//! Interface to the CRD header definition in K8 key value store
//!
#[derive(Debug)]
pub struct Crd {
    pub group: &'static str,
    pub version: &'static str,
    pub names: CrdNames,
}

#[derive(Debug)]
pub struct CrdNames {
    pub kind: &'static str,
    pub plural: &'static str,
    pub singular: &'static str,
}

pub const GROUP: &'static str = "fluvio.infinyon.com";
pub const V1: &'static str = "v1";
