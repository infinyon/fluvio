#![allow(clippy::assign_op_pattern)]

//!
//! # Schema Spec
//!
//!
use fluvio_protocol::{Encoder, Decoder};

/// Spec for Schemas
/// Each partition has replicas spread among SPU
/// one of replica is leader which is duplicated in the leader field
#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SchemaSpec {
    pub schema_version: String,  // Semver?
    pub schema_provider: String, // smartmodule reference
}

impl SchemaSpec {
    pub fn new(schema_provider: String) -> Self {
        Self {
            schema_provider,
            ..Default::default()
        }
    }
}
