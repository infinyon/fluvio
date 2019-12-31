//!
//! # AuthToken Spec
//!
//! Interface to the AuthToken spec in K8 key value store
//!

use crate::AUTH_TOKEN_API;
use metadata_core::Crd;
use metadata_core::Spec;

use serde::Deserialize;
use serde::Serialize;

use super::AuthTokenStatus;

// -----------------------------------
// Data Structures
// -----------------------------------

impl Spec for AuthTokenSpec {
    type Status = AuthTokenStatus;
    fn metadata() -> &'static Crd {
        &AUTH_TOKEN_API
    }
}

// TODO: add refresh secret

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AuthTokenSpec {
    pub token_type: TokenType,
    pub min_spu: i32,
    pub max_spu: i32,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum TokenType {
    Any,
    Custom,
    Managed,
}

// -----------------------------------
// Implementation - AuthTokenSpec
// -----------------------------------

impl AuthTokenSpec {
    pub fn new(token_type: TokenType, min_spu: i32, max_spu: i32) -> Self {
        AuthTokenSpec {
            token_type,
            min_spu,
            max_spu,
        }
    }
}
