//!
//! # Auth Token Spec
//!
//! Auth Token Spec metadata information cached locally.
//!
use types::SpuId;

use kf_protocol::derive::{Decode, Encode};

use k8_metadata::auth_token::AuthTokenSpec as K8AuthTokenSpec;
use k8_metadata::auth_token::TokenType as K8TokenType;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decode, Encode, Debug, Clone, PartialEq, Default)]
pub struct AuthTokenSpec {
    pub token_type: TokenType,
    pub min_spu: SpuId,
    pub max_spu: SpuId,
}

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub enum TokenType {
    Any,
    Custom,
    Managed,
}

// -----------------------------------
// Encode - from K8 AuthTokenSpec
// -----------------------------------

impl From<K8AuthTokenSpec> for AuthTokenSpec {
    fn from(k8_spec: K8AuthTokenSpec) -> Self {
        AuthTokenSpec {
            token_type: k8_spec.token_type.into(),
            min_spu: k8_spec.min_spu,
            max_spu: k8_spec.max_spu,
        }
    }
}


impl From<K8TokenType> for TokenType {
    fn from(k8_token_type: K8TokenType) -> Self {
        match k8_token_type {
            K8TokenType::Any => TokenType::Any,
            K8TokenType::Custom => TokenType::Custom,
            K8TokenType::Managed => TokenType::Managed,
        }
    }
}

// -----------------------------------
// Implementation - AuthTokenSpec
// -----------------------------------

impl AuthTokenSpec {
    pub fn token_type_label(token_type: &TokenType) -> &'static str {
        match token_type {
            TokenType::Any => "any",
            TokenType::Custom => "custom",
            TokenType::Managed => "managed",
        }
    }
}

// -----------------------------------
// Implementation - TokenType
// -----------------------------------

impl ::std::default::Default for TokenType {
    fn default() -> Self {
        TokenType::Any
    }
}
