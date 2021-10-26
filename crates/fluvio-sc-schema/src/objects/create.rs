#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;

use crate::{AdminPublicApiKey, AdminSpec, CreateDecoder, Status};
pub use create::AllCreatableSpec;

use super::{ObjectApiEnum, ObjectApiDecode};

ObjectApiEnum!(CreateRequest);

#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateRequest<S: AdminSpec> {
    pub name: String,
    pub dry_run: bool,
    pub spec: AllCreatableSpec<S>,
}

impl Request<CreateDecoder> for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = Status;

    ObjectApiDecode!(CreateRequest, CreateDecoder);
}

/// Used for compatibility with older versions of the API
pub enum CreateType {
    Topic = 0,
    CustomSPU = 1,
    SPG = 2,
    ManagedConnector = 3,
    SmartModule = 4,
    TABLE = 5,
    SmartStream = 6,
}

#[allow(clippy::module_inception)]
mod create {

    use super::*;

    #[derive(Debug, Encoder, Decoder)]
    /// This is not really need but keep for compatibility with exiting enum
    pub struct AllCreatableSpec<S: AdminSpec> {
        inner: S,
    }

    impl<S> AllCreatableSpec<S>
    where
        S: AdminSpec,
    {
        pub fn inner(&self) -> &S {
            &self.inner
        }

        pub fn to_inner(self) -> S {
            self.inner
        }
    }

    impl<S> Default for AllCreatableSpec<S>
    where
        S: AdminSpec,
    {
        fn default() -> Self {
            Self {
                inner: S::default(),
            }
        }
    }
}
