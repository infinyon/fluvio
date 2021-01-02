#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::derive::{Decode, Encode};
use dataplane::core::Encoder;
use dataplane::core::Decoder;
use dataplane::api::Request;

use crate::Status;
use crate::AdminPublicApiKey;
use crate::AdminRequest;

pub use create::AllCreatableSpec;

#[derive(Encode, Decode, Default, Debug)]
pub struct CreateRequest {
    pub name: String,
    pub dry_run: bool,
    pub spec: AllCreatableSpec,
}

impl Request for CreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = Status;
}

impl AdminRequest for CreateRequest {}

#[allow(clippy::module_inception)]
mod create {

    use std::io::Error;
    use std::io::ErrorKind;

    use tracing::trace;

    use dataplane::core::Version;
    use dataplane::bytes::{Buf, BufMut};
    use fluvio_controlplane_metadata::topic::TopicSpec;
    use fluvio_controlplane_metadata::spu::CustomSpuSpec;
    use fluvio_controlplane_metadata::spg::SpuGroupSpec;
    use super::*;

    const TOPIC: u8 = 0;
    const CUSTOM_SPU: u8 = 1;
    const SPG: u8 = 2;

    #[derive(Debug)]
    /// enum of spec that can be created
    pub enum AllCreatableSpec {
        Topic(TopicSpec),
        CustomSpu(CustomSpuSpec),
        SpuGroup(SpuGroupSpec),
    }

    impl Default for AllCreatableSpec {
        fn default() -> Self {
            Self::Topic(TopicSpec::default())
        }
    }

    impl Encoder for AllCreatableSpec {
        fn write_size(&self, version: Version) -> usize {
            let type_size = (0u8).write_size(version);

            type_size
                + match self {
                    Self::Topic(s) => s.write_size(version),
                    Self::CustomSpu(s) => s.write_size(version),
                    Self::SpuGroup(s) => s.write_size(version),
                }
        }

        // encode match
        fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
        where
            T: BufMut,
        {
            match self {
                Self::Topic(s) => {
                    let typ: u8 = TOPIC;
                    typ.encode(dest, version)?;
                    s.encode(dest, version)?;
                }

                Self::CustomSpu(s) => {
                    let typ: u8 = CUSTOM_SPU;
                    typ.encode(dest, version)?;
                    s.encode(dest, version)?;
                }

                Self::SpuGroup(s) => {
                    let typ: u8 = SPG;
                    typ.encode(dest, version)?;
                    s.encode(dest, version)?;
                }
            }

            Ok(())
        }
    }

    impl Decoder for AllCreatableSpec {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
        where
            T: Buf,
        {
            let mut typ: u8 = 0;
            typ.decode(src, version)?;
            trace!("decoded type: {}", typ);

            match typ {
                TOPIC => {
                    let mut response = TopicSpec::default();
                    response.decode(src, version)?;
                    *self = Self::Topic(response);
                    Ok(())
                }

                CUSTOM_SPU => {
                    let mut response = CustomSpuSpec::default();
                    response.decode(src, version)?;
                    *self = Self::CustomSpu(response);
                    Ok(())
                }

                SPG => {
                    let mut response = SpuGroupSpec::default();
                    response.decode(src, version)?;
                    *self = Self::SpuGroup(response);
                    Ok(())
                }

                // Unexpected type
                _ => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid spec type {}", typ),
                )),
            }
        }
    }
}
