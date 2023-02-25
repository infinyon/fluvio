#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use fluvio_protocol::bytes::{BufMut, Buf};
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
use fluvio_protocol::Version;

use crate::topic::TopicSpec;
use crate::customspu::CustomSpuSpec;
use crate::smartmodule::SmartModuleSpec;
use crate::tableformat::TableFormatSpec;
use crate::spg::SpuGroupSpec;

use crate::{AdminPublicApiKey, CreatableAdminSpec, Status};

#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateRequest<S: CreatableAdminSpec> {
    pub request: S,
}

/// Every create request must have this parameters
#[derive(Encoder, Decoder, Default, Debug)]
pub struct CommonCreateRequest {
    pub name: String,
    pub dry_run: bool,
    #[fluvio(min_version = 7)]
    pub timeout: Option<u32>, // timeout in milliseconds
}

impl Request for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const MIN_API_VERSION: i16 = 9;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = Status;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectApiCreateRequest {
    pub common: CommonCreateRequest,
    pub request: ObjectCreateRequest,
}

#[derive(Debug)]
pub enum ObjectCreateRequest {
    Topic(TopicSpec),
    CustomSpu(CustomSpuSpec),
    SmartModule(SmartModuleSpec),
    SpuGroup(SpuGroupSpec),
    TableFormat(TableFormatSpec),
}

impl Default for ObjectCreateRequest {
    fn default() -> Self {
        Self::Topic(TopicSpec::default())
    }
}

impl ObjectCreateRequest {
    fn type_value(&self) -> u8 {
        match self {
            Self::Topic(_) => TopicSpec::CREATE_TYPE,
            Self::CustomSpu(_) => CustomSpuSpec::CREATE_TYPE,
            Self::SmartModule(_) => SmartModuleSpec::CREATE_TYPE,
            Self::SpuGroup(_) => SpuGroupSpec::CREATE_TYPE,
            Self::TableFormat(_) => TableFormatSpec::CREATE_TYPE,
        }
    }
}

impl Encoder for ObjectCreateRequest {
    fn write_size(&self, version: Version) -> usize {
        let type_size = (0u8).write_size(version);

        type_size
            + match self {
                Self::Topic(s) => s.write_size(version),
                Self::CustomSpu(s) => s.write_size(version),
                Self::SmartModule(s) => s.write_size(version),
                Self::SpuGroup(s) => s.write_size(version),
                Self::TableFormat(s) => s.write_size(version),
            }
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), std::io::Error>
    where
        T: BufMut,
    {
        self.type_value().encode(dest, version)?;
        match self {
            Self::Topic(s) => s.encode(dest, version)?,
            Self::CustomSpu(s) => s.encode(dest, version)?,
            Self::SmartModule(s) => s.encode(dest, version)?,
            Self::SpuGroup(s) => s.encode(dest, version)?,
            Self::TableFormat(s) => s.encode(dest, version)?,
        }

        Ok(())
    }
}

// We implement decode signature even thought this will be never called.
// RequestMessage use decode_object.  But in order to provide backward compatibility, we pretend
// to provide decode implementation but should be never called
impl Decoder for ObjectCreateRequest {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), std::io::Error>
    where
        T: Buf,
    {
        let mut typ: u8 = 0;
        typ.decode(src, version)?;
        tracing::trace!("decoded type: {}", typ);

        match typ {
            TopicSpec::CREATE_TYPE => {
                tracing::trace!("detected topic");
                let mut request = TopicSpec::default();
                request.decode(src, version)?;
                *self = Self::Topic(request);
                Ok(())
            }

            TableFormatSpec::CREATE_TYPE => {
                tracing::trace!("detected table");
                let mut request = TableFormatSpec::default();
                request.decode(src, version)?;
                *self = Self::TableFormat(request);
                Ok(())
            }

            CustomSpuSpec::CREATE_TYPE => {
                tracing::trace!("detected custom spu");
                let mut request = CustomSpuSpec::default();
                request.decode(src, version)?;
                *self = Self::CustomSpu(request);
                Ok(())
            }

            SpuGroupSpec::CREATE_TYPE => {
                tracing::trace!("detected custom spu");
                let mut request = SpuGroupSpec::default();
                request.decode(src, version)?;
                *self = Self::SpuGroup(request);
                Ok(())
            }

            SmartModuleSpec::CREATE_TYPE => {
                tracing::trace!("detected smartmodule");
                let mut request = SmartModuleSpec::default();
                request.decode(src, version)?;
                *self = Self::SmartModule(request);
                Ok(())
            }

            // Unexpected type
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid create type {typ:#?}"),
            )),
        }
    }
}

/// Macro to convert create request
/// impl From<(CommonCreateRequest TopicSpec)> for ObjectApiCreateRequest {
/// fn from(req: (CommonCreateRequest TopicSpec)) -> Self {
///       ObjectApiCreateRequest {
///           common: req.0,
///           request: req.1
///       }
/// }
/// ObjectFrom!(WatchRequest, Topic);

macro_rules! CreateFrom {
    ($create:ty,$specTy:ident) => {
        impl From<(crate::objects::CommonCreateRequest, $create)>
            for crate::objects::ObjectApiCreateRequest
        {
            fn from(fr: (crate::objects::CommonCreateRequest, $create)) -> Self {
                crate::objects::ObjectApiCreateRequest {
                    common: fr.0,
                    request: crate::objects::ObjectCreateRequest::$specTy(fr.1),
                }
            }
        }
    };
}

pub(crate) use CreateFrom;

use super::COMMON_VERSION;
