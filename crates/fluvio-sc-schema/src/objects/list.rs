#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::fmt::Display;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::Error as IoError;
use std::io::ErrorKind;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;

use fluvio_controlplane_metadata::core::*;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_controlplane_metadata::spu::*;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::store::*;
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;
use crate::AdminPublicApiKey;
use crate::AdminRequest;

/// marker trait
pub trait ListFilter {}

/// filter by name
pub type NameFilter = String;

impl ListFilter for NameFilter {}

pub trait ListSpec: Spec {
    /// filter type
    type Filter: ListFilter;

    /// convert to list request with filters
    #[allow(clippy::wrong_self_convention)]
    fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest;
}

#[derive(Debug)]
pub enum ListRequest {
    Topic(Vec<NameFilter>),
    Spu(Vec<NameFilter>),
    SpuGroup(Vec<NameFilter>),
    CustomSpu(Vec<NameFilter>),
    Partition(Vec<NameFilter>),
    ManagedConnector(Vec<NameFilter>),
}

impl Default for ListRequest {
    fn default() -> Self {
        Self::Spu(vec![])
    }
}

impl Request for ListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = ListResponse;
}

impl AdminRequest for ListRequest {}

#[derive(Debug)]
pub enum ListResponse {
    Topic(Vec<Metadata<TopicSpec>>),
    Spu(Vec<Metadata<SpuSpec>>),
    CustomSpu(Vec<Metadata<CustomSpuSpec>>),
    SpuGroup(Vec<Metadata<SpuGroupSpec>>),
    Partition(Vec<Metadata<PartitionSpec>>),
    ManagedConnector(Vec<Metadata<ManagedConnectorSpec>>),
}

impl Default for ListResponse {
    fn default() -> Self {
        Self::Topic(vec![])
    }
}

#[derive(Encoder, Decoder, Default, Clone, Debug)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Metadata<S>
where
    S: Spec + Debug + Encoder + Decoder,
    S::Status: Debug + Encoder + Decoder,
{
    pub name: String,
    pub spec: S,
    pub status: S::Status,
}

impl<S, C> From<MetadataStoreObject<S, C>> for Metadata<S>
where
    S: Spec + Encoder + Decoder,
    S::IndexKey: ToString,
    S::Status: Encoder + Decoder,
    C: MetadataItem,
{
    fn from(meta: MetadataStoreObject<S, C>) -> Self {
        Self {
            name: meta.key.to_string(),
            spec: meta.spec,
            status: meta.status,
        }
    }
}

impl<S, C> TryFrom<Metadata<S>> for MetadataStoreObject<S, C>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder,
    C: MetadataItem,
    <S as Spec>::IndexKey: TryFrom<String>,
    <<S as Spec>::IndexKey as TryFrom<String>>::Error: Display,
{
    type Error = IoError;

    fn try_from(value: Metadata<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            spec: value.spec,
            status: value.status,
            key: value.name.try_into().map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("problem converting: {}", err),
                )
            })?,
            ctx: MetadataContext::default(),
        })
    }
}

// later this can be written using procedure macro
mod encoding {

    use std::io::Error;
    use std::io::ErrorKind;

    use tracing::trace;

    use dataplane::core::Encoder;
    use dataplane::core::Decoder;
    use dataplane::core::Version;
    use dataplane::bytes::{Buf, BufMut};

    use super::*;

    impl ListRequest {
        /// type represent as string
        fn type_string(&self) -> &'static str {
            match self {
                Self::Topic(_) => TopicSpec::LABEL,
                Self::Spu(_) => SpuSpec::LABEL,
                Self::SpuGroup(_) => SpuGroupSpec::LABEL,
                Self::CustomSpu(_) => CustomSpuSpec::LABEL,
                Self::Partition(_) => PartitionSpec::LABEL,
                Self::ManagedConnector(_) => ManagedConnectorSpec::LABEL,
            }
        }
    }

    impl Encoder for ListRequest {
        fn write_size(&self, version: Version) -> usize {
            let type_size = self.type_string().to_owned().write_size(version);

            type_size
                + match self {
                    Self::Topic(s) => s.write_size(version),
                    Self::CustomSpu(s) => s.write_size(version),
                    Self::SpuGroup(s) => s.write_size(version),
                    Self::Spu(s) => s.write_size(version),
                    Self::Partition(s) => s.write_size(version),
                    Self::ManagedConnector(s) => s.write_size(version),
                }
        }

        // encode match
        fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
        where
            T: BufMut,
        {
            self.type_string().to_owned().encode(dest, version)?;

            match self {
                Self::Topic(s) => s.encode(dest, version)?,
                Self::CustomSpu(s) => s.encode(dest, version)?,
                Self::SpuGroup(s) => s.encode(dest, version)?,
                Self::Spu(s) => s.encode(dest, version)?,
                Self::Partition(s) => s.encode(dest, version)?,
                Self::ManagedConnector(s) => s.encode(dest, version)?,
            }

            Ok(())
        }
    }

    impl Decoder for ListRequest {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
        where
            T: Buf,
        {
            let mut typ = "".to_owned();
            typ.decode(src, version)?;
            trace!("decoded type: {}", typ);

            match typ.as_ref() {
                TopicSpec::LABEL => {
                    let mut response: Vec<NameFilter> = vec![];
                    response.decode(src, version)?;
                    *self = Self::Topic(response);
                    Ok(())
                }

                CustomSpuSpec::LABEL => {
                    let mut response: Vec<NameFilter> = vec![];
                    response.decode(src, version)?;
                    *self = Self::CustomSpu(response);
                    Ok(())
                }

                SpuGroupSpec::LABEL => {
                    let mut response: Vec<NameFilter> = vec![];
                    response.decode(src, version)?;
                    *self = Self::SpuGroup(response);
                    Ok(())
                }

                SpuSpec::LABEL => {
                    let mut response: Vec<NameFilter> = vec![];
                    response.decode(src, version)?;
                    *self = Self::Spu(response);
                    Ok(())
                }

                PartitionSpec::LABEL => {
                    let mut response: Vec<NameFilter> = vec![];
                    response.decode(src, version)?;
                    *self = Self::Partition(response);
                    Ok(())
                }
                ManagedConnectorSpec::LABEL => {
                    let mut response: Vec<NameFilter> = vec![];
                    response.decode(src, version)?;
                    *self = Self::ManagedConnector(response);
                    Ok(())
                }

                // Unexpected type
                _ => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid request type {}", typ),
                )),
            }
        }
    }

    impl ListResponse {
        /// type represent as string
        fn type_string(&self) -> &'static str {
            match self {
                Self::Topic(_) => TopicSpec::LABEL,
                Self::Spu(_) => SpuSpec::LABEL,
                Self::SpuGroup(_) => SpuGroupSpec::LABEL,
                Self::CustomSpu(_) => CustomSpuSpec::LABEL,
                Self::Partition(_) => PartitionSpec::LABEL,
                Self::ManagedConnector(_) => ManagedConnectorSpec::LABEL,
            }
        }
    }

    impl Encoder for ListResponse {
        fn write_size(&self, version: Version) -> usize {
            let type_size = self.type_string().to_owned().write_size(version);

            type_size
                + match self {
                    Self::Topic(s) => s.write_size(version),
                    Self::CustomSpu(s) => s.write_size(version),
                    Self::SpuGroup(s) => s.write_size(version),
                    Self::Spu(s) => s.write_size(version),
                    Self::Partition(s) => s.write_size(version),
                    Self::ManagedConnector(s) => s.write_size(version),
                }
        }

        // encode match
        fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
        where
            T: BufMut,
        {
            self.type_string().to_owned().encode(dest, version)?;

            match self {
                Self::Topic(s) => s.encode(dest, version)?,
                Self::CustomSpu(s) => s.encode(dest, version)?,
                Self::SpuGroup(s) => s.encode(dest, version)?,
                Self::Spu(s) => s.encode(dest, version)?,
                Self::Partition(s) => s.encode(dest, version)?,
                Self::ManagedConnector(s) => s.encode(dest, version)?,
            }

            Ok(())
        }
    }

    impl Decoder for ListResponse {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
        where
            T: Buf,
        {
            let mut typ = "".to_owned();
            typ.decode(src, version)?;
            trace!("decoded type: {}", typ);

            match typ.as_ref() {
                TopicSpec::LABEL => {
                    let mut response: Vec<Metadata<TopicSpec>> = vec![];
                    response.decode(src, version)?;
                    *self = Self::Topic(response);
                    Ok(())
                }

                CustomSpuSpec::LABEL => {
                    let mut response: Vec<Metadata<CustomSpuSpec>> = vec![];
                    response.decode(src, version)?;
                    *self = Self::CustomSpu(response);
                    Ok(())
                }

                SpuGroupSpec::LABEL => {
                    let mut response: Vec<Metadata<SpuGroupSpec>> = vec![];
                    response.decode(src, version)?;
                    *self = Self::SpuGroup(response);
                    Ok(())
                }

                SpuSpec::LABEL => {
                    let mut response: Vec<Metadata<SpuSpec>> = vec![];
                    response.decode(src, version)?;
                    *self = Self::Spu(response);
                    Ok(())
                }

                PartitionSpec::LABEL => {
                    let mut response: Vec<Metadata<PartitionSpec>> = vec![];
                    response.decode(src, version)?;
                    *self = Self::Partition(response);
                    Ok(())
                }
                ManagedConnectorSpec::LABEL => {
                    let mut response: Vec<Metadata<ManagedConnectorSpec>> = vec![];
                    response.decode(src, version)?;
                    *self = Self::ManagedConnector(response);
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
