#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::marker::PhantomData;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::{Request};
use fluvio_controlplane_metadata::store::Epoch;
use fluvio_controlplane_metadata::message::Message;

use crate::{AdminPublicApiKey, AdminSpec};
use crate::core::Spec;

use super::{Metadata, ObjectApiEnum};

ObjectApiEnum!(WatchRequest);
ObjectApiEnum!(WatchResponse);

/// Watch resources
/// Argument epoch is not being used, it is always 0
#[derive(Debug, Encoder, Default, Decoder)]
pub struct WatchRequest<S: AdminSpec> {
    epoch: Epoch,
    data: PhantomData<S>,
}

impl Request for ObjectApiWatchRequest {
    const API_KEY: u16 = AdminPublicApiKey::Watch as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = ObjectApiWatchResponse;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct WatchResponse<S: AdminSpec>
where
    <S::WatchResponseType as Spec>::Status: Encoder + Decoder,
{
    inner: MetadataUpdate<S::WatchResponseType>,
}

impl<S> WatchResponse<S>
where
    S: AdminSpec,
    <S::WatchResponseType as Spec>::Status: Encoder + Decoder,
{
    pub fn new(inner: MetadataUpdate<S::WatchResponseType>) -> Self {
        Self { inner }
    }

    pub fn inner(self) -> MetadataUpdate<S::WatchResponseType> {
        self.inner
    }
}

/// updates on metadata
#[derive(Encoder, Decoder, Default, Clone, Debug)]
pub struct MetadataUpdate<S>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder + Debug,
{
    pub epoch: Epoch,
    pub changes: Vec<Message<Metadata<S>>>,
    pub all: Vec<Metadata<S>>,
}

impl<S> MetadataUpdate<S>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder + Debug,
{
    pub fn with_changes(epoch: i64, changes: Vec<Message<Metadata<S>>>) -> Self {
        Self {
            epoch,
            changes,
            all: vec![],
        }
    }

    pub fn with_all(epoch: i64, all: Vec<Metadata<S>>) -> Self {
        Self {
            epoch,
            changes: vec![],
            all,
        }
    }
}
<<<<<<< HEAD
=======

// later this can be written using procedure macro
mod encoding {

    use std::io::Error;
    use std::io::ErrorKind;

    use fluvio_controlplane_metadata::smartstream::SmartStreamSpec;
    use tracing::trace;

    use dataplane::core::Encoder;
    use dataplane::core::Decoder;
    use dataplane::core::Version;
    use dataplane::bytes::{Buf, BufMut};

    use super::*;

    impl WatchRequest {
        /// type represent as string
        fn type_string(&self) -> &'static str {
            match self {
                Self::Topic(_) => TopicSpec::LABEL,
                Self::Spu(_) => SpuSpec::LABEL,
                Self::SpuGroup(_) => SpuGroupSpec::LABEL,
                Self::Partition(_) => PartitionSpec::LABEL,
                Self::ManagedConnector(_) => ManagedConnectorSpec::LABEL,
                Self::SmartModule(_) => SmartModuleSpec::LABEL,
                Self::Table(_) => TableSpec::LABEL,
                Self::SmartStream(_) => SmartStreamSpec::LABEL,
            }
        }
    }

    impl Encoder for WatchRequest {
        fn write_size(&self, version: Version) -> usize {
            let type_size = self.type_string().to_owned().write_size(version);

            type_size
                + match self {
                    Self::Topic(s) => s.write_size(version),
                    Self::SpuGroup(s) => s.write_size(version),
                    Self::Spu(s) => s.write_size(version),
                    Self::Partition(s) => s.write_size(version),
                    Self::ManagedConnector(s) => s.write_size(version),
                    Self::SmartModule(s) => s.write_size(version),
                    Self::Table(s) => s.write_size(version),
                    Self::SmartStream(s) => s.write_size(version),
                    
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
                Self::SpuGroup(s) => s.encode(dest, version)?,
                Self::Spu(s) => s.encode(dest, version)?,
                Self::Partition(s) => s.encode(dest, version)?,
                Self::ManagedConnector(s) => s.encode(dest, version)?,
                Self::SmartModule(s) => s.encode(dest, version)?,
                Self::Table(s) => s.encode(dest, version)?,
                Self::SmartStream(s) => s.encode(dest, version)?,
            }

            Ok(())
        }
    }

    impl Decoder for WatchRequest {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
        where
            T: Buf,
        {
            let mut typ = "".to_owned();
            typ.decode(src, version)?;
            trace!("decoded type: {}", typ);

            match typ.as_ref() {
                TopicSpec::LABEL => {
                    let mut response: Epoch = Epoch::default();
                    response.decode(src, version)?;
                    *self = Self::Topic(response);
                    Ok(())
                }

                SpuGroupSpec::LABEL => {
                    let mut response: Epoch = Epoch::default();
                    response.decode(src, version)?;
                    *self = Self::SpuGroup(response);
                    Ok(())
                }

                SpuSpec::LABEL => {
                    let mut response: Epoch = Epoch::default();
                    response.decode(src, version)?;
                    *self = Self::Spu(response);
                    Ok(())
                }

                PartitionSpec::LABEL => {
                    let mut response: Epoch = Epoch::default();
                    response.decode(src, version)?;
                    *self = Self::Partition(response);
                    Ok(())
                }

                TableSpec::LABEL => {
                    let mut response: Epoch = Epoch::default();
                    response.decode(src, version)?;
                    *self = Self::Table(response);
                    Ok(())
                }

                SmartStreamSpec::LABEL => {
                    let mut response: Epoch = Epoch::default();
                    response.decode(src, version)?;
                    *self = Self::SmartStream(response);
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

    impl WatchResponse {
        /// type represent as string
        fn type_string(&self) -> &'static str {
            match self {
                Self::Topic(_) => TopicSpec::LABEL,
                Self::Spu(_) => SpuSpec::LABEL,
                Self::SpuGroup(_) => SpuGroupSpec::LABEL,
                Self::Partition(_) => PartitionSpec::LABEL,
                Self::ManagedConnector(_) => ManagedConnectorSpec::LABEL,
                Self::SmartModule(_) => SmartModuleSpec::LABEL,
                Self::Table(_) => TableSpec::LABEL,
                Self::SmartStream(_) => SmartStreamSpec::LABEL,
            }
        }
    }

    impl Encoder for WatchResponse {
        fn write_size(&self, version: Version) -> usize {
            let type_size = self.type_string().to_owned().write_size(version);

            type_size
                + match self {
                    Self::Topic(s) => s.write_size(version),
                    Self::SpuGroup(s) => s.write_size(version),
                    Self::Spu(s) => s.write_size(version),
                    Self::Partition(s) => s.write_size(version),
                    Self::ManagedConnector(s) => s.write_size(version),
                    Self::SmartModule(s) => s.write_size(version),
                    Self::Table(s) => s.write_size(version),
                    Self::SmartStream(s) => s.write_size(version),
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
                Self::SpuGroup(s) => s.encode(dest, version)?,
                Self::Spu(s) => s.encode(dest, version)?,
                Self::Partition(s) => s.encode(dest, version)?,
                Self::ManagedConnector(s) => s.encode(dest, version)?,
                Self::SmartModule(s) => s.encode(dest, version)?,
                Self::Table(s) => s.encode(dest, version)?,
                Self::SmartStream(s) => s.encode(dest, version)?,
            }

            Ok(())
        }
    }

    impl Decoder for WatchResponse {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
        where
            T: Buf,
        {
            let mut typ = "".to_owned();
            typ.decode(src, version)?;
            trace!("decoded type: {}", typ);

            match typ.as_ref() {
                TopicSpec::LABEL => {
                    let mut response = MetadataUpdate::default();
                    response.decode(src, version)?;
                    *self = Self::Topic(response);
                    Ok(())
                }

                SpuGroupSpec::LABEL => {
                    let mut response = MetadataUpdate::default();
                    response.decode(src, version)?;
                    *self = Self::SpuGroup(response);
                    Ok(())
                }

                SpuSpec::LABEL => {
                    let mut response = MetadataUpdate::default();
                    response.decode(src, version)?;
                    *self = Self::Spu(response);
                    Ok(())
                }

                PartitionSpec::LABEL => {
                    let mut response = MetadataUpdate::default();
                    response.decode(src, version)?;
                    *self = Self::Partition(response);
                    Ok(())
                }

                SmartModuleSpec::LABEL => {
                    let mut response = MetadataUpdate::default();
                    response.decode(src, version)?;
                    *self = Self::SmartModule(response);
                    Ok(())
                }

                TableSpec::LABEL => {
                    let mut response = MetadataUpdate::default();
                    response.decode(src, version)?;
                    *self = Self::Table(response);
                    Ok(())
                }

                SmartStreamSpec::LABEL => {
                    let mut response = MetadataUpdate::default();
                    response.decode(src, version)?;
                    *self = Self::SmartStream(response);
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
>>>>>>> add missing files
