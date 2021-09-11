#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;

use fluvio_controlplane_metadata::core::*;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_controlplane_metadata::spu::*;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::store::Epoch;
use fluvio_controlplane_metadata::message::Message;

use crate::AdminPublicApiKey;
use crate::AdminRequest;

use super::*;

/// marker trait for List
pub trait WatchSpec: Spec {
    /// convert to list request with filters
    #[allow(clippy::wrong_self_convention)]
    fn into_list_request(epoch: Epoch) -> WatchRequest;
}

/// Watch resources
/// Argument epoch is not being used, it is always 0
#[derive(Debug)]
pub enum WatchRequest {
    Topic(Epoch),
    Spu(Epoch),
    SpuGroup(Epoch),
    Partition(Epoch),
}

impl Default for WatchRequest {
    fn default() -> Self {
        Self::Spu(0)
    }
}

impl Request for WatchRequest {
    const API_KEY: u16 = AdminPublicApiKey::Watch as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = WatchResponse;
}

impl AdminRequest for WatchRequest {}

#[derive(Debug)]
pub enum WatchResponse {
    Topic(MetadataUpdate<TopicSpec>),
    Spu(MetadataUpdate<SpuSpec>),
    SpuGroup(MetadataUpdate<SpuGroupSpec>),
    Partition(MetadataUpdate<PartitionSpec>),
}

impl Default for WatchResponse {
    fn default() -> Self {
        Self::Topic(MetadataUpdate::default())
    }
}

/// updates on metadata
#[derive(Encoder, Decoder, Default, Clone, Debug)]
pub struct MetadataUpdate<S>
where
    S: Spec + Debug + Encoder + Decoder,
    S::Status: Debug + Encoder + Decoder,
{
    pub epoch: Epoch,
    pub changes: Vec<Message<Metadata<S>>>,
    pub all: Vec<Metadata<S>>,
}

impl<S> MetadataUpdate<S>
where
    S: Spec + Debug + Encoder + Decoder,
    S::Status: Debug + Encoder + Decoder,
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

    impl WatchRequest {
        /// type represent as string
        fn type_string(&self) -> &'static str {
            match self {
                Self::Topic(_) => TopicSpec::LABEL,
                Self::Spu(_) => SpuSpec::LABEL,
                Self::SpuGroup(_) => SpuGroupSpec::LABEL,
                Self::Partition(_) => PartitionSpec::LABEL,
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

                // Unexpected type
                _ => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid spec type {}", typ),
                )),
            }
        }
    }
}
