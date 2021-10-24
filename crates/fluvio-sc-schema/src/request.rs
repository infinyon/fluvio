//!
//! # API Requests
//!
//! Maps SC Api Requests with their associated Responses.
//!

use std::convert::{TryInto};
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Debug;

use fluvio_controlplane_metadata::spu::CustomSpu;
use tracing::{debug, trace};
use paste::paste;

use dataplane::bytes::{Buf, BufMut};
use dataplane::api::{ApiMessage};
use dataplane::api::RequestHeader;
use dataplane::api::RequestMessage;

use dataplane::api::api_decode;
use dataplane::core::{Encoder, Decoder, Version};
use dataplane::versions::ApiVersionsRequest;

use crate::AdminPublicApiKey;
use crate::AdminSpec;
use crate::objects::{CreateRequest, DeleteRequest, ListRequest, WatchRequest};
use crate::topic::TopicSpec;
use crate::spu::{SpuSpec, CustomSpuSpec};
use crate::smartmodule::SmartModuleSpec;
use crate::partition::PartitionSpec;
use crate::connector::ManagedConnectorSpec;
use crate::spg::SpuGroupSpec;
use crate::table::TableSpec;
use crate::core::Spec;

#[derive(Debug, Encoder)]
pub enum AdminPublicRequest {
    //  ApiVersionsRequest(ObjectRequest<NullDecoder,ApiVersionsRequest>),
    CreateRequest(ObjectRequest<CreateDecoder, ObjectApiCreateRequest>),
    //  DeleteRequest(ObjectRequest<ObjectDecoder,ObjectApiDeleteRequest>),
    ListRequest(ObjectRequest<ObjectDecoder, ObjectApiListRequest>),
    WatchRequest(ObjectRequest<ObjectDecoder, ObjectApiWatchRequest>),
}

impl Default for AdminPublicRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

/// Most of Request except create which has special format
#[derive(Default,Debug)]
pub struct ObjectRequest<Obj>
{
    header: RequestHeader,
    object: Obj,
}

impl<Obj> Encoder for ObjectRequest<Obj>
where
    Obj: Debug + Encoder,
{
    fn write_size(&self, version: Version) -> usize {
        self.header.write_size(version) + self.object.write_size(self.header.api_version())
    }

    fn encode<T>(&self, out: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        trace!("encoding header: {:#?}", &self.header);
        self.header.encode(out, version)?;
        
        trace!("encoding object: {:#?}", &self.object);
        self.object.encode(out, self.header.api_version())?;

        trace!("encoding body: {:#?}", &self.body);
        self.body.encode(out, self.header.api_version())?;
        Ok(())
    }
}

impl<Obj, Body> Decoder for ObjectRequest<Obj, Body>
where
    Obj: Debug + Decoder  ,
    Body: Debug + Decoder,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        // decode header
        let mut header = RequestHeader::default();
        header.decode(src, version)?;

        // decode header
        let mut object = Obj::default();
        header.decode(src, version)?;

        let mut body = Body::default();
        body.decode(src, &header, version)?;

        Ok(())
    }
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectDecoder {}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct NullDecoder {}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct CreateDecoder {}

//ObjectApiEnum!(DeleteRequest);

impl ApiMessage for AdminPublicRequest {
    type ApiKey = AdminPublicApiKey;

    fn decode_from<T>(src: &mut T) -> Result<Self, IoError>
    where
        T: Buf,
    {
        let api_key = header.api_key().try_into()?;
        debug!(
            "decoding admin public request from: {} api: {:#?}",
            header.client_id(),
            api_key
        );
        match api_key {
            AdminPublicApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),
            AdminPublicApiKey::Object => api_decode!(Self, ObjectRequest, src, header),
            /*
            AdminPublicApiKey::Create => api_decode!(Self, CreateRequest, src, header),
            AdminPublicApiKey::Delete => api_decode!(Self, DeleteRequest, src, header),
            AdminPublicApiKey::List => api_decode!(Self, ListRequest, src, header),
            AdminPublicApiKey::Watch => api_decode!(Self, WatchRequest, src, header),
            */
        }
    }
}

macro_rules! ObjectApiEnum {
    ($api:ident) => {


        paste! {
            #[derive(Debug)]
            pub enum [<ObjectApi $api>] {
                Topic($api<TopicSpec>),
                Spu($api<SpuSpec>),
                CustomSpu($api<CustomSpuSpec>),
                SmartModule($api<SmartModuleSpec>),
                Partition($api<PartitionSpec>),
                ManagedConnector($api<ManagedConnectorSpec>),
                SpuGroup($api<SpuGroupSpec>),
                Table($api<TableSpec>),
            }

            impl Default for [<ObjectApi $api>] {
                fn default() -> Self {
                    Self::Topic($api::<TopicSpec>::default())
                }
            }

            impl  [<ObjectApi $api>] {
                fn decode<T,O>(&mut self, src: &mut T, obj_ty: O,version: Version) -> Result<(), IoError>
                where
                    T: Buf,
                    O: AdminObjectDecoder

                {

                    if obj_ty.is_topic() {
                        let mut request = $api::<TopicSpec>::default();
                        request.decode(src, version)?;
                        *self = Self::Topic(request);
                        return Ok(())
                    } else if obj_ty.is_spu() {
                        let mut request = $api::<SpuSpec>::default();
                        request.decode(src, version)?;
                        *self = Self::Spu(request);
                        return Ok(())
                    } else if obj_ty.is_smart_module(){
                        let mut request = $api::<SmartModuleSpec>::default();
                        request.decode(src, version)?;
                        *self = Self::SmartModule(request);
                        return Ok(())
                    } else if obj_ty.is_partition(){

                        let mut request = $api::<PartitionSpec>::default();
                        request.decode(src, version)?;
                        *self = Self::Partition(request);

                        Ok(())
                    } else  {

                        Err(IoError::new(
                            ErrorKind::InvalidData,
                            format!("invalid request type {:#?}", obj_ty),
                        ))
                    }
                }
            }

        }
    }
}

ObjectApiEnum!(CreateRequest);
ObjectApiEnum!(ListRequest);
ObjectApiEnum!(WatchRequest);


trait AdminObjectDecoder: Debug {

    fn is_topic(&self) -> bool;
    fn is_spu(&self) -> bool;
    fn is_partition(&self) -> bool;
    fn is_smart_module(&self) -> bool;
    

}
