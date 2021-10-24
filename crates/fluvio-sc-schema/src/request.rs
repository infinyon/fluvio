//!
//! # API Requests
//!
//! Maps SC Api Requests with their associated Responses.
//!

use std::convert::{TryInto};
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Debug;


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
use crate::objects::{CreateRequest,  DeleteRequest, ListRequest, ListResponse, WatchRequest};

use crate::core::Spec;

pub use objects::*;




#[derive(Debug, Encoder)]
pub enum AdminPublicRequest {
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),
    CreateRequest(ObjectRequest<CreateDecoder, ObjectApiCreateRequest>),
    //  DeleteRequest(ObjectRequest<ObjectDecoder,ObjectApiDeleteRequest>),
    ListRequest(ObjListRequest),
    WatchRequest(ObjectRequest<ObjectDecoder, ObjectApiWatchRequest>),
}

impl Default for AdminPublicRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}



//ObjectApiEnum!(DeleteRequest);

impl ApiMessage for AdminPublicRequest {
    type ApiKey = AdminPublicApiKey;

    fn decode_with_header<T>(_src: &mut T, _header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf {
            panic!("not needed")
        }

    fn decode_from<T>(src: &mut T) -> Result<Self, IoError>
    where
        T: Buf,
    {
        let header = RequestHeader::decode_from(src, 0)?;

        let api_key = header.api_key().try_into()?;
        debug!(
            "decoding admin public request from: {} api: {:#?}",
            header.client_id(),
            api_key
        );
        match api_key {
            AdminPublicApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),
            AdminPublicApiKey::Create => {
                let mut object = CreateDecoder::default();
                object.decode(src, header.api_version())?;
                let mut body = ObjectApiCreateRequest::default();
                body.decode_object(src, &object, header.api_version())?;
                Ok(Self::CreateRequest(ObjectRequest {
                    header,
                    object,
                    body,
                }))
            },
            
            AdminPublicApiKey::Delete => {
                todo!()
                /* 
                let mut object = ObjectDecoder::default();
                object.decode(src, header.api_version())?;
                let request = ObjectApiDeleteRequest::default();
                request.decode_object(src, &object,header.api_version())?;
                Ok(Self::CreateRequest(ObjectRequest {
                    header,
                    object,
                    body: ObjectApiCreateRequest::default(),
                }))
                */
            }
            
            AdminPublicApiKey::List => {
                let mut object = ObjectDecoder::default();
                object.decode(src, header.api_version())?;
                let mut body = ObjectApiListRequest::default();
                body.decode_object(src, &object, header.api_version())?;
                Ok(Self::ListRequest(ObjectRequest {
                    header,
                    object,
                    body,
                }))
            }
            AdminPublicApiKey::Watch => {
                let mut object = ObjectDecoder::default();
                object.decode(src, header.api_version())?;
                let mut body = ObjectApiWatchRequest::default();
                body.decode_object(src, &object, header.api_version())?;
                Ok(Self::WatchRequest(ObjectRequest {
                    header,
                    object,
                    body,
                }))
            }
        }
    }
}


mod objects {

    use crate::topic::TopicSpec;
    use crate::spu::{SpuSpec, CustomSpuSpec};
    use crate::smartmodule::SmartModuleSpec;
    use crate::partition::PartitionSpec;
    use crate::connector::ManagedConnectorSpec;
    use crate::spg::SpuGroupSpec;
    use crate::table::TableSpec;

    use super::*;

    pub type ObjListRequest = ObjectRequest<ObjectDecoder, ObjectApiListRequest>;
    pub type ObjListResponse = ObjectRequest<ObjectDecoder, ObjectApiListResponse>;
    pub type ObjCreateRequest = ObjectRequest<CreateDecoder, ObjectApiCreateRequest>;
    pub type ObjWatchRequest = ObjectRequest<ObjectDecoder, ObjectApiWatchRequest>;

    macro_rules! ObjectApiEnum {
        ($api:ident) => {


            paste! {
                #[derive(Debug,Encoder)]
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
                    pub fn decode_object<T,O>(&mut self, src: &mut T, obj_ty: &O,version: Version) -> Result<(), IoError>
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
    ObjectApiEnum!(ListResponse);
    ObjectApiEnum!(WatchRequest);




    /// Most of Request except create which has special format
    #[derive(Default, Debug)]
    pub struct ObjectRequest<Obj, Body> {
        pub header: RequestHeader,
        pub object: Obj,
        pub body: Body,
    }

    impl <Obj,Body> ObjectRequest<Obj,Body> {

        pub fn get_header_request(self) -> (RequestHeader, Body) {
            (self.header, self.body)
        }

    }

    impl<Obj, Body> Encoder for ObjectRequest<Obj, Body>
    where
        Obj: Debug + Encoder,
        Body: Debug + Encoder,
    {
        fn write_size(&self, version: Version) -> usize {
            self.header.write_size(version)
                + self.object.write_size(self.header.api_version())
                + self.body.write_size(self.header.api_version())
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




    /// Most of Request except create which has special format
    #[derive(Default, Debug)]
    pub struct ObjectResponse<Obj, Body> {
        object: Obj,
        body: Body,
    }

    impl <Obj,Body> ObjectResponse<Obj,Body> {



    }

    impl<Obj, Body> Encoder for ObjectResponse<Obj, Body>
    where
        Obj: Debug + Encoder,
        Body: Debug + Encoder,
    {
        fn write_size(&self, version: Version) -> usize {
            self.object.write_size(version)
                + self.body.write_size(version)
        }

        fn encode<T>(&self, out: &mut T, version: Version) -> Result<(), IoError>
        where
            T: BufMut,
        {
            
            trace!("encoding object: {:#?}", &self.object);
            self.object.encode(out, version)?;

            trace!("encoding body: {:#?}", &self.body);
            self.body.encode(out, version)?;
            Ok(())
        }
    }



    pub trait AdminObjectDecoder: Debug {
        fn is_topic(&self) -> bool;
        fn is_spu(&self) -> bool;
        fn is_partition(&self) -> bool;
        fn is_smart_module(&self) -> bool;
    }

    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct ObjectDecoder{
        ty: String
    }

    impl AdminObjectDecoder for ObjectDecoder {
        fn is_topic(&self) -> bool {
            &self.ty == TopicSpec::LABEL
        }

        fn is_spu(&self) -> bool {
            &self.ty == SpuSpec::LABEL
        }

        fn is_partition(&self) -> bool {
            &self.ty == PartitionSpec::LABEL
        }

        fn is_smart_module(&self) -> bool {
            &self.ty == SmartModuleSpec::LABEL
        }
    }


    const TOPIC: u8 = 0;
    const CUSTOM_SPU: u8 = 1;
    const SPG: u8 = 2;
    const MANAGED_CONNECTOR: u8 = 3;
    const SMART_MODULE: u8 = 4;
    const TABLE: u8 = 5;

    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct CreateDecoder {
        ty: u8
    }

    impl AdminObjectDecoder for CreateDecoder {
        fn is_topic(&self) -> bool {
            self.ty == TOPIC
        }

        fn is_spu(&self) -> bool {
            false
        }

        fn is_partition(&self) -> bool {
            false
        }

        fn is_smart_module(&self) -> bool {
            self.ty == SMART_MODULE
        }
    }

}