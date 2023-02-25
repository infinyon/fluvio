mod create;
mod delete;
mod list;
mod watch;

pub use create::*;
pub use delete::*;
pub use list::*;
pub use watch::*;
pub use metadata::*;

pub(crate) use object_macro::*;
pub(crate) use delete_macro::*;

pub(crate) const COMMON_VERSION: i16 = 10; // from now, we use a single version for all objects

mod metadata {

    use std::convert::{TryFrom, TryInto};
    use std::fmt::{Debug, Display};
    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use fluvio_protocol::{Encoder, Decoder};

    use fluvio_controlplane_metadata::store::MetadataStoreObject;
    use fluvio_controlplane_metadata::core::{MetadataContext, MetadataItem};

    use crate::AdminSpec;
    use crate::core::Spec;

    #[derive(Encoder, Decoder, Default, Clone, Debug)]
    #[cfg_attr(
        feature = "use_serde",
        derive(serde::Serialize, serde::Deserialize),
        serde(rename_all = "camelCase")
    )]
    pub struct Metadata<S>
    where
        S: Spec + Encoder + Decoder,
        S::Status: Encoder + Decoder,
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

    impl<S> Metadata<S>
    where
        S: AdminSpec + Encoder + Decoder,
        S::Status: Encoder + Decoder,
    {
        pub fn summary(self) -> Self {
            Self {
                name: self.name,
                spec: self.spec.summary(),
                status: self.status,
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
                    IoError::new(ErrorKind::InvalidData, format!("problem converting: {err}"))
                })?,
                ctx: MetadataContext::default(),
            })
        }
    }
}

mod object_macro {

    /// Macro to objectify generic Request/Response for Admin Objects
    /// AdminSpec is difficult to turn into TraitObject due to associated types and use of other derived
    /// properties such as `PartialEq`.  This generates all possible variation of given API.  
    /// Not all variation will be constructed or used
    macro_rules! ObjectApiEnum {
        ($api:ident) => {

            paste::paste! {


                #[derive(Debug)]
                pub enum [<ObjectApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    Spu($api<crate::spu::SpuSpec>),
                    CustomSpu($api<crate::customspu::CustomSpuSpec>),
                    SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                    Partition($api<crate::partition::PartitionSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    TableFormat($api<crate::tableformat::TableFormatSpec>),
                }

                impl Default for [<ObjectApi $api>] {
                    fn default() -> Self {
                        Self::Topic($api::<crate::topic::TopicSpec>::default())
                    }
                }

                impl [<ObjectApi $api>] {
                    fn type_string(&self) -> &'static str {
                        use fluvio_controlplane_metadata::core::Spec;
                        match self {
                            Self::Topic(_) => crate::topic::TopicSpec::LABEL,
                            Self::Spu(_) => crate::spu::SpuSpec::LABEL,
                            Self::CustomSpu(_) => crate::customspu::CustomSpuSpec::LABEL,
                            Self::SmartModule(_) => crate::smartmodule::SmartModuleSpec::LABEL,
                            Self::Partition(_) => crate::partition::PartitionSpec::LABEL,
                            Self::SpuGroup(_) => crate::spg::SpuGroupSpec::LABEL,
                            Self::TableFormat(_) => crate::tableformat::TableFormatSpec::LABEL,

                        }
                    }
                }

                impl  fluvio_protocol::Encoder for [<ObjectApi $api>] {

                    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
                        let type_size = self.type_string().to_owned().write_size(version);

                        type_size
                            + match self {
                                Self::Topic(s) => s.write_size(version),
                                Self::Spu(s) => s.write_size(version),
                                Self::CustomSpu(s) => s.write_size(version),
                                Self::Partition(s) => s.write_size(version),
                                Self::SmartModule(s) => s.write_size(version),
                                Self::SpuGroup(s) => s.write_size(version),
                                Self::TableFormat(s) => s.write_size(version),
                            }
                    }

                    fn encode<T>(&self, dest: &mut T, version: fluvio_protocol::Version) -> Result<(), std::io::Error>
                    where
                        T: fluvio_protocol::bytes::BufMut,
                    {
                        let ty = self.type_string().to_owned();

                        tracing::trace!(%ty,len = self.write_size(version),"encoding objects");
                        ty.encode(dest, version)?;

                        match self {
                            Self::Topic(s) => s.encode(dest, version)?,
                            Self::CustomSpu(s) => s.encode(dest, version)?,
                            Self::SpuGroup(s) => s.encode(dest, version)?,
                            Self::Spu(s) => s.encode(dest, version)?,
                            Self::Partition(s) => s.encode(dest, version)?,
                            Self::SmartModule(s) => s.encode(dest, version)?,
                            Self::TableFormat(s) => s.encode(dest, version)?,
                        }

                        Ok(())
                    }

                }


                impl  fluvio_protocol::Decoder for [<ObjectApi $api>] {

                    fn decode<T>(&mut self, src: &mut T, version: fluvio_protocol::Version) -> Result<(),std::io::Error>
                    where
                        T: fluvio_protocol::bytes::Buf
                    {
                        use fluvio_controlplane_metadata::core::Spec;

                        let mut typ = "".to_owned();
                        typ.decode(src, version)?;
                        tracing::trace!(%typ,"decoded type");

                        match typ.as_ref() {
                            crate::topic::TopicSpec::LABEL => {
                                tracing::trace!("detected topic");
                                let mut request = $api::<crate::topic::TopicSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::Topic(request);
                                return Ok(())
                            }

                            crate::spu::SpuSpec::LABEL  => {
                                tracing::trace!("detected spu");
                                let mut request = $api::<crate::spu::SpuSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::Spu(request);
                                return Ok(())
                            }

                            crate::tableformat::TableFormatSpec::LABEL => {
                                tracing::trace!("detected tableformat");
                                let mut request = $api::<crate::tableformat::TableFormatSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::TableFormat(request);
                                return Ok(())
                            }

                            crate::customspu::CustomSpuSpec::LABEL => {
                                tracing::trace!("detected custom spu");
                                let mut request = $api::<crate::customspu::CustomSpuSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::CustomSpu(request);
                                return Ok(())
                            }

                            crate::spg::SpuGroupSpec::LABEL => {
                                tracing::trace!("detected custom spu");
                                let mut request = $api::<crate::spg::SpuGroupSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::SpuGroup(request);
                                return Ok(())
                            }

                            crate::smartmodule::SmartModuleSpec::LABEL => {
                                tracing::trace!("detected smartmodule");
                                let mut request = $api::<crate::smartmodule::SmartModuleSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::SmartModule(request);
                                return Ok(())
                            }

                            crate::partition::PartitionSpec::LABEL => {
                                tracing::trace!("detected partition");
                                let mut request = $api::<crate::partition::PartitionSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::Partition(request);
                                Ok(())
                            }


                            // Unexpected type
                            _ => Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("invalid object type {:#?}", typ),
                            ))
                        }
                    }

                }
            }
        }
    }

    /// Macro to convert request with generic signature with ObjectAPI which is non generic which then can be transported
    /// over network.
    /// This conversion is possible because ObjectAPI (ex: ObjectApiListRequest) is built on Enum with matching object
    /// which make it possible to convert ListRequest<TopicSpec> => ObjectApiListRequest::Topic(req)
    /// This should generate code such as:
    /// impl From<WatchRequest<TopicSpec>> for ObjectApiWatchRequest {
    /// fn from(req: WatchRequest<TopicSpec>) -> Self {
    ///       ObjectApiWatchRequest::Topic(req
    /// }
    /// ObjectFrom!(WatchRequest, Topic);
    macro_rules! ObjectFrom {
        ($from:ident,$spec:ident) => {
            paste::paste! {

                impl From<$from<[<$spec Spec>]>> for crate::objects::[<ObjectApi $from>] {
                    fn from(fr: $from<[<$spec Spec>]>) -> Self {
                        crate::objects::[<ObjectApi $from>]::$spec(fr)
                    }
                }
            }
        };

        ($from:ident,$spec:ident) => {
            crate::objects::ObjectFrom!($from, $spec, Object);
        };
    }

    /// Convert unknown object type to ObjectApi<T>
    /// Since we don't know the type of object, we perform Try
    macro_rules! ObjectTryFrom {
        ($from:ident,$spec:ident) => {

            paste::paste! {

                impl std::convert::TryFrom<crate::objects::[<ObjectApi $from>]> for $from<[<$spec Spec>]> {
                    type Error = std::io::Error;

                    fn try_from(response: crate::objects::[<ObjectApi $from>]) -> Result<Self, Self::Error> {
                        match response {
                            crate::objects::[<ObjectApi $from>]::$spec(response) => Ok(response),
                            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, concat!("not ",stringify!($spec)))),
                        }
                    }
                }
            }
        };
    }

    pub(crate) use ObjectApiEnum;
    pub(crate) use ObjectFrom;
    pub(crate) use ObjectTryFrom;
}

mod delete_macro {

    /// Macro to for converting delete object to generic Delete
    macro_rules! DeleteApiEnum {
        ($api:ident) => {

            paste::paste! {

                #[derive(Debug)]
                pub enum [<ObjectApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    CustomSpu($api<crate::customspu::CustomSpuSpec>),
                    SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    TableFormat($api<crate::tableformat::TableFormatSpec>),
                }

                impl Default for [<ObjectApi $api>] {
                    fn default() -> Self {
                        Self::Topic($api::<crate::topic::TopicSpec>::default())
                    }
                }

                impl [<ObjectApi $api>] {
                    fn type_string(&self) -> &'static str {
                        use fluvio_controlplane_metadata::core::Spec;
                        match self {
                            Self::Topic(_) => crate::topic::TopicSpec::LABEL,
                            Self::CustomSpu(_) => crate::customspu::CustomSpuSpec::LABEL,
                            Self::SmartModule(_) => crate::smartmodule::SmartModuleSpec::LABEL,
                            Self::SpuGroup(_) => crate::spg::SpuGroupSpec::LABEL,
                            Self::TableFormat(_) => crate::tableformat::TableFormatSpec::LABEL,
                        }
                    }
                }

                impl  fluvio_protocol::Encoder for [<ObjectApi $api>] {

                    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
                        let type_size = self.type_string().to_owned().write_size(version);

                        type_size
                            + match self {
                                Self::Topic(s) => s.write_size(version),
                                Self::CustomSpu(s) => s.write_size(version),
                                Self::SmartModule(s) => s.write_size(version),
                                Self::SpuGroup(s) => s.write_size(version),
                                Self::TableFormat(s) => s.write_size(version),
                            }
                    }

                    fn encode<T>(&self, dest: &mut T, version: fluvio_protocol::Version) -> Result<(), std::io::Error>
                    where
                        T: fluvio_protocol::bytes::BufMut,
                    {
                        let ty = self.type_string().to_owned();

                        tracing::trace!(%ty,len = self.write_size(version),"encoding objects");
                        ty.encode(dest, version)?;

                        match self {
                            Self::Topic(s) => s.encode(dest, version)?,
                            Self::CustomSpu(s) => s.encode(dest, version)?,
                            Self::SpuGroup(s) => s.encode(dest, version)?,
                            Self::SmartModule(s) => s.encode(dest, version)?,
                            Self::TableFormat(s) => s.encode(dest, version)?,
                        }

                        Ok(())
                    }

                }


                impl  fluvio_protocol::Decoder for [<ObjectApi $api>] {

                    fn decode<T>(&mut self, src: &mut T, version: fluvio_protocol::Version) -> Result<(),std::io::Error>
                    where
                        T: fluvio_protocol::bytes::Buf
                    {
                        use fluvio_controlplane_metadata::core::Spec;

                        let mut typ = "".to_owned();
                        typ.decode(src, version)?;
                        tracing::trace!(%typ,"decoded type");

                        match typ.as_ref() {
                            crate::topic::TopicSpec::LABEL => {
                                tracing::trace!("detected topic");
                                let mut request = $api::<crate::topic::TopicSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::Topic(request);
                                return Ok(())
                            }

                            crate::tableformat::TableFormatSpec::LABEL => {
                                tracing::trace!("detected tableformat");
                                let mut request = $api::<crate::tableformat::TableFormatSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::TableFormat(request);
                                return Ok(())
                            }

                            crate::customspu::CustomSpuSpec::LABEL => {
                                tracing::trace!("detected custom spu");
                                let mut request = $api::<crate::customspu::CustomSpuSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::CustomSpu(request);
                                return Ok(())
                            }

                            crate::spg::SpuGroupSpec::LABEL => {
                                tracing::trace!("detected custom spu");
                                let mut request = $api::<crate::spg::SpuGroupSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::SpuGroup(request);
                                return Ok(())
                            }

                            crate::smartmodule::SmartModuleSpec::LABEL => {
                                tracing::trace!("detected smartmodule");
                                let mut request = $api::<crate::smartmodule::SmartModuleSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::SmartModule(request);
                                Ok(())
                            },


                            // Unexpected type
                            _ => Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("invalid object type {:#?}", typ),
                            ))
                        }
                    }

                }
            }
        }
    }

    pub(crate) use DeleteApiEnum;
}

#[cfg(test)]
mod test {

    use std::convert::TryInto;
    use std::io::Cursor;

    use fluvio_protocol::api::{RequestHeader, RequestMessage, ResponseMessage};
    use fluvio_protocol::{Encoder, Decoder};
    use fluvio_protocol::api::Request;
    use fluvio_controlplane_metadata::spu::SpuStatus;

    use crate::objects::{
        ObjectApiListResponse, ListRequest, ListResponse, Metadata, MetadataUpdate,
        ObjectApiListRequest, ObjectApiWatchRequest, ObjectApiWatchResponse, WatchResponse,
    };

    use crate::topic::TopicSpec;
    use crate::customspu::CustomSpuSpec;

    fn create_req() -> ObjectApiListRequest {
        let list_request: ListRequest<TopicSpec> = ListRequest::new(vec![], false);
        list_request.into()
    }

    fn create_res() -> ObjectApiWatchResponse {
        let update = MetadataUpdate {
            epoch: 2,
            changes: vec![],
            all: vec![],
        };
        let watch_response: WatchResponse<TopicSpec> = WatchResponse::new(update);
        watch_response.into()
    }

    #[test]
    fn test_from() {
        let req = create_req();

        assert!(matches!(req, ObjectApiListRequest::Topic(_)));
    }

    #[test]
    fn test_encode_decoding() {
        use fluvio_protocol::api::Request;

        let req = create_req();

        let mut req_msg = RequestMessage::new_request(req);
        req_msg
            .get_mut_header()
            .set_client_id("test")
            .set_api_version(ObjectApiListRequest::API_KEY as i16);

        let mut src = vec![];
        req_msg.encode(&mut src, 0).expect("encoding");

        let dec_msg: RequestMessage<ObjectApiListRequest> = RequestMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiListRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(matches!(dec_msg.request, ObjectApiListRequest::Topic(_)));
    }

    // test encoding and decoding of metadata update
    #[test]
    fn test_watch_response_encoding() {
        fluvio_future::subscriber::init_logger();
        let update = MetadataUpdate {
            epoch: 2,
            changes: vec![],
            all: vec![],
        };
        let watch_response: WatchResponse<TopicSpec> = WatchResponse::new(update);

        let mut src = vec![];
        watch_response
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");
        //watch_response.encode(&mut src, 0).expect("encoding");
        println!("output: {src:#?}");
        let dec = WatchResponse::<TopicSpec>::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        assert_eq!(dec.inner().epoch, 2);
    }

    #[test]
    fn test_obj_watch_response_encode_decoding() {
        fluvio_future::subscriber::init_logger();

        let res = create_res();

        let mut header = RequestHeader::new(ObjectApiWatchRequest::API_KEY);
        header.set_client_id("test");
        header.set_correlation_id(11);
        let res_msg = ResponseMessage::from_header(&header, res);

        let mut src = vec![];
        res_msg
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");

        println!("output: {src:#?}");

        assert_eq!(
            src.len(),
            res_msg.write_size(ObjectApiWatchRequest::API_KEY as i16)
        );

        let dec_msg: ResponseMessage<ObjectApiWatchResponse> = ResponseMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(matches!(dec_msg.response, ObjectApiWatchResponse::Topic(_)));
    }

    #[test]
    fn test_obj_watch_api_decoding() {
        fluvio_future::subscriber::init_logger();

        let res = create_res();

        let mut header = RequestHeader::new(ObjectApiWatchRequest::API_KEY);
        header.set_client_id("test");
        header.set_correlation_id(11);
        let res_msg = ResponseMessage::from_header(&header, res);

        let mut src = vec![];
        res_msg
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");

        println!("output: {src:#?}");

        assert_eq!(
            src.len(),
            res_msg.write_size(ObjectApiWatchRequest::API_KEY as i16)
        );

        let dec_msg: ResponseMessage<ObjectApiWatchResponse> = ResponseMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(matches!(dec_msg.response, ObjectApiWatchResponse::Topic(_)));
    }

    #[test]
    fn test_list_response_encode_decoding() {
        use fluvio_protocol::api::Request;

        fluvio_future::subscriber::init_logger();

        let list = ListResponse::<CustomSpuSpec>::new(vec![Metadata {
            name: "test".to_string(),
            spec: CustomSpuSpec::default(),
            status: SpuStatus::default(),
        }]);

        let resp: ObjectApiListResponse = list.into();

        let mut header = RequestHeader::new(ObjectApiListRequest::API_KEY);
        header.set_client_id("test");
        header.set_correlation_id(11);
        let res_msg = ResponseMessage::from_header(&header, resp);
        let mut src = vec![];
        res_msg.encode(&mut src, 0).expect("encoding");

        println!("output: {src:#?}");

        let dec_msg: ResponseMessage<ObjectApiListResponse> = ResponseMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiListRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(matches!(
            dec_msg.response,
            ObjectApiListResponse::CustomSpu(_)
        ));

        let list_res: ListResponse<CustomSpuSpec> = dec_msg.response.try_into().expect("extract");
        assert_eq!(list_res.inner().len(), 1);
    }
}
