mod create;
mod delete;
mod list;
mod watch;

pub use create::*;
pub use delete::*;
pub use list::*;
pub use watch::*;
pub use metadata::*;

pub use crate::NameFilter;
pub(crate) use object_macro::*;
pub(crate) use create_macro::*;

mod metadata {

    use std::convert::{TryFrom, TryInto};
    use std::fmt::{Debug, Display};
    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use dataplane::core::{Encoder, Decoder};

    use fluvio_controlplane_metadata::store::MetadataStoreObject;
    use fluvio_controlplane_metadata::core::{MetadataContext, MetadataItem};

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
}

mod object_macro {

    /// Macro to objectify generic Request/Response for Admin Objects
    /// AdminSpec is difficult to turn into TraitObject due to associated types and use of other derived
    /// properties such as `PartialEq`.  This generates all possible variation of given API.  
    /// Not all variation will be constructed or used.  It is possible that invalid combination could be
    /// constructed (for example, Creating SPU) but it is not possible when using client API
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
                    ManagedConnector($api<crate::connector::ManagedConnectorSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    Table($api<crate::table::TableSpec>),
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
                            Self::ManagedConnector(_) => crate::connector::ManagedConnectorSpec::LABEL,
                            Self::SpuGroup(_) => crate::spg::SpuGroupSpec::LABEL,
                            Self::Table(_) => crate::table::TableSpec::LABEL,
                        }
                    }
                }

                impl  dataplane::core::Encoder for [<ObjectApi $api>] {

                    fn write_size(&self, version: dataplane::core::Version) -> usize {
                        let type_size = self.type_string().to_owned().write_size(version);

                        type_size
                            + match self {
                                Self::Topic(s) => s.write_size(version),
                                Self::Spu(s) => s.write_size(version),
                                Self::CustomSpu(s) => s.write_size(version),
                                Self::Partition(s) => s.write_size(version),
                                Self::SmartModule(s) => s.write_size(version),
                                Self::ManagedConnector(s) => s.write_size(version),
                                Self::SpuGroup(s) => s.write_size(version),
                                Self::Table(s) => s.write_size(version),
                            }
                    }

                    fn encode<T>(&self, dest: &mut T, version: dataplane::core::Version) -> Result<(), std::io::Error>
                    where
                        T: dataplane::bytes::BufMut,
                    {
                        self.type_string().to_owned().encode(dest, version)?;

                        match self {
                            Self::Topic(s) => s.encode(dest, version)?,
                            Self::CustomSpu(s) => s.encode(dest, version)?,
                            Self::SpuGroup(s) => s.encode(dest, version)?,
                            Self::Spu(s) => s.encode(dest, version)?,
                            Self::Partition(s) => s.encode(dest, version)?,
                            Self::ManagedConnector(s) => s.encode(dest, version)?,
                            Self::SmartModule(s) => s.encode(dest, version)?,
                            Self::Table(s) => s.encode(dest, version)?
                        }

                        Ok(())
                    }

                }


                // We implement decode signature even thought this will be never called.
                // RequestMessage use decode_object.  But in order to provide backward compatibility, we pretend
                // to provide decode implementation but shoudl be never called
                impl  dataplane::core::Decoder for [<ObjectApi $api>] {

                    fn decode<T>(&mut self, src: &mut T, version: dataplane::core::Version) -> Result<(),std::io::Error>
                    where
                        T: dataplane::bytes::Buf
                    {
                        use fluvio_controlplane_metadata::core::Spec;

                        let mut typ = "".to_owned();
                        typ.decode(src, version)?;
                        tracing::trace!("decoded type: {}", typ);

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

                            crate::table::TableSpec::LABEL => {
                                tracing::trace!("detected table");
                                let mut request = $api::<crate::table::TableSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::Table(request);
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

                            crate::connector::ManagedConnectorSpec::LABEL => {
                                tracing::trace!("detected connector");
                                let mut request = $api::<crate::connector::ManagedConnectorSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::ManagedConnector(request);
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
    /// This conversion is possible because ObjectAPI (ex: ObjectApiListRequst) is built on Enum with matching object
    /// which make it possible to convert ListRequest<TopicSpec> => ObjectApiListRequest::Topic(req)
    /// This should generate code such as:
    /// impl From<WatchRequest<TopicSpec>> for (ObjectApiWatchRequest, ObjectDecoder) {
    /// fn from(req: WatchRequest<TopicSpec>) -> Self {
    ///    (
    ///       ObjectApiWatchRequest::Topic(req),
    ///        TopicSpec::object_decoder(),
    ///    )
    /// }
    /// ObjectFrom!(WatchRequest, Topic);
    macro_rules! ObjectFrom {
        ($from:ident,$spec:ident,$dec:ty) => {
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

    macro_rules! ObjectTryFrom {
        ($from:ident,$spec:ident,$dec:ty) => {

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

        ($from:ident,$spec:ident) => {

            crate::objects::ObjectTryFrom!($from,$spec,crate::ObjectDecoder);
        }
    }

    pub(crate) use ObjectApiEnum;
    pub(crate) use ObjectFrom;
    pub(crate) use ObjectTryFrom;
}

mod create_macro {

    /// Macro to objectify generic Request/Response for Admin Objects
    /// AdminSpec is difficult to turn into TraitObject due to associated types and use of other derived
    /// properties such as `PartialEq`.  This generates all possible variation of given API.  
    /// Not all variation will be constructed or used.  It is possible that invalid combination could be
    /// constructed (for example, Creating SPU) but it is not possible when using client API
    macro_rules! CreateApiEnum {
        ($api:ident) => {

            paste::paste! {


                #[derive(Debug)]
                pub enum [<ObjectApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    CustomSpu($api<crate::customspu::CustomSpuSpec>),
                    SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                    ManagedConnector($api<crate::connector::ManagedConnectorSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    Table($api<crate::table::TableSpec>),
                }

                impl Default for [<ObjectApi $api>] {
                    fn default() -> Self {
                        Self::Topic($api::<crate::topic::TopicSpec>::default())
                    }
                }

                impl [<ObjectApi $api>] {
                    fn type_value(&self) -> u8 {
                        //use fluvio_controlplane_metadata::core::Spec;
                        match self {
                            Self::Topic(_) => crate::topic::TopicSpec::CREATE_TYPE,
                            Self::CustomSpu(_) => crate::customspu::CustomSpuSpec::CREATE_TYPE,
                            Self::SmartModule(_) => crate::smartmodule::SmartModuleSpec::CREATE_TYPE,
                            Self::ManagedConnector(_) => crate::connector::ManagedConnectorSpec::CREATE_TYPE,
                            Self::SpuGroup(_) => crate::spg::SpuGroupSpec::CREATE_TYPE,
                            Self::Table(_) => crate::table::TableSpec::CREATE_TYPE,
                        }
                    }
                }

                impl  dataplane::core::Encoder for [<ObjectApi $api>] {

                    fn write_size(&self, version: dataplane::core::Version) -> usize {
                        let type_size = (0u8).write_size(version);

                        type_size
                            + match self {
                                Self::Topic(s) => s.write_size(version),
                                Self::CustomSpu(s) => s.write_size(version),
                                Self::SmartModule(s) => s.write_size(version),
                                Self::ManagedConnector(s) => s.write_size(version),
                                Self::SpuGroup(s) => s.write_size(version),
                                Self::Table(s) => s.write_size(version),
                            }
                    }

                    fn encode<T>(&self, dest: &mut T, version: dataplane::core::Version) -> Result<(), std::io::Error>
                    where
                        T: dataplane::bytes::BufMut,
                    {

                        self.type_value().encode(dest, version)?;
                        match self {
                            Self::Topic(s) => s.encode(dest, version)?,
                            Self::CustomSpu(s) => s.encode(dest, version)?,
                            Self::ManagedConnector(s) => s.encode(dest, version)?,
                            Self::SmartModule(s) => s.encode(dest, version)?,
                            Self::SpuGroup(s) => s.encode(dest,version)?,
                            Self::Table(s) => s.encode(dest, version)?,
                        }

                        Ok(())
                    }

                }


                // We implement decode signature even thought this will be never called.
                // RequestMessage use decode_object.  But in order to provide backward compatibility, we pretend
                // to provide decode implementation but shoudl be never called
                impl  dataplane::core::Decoder for [<ObjectApi $api>] {

                    fn decode<T>(&mut self, src: &mut T, version: dataplane::core::Version) -> Result<(),std::io::Error>
                    where
                        T: dataplane::bytes::Buf
                    {
                        //use fluvio_controlplane_metadata::core::Spec;

                        let mut typ: u8 = 0;
                        typ.decode(src, version)?;
                        tracing::trace!("decoded type: {}", typ);


                        match typ {
                            crate::topic::TopicSpec::CREATE_TYPE => {
                                tracing::trace!("detected topic");
                                let mut request = $api::<crate::topic::TopicSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::Topic(request);
                                return Ok(())
                            }

                            crate::table::TableSpec::CREATE_TYPE => {
                                tracing::trace!("detected table");
                                let mut request = $api::<crate::table::TableSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::Table(request);
                                return Ok(())
                            }

                            crate::customspu::CustomSpuSpec::CREATE_TYPE => {
                                tracing::trace!("detected custom spu");
                                let mut request = $api::<crate::customspu::CustomSpuSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::CustomSpu(request);
                                return Ok(())
                            }

                            crate::spg::SpuGroupSpec::CREATE_TYPE => {
                                tracing::trace!("detected custom spu");
                                let mut request = $api::<crate::spg::SpuGroupSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::SpuGroup(request);
                                return Ok(())
                            }

                            crate::smartmodule::SmartModuleSpec::CREATE_TYPE => {
                                tracing::trace!("detected smartmodule");
                                let mut request = $api::<crate::smartmodule::SmartModuleSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::SmartModule(request);
                                return Ok(())
                            }

                            crate::connector::ManagedConnectorSpec::CREATE_TYPE => {
                                tracing::trace!("detected connector");
                                let mut request = $api::<crate::connector::ManagedConnectorSpec>::default();
                                request.decode(src, version)?;
                                *self = Self::ManagedConnector(request);
                                Ok(())
                            }

                            // Unexpected type
                            _ => Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("invalid create type {:#?}", typ),
                            ))
                        }
                    }

                }
            }
        }
    }

    pub(crate) use CreateApiEnum;
}

#[cfg(test)]
mod test {

    use std::io::Cursor;

    use dataplane::api::{RequestHeader, RequestMessage, ResponseMessage};
    use dataplane::core::{Encoder, Decoder};
    use dataplane::api::Request;

    use crate::objects::{
        CreateRequest, ListRequest, MetadataUpdate, ObjectApiCreateRequest, ObjectApiListRequest,
        ObjectApiWatchRequest, ObjectApiWatchResponse, WatchResponse,
    };

    use crate::topic::TopicSpec;
    use crate::customspu::CustomSpuSpec;

    fn create_req() -> ObjectApiListRequest {
        let list_request: ListRequest<TopicSpec> = ListRequest::new(vec![]);
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
        use dataplane::api::Request;

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
        println!("output: {:#?}", src);
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

        println!("output: {:#?}", src);

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
    fn test_create_encode_decoding() {
        use dataplane::api::Request;

        let create: CreateRequest<CustomSpuSpec> = CreateRequest {
            name: "test".to_string(),
            dry_run: false,
            spec: CustomSpuSpec::default(),
        };

        let req: ObjectApiCreateRequest = create.into();

        let mut req_msg = RequestMessage::new_request(req);
        req_msg
            .get_mut_header()
            .set_client_id("test")
            .set_api_version(ObjectApiCreateRequest::API_KEY as i16);

        let mut src = vec![];
        req_msg.encode(&mut src, 0).expect("encoding");

        let dec_msg: RequestMessage<ObjectApiCreateRequest> = RequestMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiCreateRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(matches!(
            dec_msg.request,
            ObjectApiCreateRequest::CustomSpu(_)
        ));
    }
}
