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


                #[derive(Debug,Encoder)]
                pub enum [<ObjectApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    Spu($api<crate::spu::SpuSpec>),
                    CustomSpu($api<crate::customspu::CustomSpuSpec>),
                    SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                    Partition($api<crate::partition::PartitionSpec>),
                    ManagedConnector($api<crate::connector::ManagedConnectorSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    Table($api<crate::table::TableSpec>),
                    Empty()
                }

                impl Default for [<ObjectApi $api>] {
                    fn default() -> Self {
                        Self::Empty()
                    }
                }

                // We implement decode signature even thought this will be never called.
                // RequestMessage use decode_object.  But in order to provide backward compatibility, we pretend
                // to provide decode implementation but shoudl be never called
                impl  dataplane::core::Decoder for [<ObjectApi $api>] {

                    fn decode<T>(&mut self, _src: &mut T, _version: dataplane::core::Version) -> Result<(),std::io::Error>
                    where
                        T: dataplane::bytes::Buf
                    {
                        panic!("should not be called");
                    }

                }
            }
        }
    }

    /// Macro to generate callback from RequestMessage
    macro_rules! ObjectApiDecode {

        ($api:ident,$m:ident) => {


            paste::paste! {


                    fn decode_object<T>(&mut self, src: &mut T, mw: &$m ,version: dataplane::core::Version) -> Result<(), std::io::Error>
                    where
                        T: dataplane::bytes::Buf,

                    {
                        use crate::AdminObjectDecoder;

                        if mw.is_topic() {
                            let mut request = $api::<crate::topic::TopicSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::Topic(request);
                            return Ok(())
                        } else if mw.is_spu() {
                            let mut request = $api::<crate::spu::SpuSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::Spu(request);
                            return Ok(())
                        } else if mw.is_table() {
                            let mut request = $api::<crate::table::TableSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::Table(request);
                            return Ok(())
                        } else if mw.is_custom_spu() {
                            let mut request = $api::<crate::customspu::CustomSpuSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::CustomSpu(request);
                            return Ok(())
                        } else if mw.is_spg() {
                            let mut request = $api::<crate::spg::SpuGroupSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::SpuGroup(request);
                            return Ok(())
                        } else if mw.is_smart_module(){
                            let mut request = $api::<crate::smartmodule::SmartModuleSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::SmartModule(request);
                            return Ok(())
                        } else if mw.is_partition(){
                            let mut request = $api::<crate::partition::PartitionSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::Partition(request);
                            Ok(())
                        } else if mw.is_connector(){
                            let mut request = $api::<crate::connector::ManagedConnectorSpec>::default();
                            request.decode(src, version)?;
                            *self = Self::ManagedConnector(request);
                            Ok(())

                        } else  {

                            Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("invalid request type {:#?}", mw),
                            ))
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

                impl From<$from<[<$spec Spec>]>> for (crate::objects::[<ObjectApi $from>],crate::[<$dec Decoder>]) {
                    fn from(fr: $from<[<$spec Spec>]>) -> Self {
                        (
                            crate::objects::[<ObjectApi $from>]::$spec(fr),
                            [<$spec Spec>]::[<$dec:lower _decoder>](),
                        )
                    }
                }
            }
        };

        ($from:ident,$spec:ident) => {
            crate::objects::ObjectFrom!($from, $spec,Object);
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
    pub(crate) use ObjectApiDecode;
    pub(crate) use ObjectFrom;
    pub(crate) use ObjectTryFrom;
}
