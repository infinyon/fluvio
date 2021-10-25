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
pub(crate) use object_macro::{ObjectApiEnum,ObjectApiDecode};

mod metadata {

    use std::convert::TryFrom;
    use std::convert::TryInto;
    use std::fmt::{Display, Debug};
    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use dataplane::core::{Encoder, Decoder};

    use fluvio_controlplane_metadata::store::MetadataStoreObject;
    use fluvio_controlplane_metadata::core::{MetadataItem, MetadataContext};

    use crate::core::Spec;

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
}

mod object_macro {

    
   // pub type ObjCreateRequest = ObjectRequest<CreateDecoder, ObjectApiCreateRequest>;
   // pub type ObjWatchRequest = ObjectRequest<ObjectDecoder, ObjectApiWatchRequest>;
   // pub type ObjWatchResponse = ObjectResponse<ObjectDecoder, ObjectApiWatchRequest>;



    macro_rules! ObjectApiEnum {
        ($api:ident) => {

            paste::paste! {
                

                #[derive(Debug,Encoder)]
                pub enum [<ObjectApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    Spu($api<crate::spu::SpuSpec>),
                    CustomSpu($api<crate::spu::CustomSpuSpec>),
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

    pub(crate) use ObjectApiEnum;
    pub(crate) use ObjectApiDecode;

}