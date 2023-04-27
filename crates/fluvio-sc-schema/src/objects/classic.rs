pub(crate) use object_macro::*;
pub(crate) use delete_macro::*;

/// carry from prev version for compatibility test
mod object_macro {

    /// Macro to objectify generic Request/Response for Admin Objects
    /// AdminSpec is difficult to turn into TraitObject due to associated types and use of other derived
    /// properties such as `PartialEq`.  This generates all possible variation of given API.  
    /// Not all variation will be constructed or used
    macro_rules! ClassicObjectApiEnum {
        ($api:ident) => {

            paste::paste! {


                #[derive(Debug)]
                pub enum [<ClassicObjectApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    Spu($api<crate::spu::SpuSpec>),
                    CustomSpu($api<crate::customspu::CustomSpuSpec>),
                    SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                    Partition($api<crate::partition::PartitionSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    TableFormat($api<crate::tableformat::TableFormatSpec>),
                }

                impl Default for [<ClassicObjectApi $api>] {
                    fn default() -> Self {
                        Self::Topic($api::<crate::topic::TopicSpec>::default())
                    }
                }

                impl [<ClassicObjectApi $api>] {
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

                impl  fluvio_protocol::Encoder for [<ClassicObjectApi $api>] {

                    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
                        let type_size = if version < crate::objects::DYN_OBJ {
                            self.type_string().to_owned().write_size(version)
                        } else {
                            0
                        };

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
                        if version < crate::objects::DYN_OBJ {
                            // only write header for classic object
                            let ty = self.type_string().to_owned();

                            tracing::trace!(%ty,len = self.write_size(version),"encoding objects");
                            ty.encode(dest, version)?;
                        }

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


                impl  fluvio_protocol::Decoder for [<ClassicObjectApi $api>] {

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
    macro_rules! ClassicObjectFrom {
        ($from:ident,$spec:ident) => {
            paste::paste! {

                impl From<$from<[<$spec Spec>]>> for crate::objects::[<ClassicObjectApi $from>] {
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
    macro_rules! ClassicObjectTryFrom {
        ($from:ident,$spec:ident) => {

            paste::paste! {

                impl std::convert::TryFrom<crate::objects::[<ClassicObjectApi $from>]> for $from<[<$spec Spec>]> {
                    type Error = std::io::Error;

                    fn try_from(response: crate::objects::[<ClassicObjectApi $from>]) -> Result<Self, Self::Error> {
                        match response {
                            crate::objects::[<ClassicObjectApi $from>]::$spec(response) => Ok(response),
                            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, concat!("not ",stringify!($spec)))),
                        }
                    }
                }
            }
        };
    }

    pub(crate) use ClassicObjectApiEnum;
    pub(crate) use ClassicObjectFrom;
    pub(crate) use ClassicObjectTryFrom;
}

mod delete_macro {

    /// Macro to for converting delete object to generic Delete
    macro_rules! ClassicDeleteApiEnum {
        ($api:ident) => {

            paste::paste! {

                #[derive(Debug)]
                pub enum [<ClassicObjectApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    CustomSpu($api<crate::customspu::CustomSpuSpec>),
                    SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    TableFormat($api<crate::tableformat::TableFormatSpec>),
                }

                impl Default for [<ClassicObjectApi $api>] {
                    fn default() -> Self {
                        Self::Topic($api::<crate::topic::TopicSpec>::default())
                    }
                }

                impl [<ClassicObjectApi $api>] {
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

                impl  fluvio_protocol::Encoder for [<ClassicObjectApi $api>] {

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


                impl  fluvio_protocol::Decoder for [<ClassicObjectApi $api>] {

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

    pub(crate) use ClassicDeleteApiEnum;
}

/// write decoder for classic api
macro_rules! ClassicDecoding {

    ($api:ident) => {

        paste::paste! {

            impl  fluvio_protocol::Decoder for [<ObjectApi $api>] {

                fn decode<T>(&mut self, src: &mut T, version: fluvio_protocol::Version) -> Result<(),std::io::Error>
                where
                    T: fluvio_protocol::bytes::Buf
                {
                    if version >= DYN_OBJ {
                        println!("decoding new");
                        self.0.decode(src, version)?;
                    } else {
                        println!("decoding classical");
                        use fluvio_protocol::Encoder;
                        let classic_obj = [<ClassicObjectApi $api>]::decode_from(src, version)?;
                        // reencode using new version
                      //  let bytes = classic_obj.as_bytes(version, COMMON_VERSION)?;
                      //  self.0.set_buf(bytes.into());
                    }
                    Ok(())
                }
            }
        }

    }
}

pub(crate) use ClassicDecoding;
