pub(crate) use object_macro::*;
pub(crate) use delete_macro::*;
pub(crate) use create::*;

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

                        // only write header for classical version
                        if version < crate::objects::DYN_OBJ {
                            // only write header for classic object
                            let ty = self.type_string().to_owned();

                            tracing::debug!(%ty,len = self.write_size(version),"encoding as new object");
                            ty.encode(dest, version)?;
                        }

                        tracing::debug!(version,"encoding class api encoding");
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

    /// write decoder for classic api
    macro_rules! ClassicDecoding {

        ($api:ident) => {

            paste::paste! {

                impl  fluvio_protocol::Decoder for [<ObjectApi $api>] {

                    fn decode<T>(&mut self, src: &mut T, version: fluvio_protocol::Version) -> Result<(),std::io::Error>
                    where
                        T: fluvio_protocol::bytes::Buf
                    {
                        if version >= crate::objects::DYN_OBJ {
                            tracing::debug!("decoding new");
                            self.0.decode(src, version)?;
                        } else {

                            use fluvio_protocol::Encoder;
                            let classic_obj = [<ClassicObjectApi $api>]::decode_from(src, version)?;
                            tracing::debug!(ty = classic_obj.type_string(),version,"decoding as classical");
                            // reencode as DYN OBJECT to strip out type
                            self.0.set_buf(crate::objects::DYN_OBJ,classic_obj.type_string().to_owned(),classic_obj.as_bytes(crate::objects::DYN_OBJ)?.into());
                        }
                        Ok(())
                    }
                }
            }

        }
    }

    pub(crate) use ClassicObjectApiEnum;
    pub(crate) use ClassicDecoding;
}

/// carry from prev version for compatibility test
mod delete_macro {

    // same as ClassicObjectApi but with limited type that can be deleted
    macro_rules! ClassicDeleteApiEnum {
        ($api:ident) => {

            paste::paste! {


                #[derive(Debug)]
                pub enum [<ClassicDeleteApi $api>] {
                    Topic($api<crate::topic::TopicSpec>),
                    CustomSpu($api<crate::customspu::CustomSpuSpec>),
                    SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                    SpuGroup($api<crate::spg::SpuGroupSpec>),
                    TableFormat($api<crate::tableformat::TableFormatSpec>),
                }

                impl Default for [<ClassicDeleteApi $api>] {
                    fn default() -> Self {
                        Self::Topic($api::<crate::topic::TopicSpec>::default())
                    }
                }

                impl [<ClassicDeleteApi $api>] {
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

                impl  fluvio_protocol::Encoder for [<ClassicDeleteApi $api>] {

                    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
                        let type_size = if version < crate::objects::DYN_OBJ {
                            self.type_string().to_owned().write_size(version)
                        } else {
                            0
                        };

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

                        // only write header for classical version
                        if version < crate::objects::DYN_OBJ {
                            // only write header for classic object
                            let ty = self.type_string().to_owned();

                            tracing::debug!(%ty,len = self.write_size(version),"encoding as new object");
                            ty.encode(dest, version)?;
                        }

                        tracing::debug!(version,"encoding class api encoding");
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


                impl  fluvio_protocol::Decoder for [<ClassicDeleteApi $api>] {

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
                                return Ok(())
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

    macro_rules! ClassicDecodingDelete {

        ($api:ident) => {

            paste::paste! {

                impl  fluvio_protocol::Decoder for [<ObjectApi $api>] {

                    fn decode<T>(&mut self, src: &mut T, version: fluvio_protocol::Version) -> Result<(),std::io::Error>
                    where
                        T: fluvio_protocol::bytes::Buf
                    {
                        if version >= crate::objects::DYN_OBJ {
                            tracing::debug!("decoding new");
                            self.0.decode(src, version)?;
                        } else {

                            use fluvio_protocol::Encoder;
                            let classic_obj = [<ClassicDeleteApi $api>]::decode_from(src, version)?;
                            tracing::debug!(ty = classic_obj.type_string(),version,"decoding as classical");
                            // reencode as DYN OBJECT to strip out type
                            self.0.set_buf(crate::objects::DYN_OBJ,classic_obj.type_string().to_owned(),classic_obj.as_bytes(crate::objects::DYN_OBJ)?.into());
                        }
                        Ok(())
                    }
                }
            }

        }
    }

    pub(crate) use ClassicDeleteApiEnum;
    pub(crate) use ClassicDecodingDelete;
}

mod create {

    use anyhow::{anyhow, Result};

    use fluvio_controlplane_metadata::mirror::MirrorSpec;
    use fluvio_protocol::bytes::{BufMut, Buf};
    use fluvio_protocol::{Encoder, Decoder};
    use fluvio_protocol::Version;

    use crate::objects::CommonCreateRequest;
    use crate::topic::TopicSpec;
    use crate::customspu::CustomSpuSpec;
    use crate::smartmodule::SmartModuleSpec;
    use crate::tableformat::TableFormatSpec;
    use crate::spg::SpuGroupSpec;

    #[derive(Debug, Default, Encoder, Decoder)]
    pub struct ClassicObjectApiCreateRequest {
        pub common: CommonCreateRequest,
        pub request: ClassicObjectCreateRequest,
    }

    #[doc(hidden)]
    pub trait ClassicCreatableAdminSpec: Sized {
        const CREATE_TYPE: u8 = 0;

        // conversion to classic protocol wrapper
        fn try_classic_convert(_spec: Self) -> Result<ClassicObjectCreateRequest> {
            Err(anyhow!("not implemented"))
        }

        fn try_convert_from_classic(_request: ClassicObjectCreateRequest) -> Option<Self> {
            None
        }
    }

    #[derive(Debug)]
    pub enum ClassicObjectCreateRequest {
        Topic(TopicSpec),
        CustomSpu(CustomSpuSpec),
        SmartModule(SmartModuleSpec),
        SpuGroup(SpuGroupSpec),
        TableFormat(TableFormatSpec),
        MirrorFormat(MirrorSpec),
    }

    impl Default for ClassicObjectCreateRequest {
        fn default() -> Self {
            Self::Topic(TopicSpec::default())
        }
    }

    impl ClassicObjectCreateRequest {
        fn type_value(&self) -> u8 {
            match self {
                Self::Topic(_) => TopicSpec::CREATE_TYPE,
                Self::CustomSpu(_) => CustomSpuSpec::CREATE_TYPE,
                Self::SmartModule(_) => SmartModuleSpec::CREATE_TYPE,
                Self::SpuGroup(_) => SpuGroupSpec::CREATE_TYPE,
                Self::TableFormat(_) => TableFormatSpec::CREATE_TYPE,
                Self::MirrorFormat(_) => MirrorSpec::CREATE_TYPE,
            }
        }

        pub(crate) fn type_string(&self) -> &'static str {
            use fluvio_controlplane_metadata::core::Spec;
            match self {
                Self::Topic(_) => crate::topic::TopicSpec::LABEL,
                Self::CustomSpu(_) => crate::customspu::CustomSpuSpec::LABEL,
                Self::SmartModule(_) => crate::smartmodule::SmartModuleSpec::LABEL,
                Self::SpuGroup(_) => crate::spg::SpuGroupSpec::LABEL,
                Self::TableFormat(_) => crate::tableformat::TableFormatSpec::LABEL,
                Self::MirrorFormat(_) => crate::mirror::MirrorSpec::LABEL,
            }
        }
    }

    impl Encoder for ClassicObjectCreateRequest {
        fn write_size(&self, version: Version) -> usize {
            let type_size = (0u8).write_size(version);

            type_size
                + match self {
                    Self::Topic(s) => s.write_size(version),
                    Self::CustomSpu(s) => s.write_size(version),
                    Self::SmartModule(s) => s.write_size(version),
                    Self::SpuGroup(s) => s.write_size(version),
                    Self::TableFormat(s) => s.write_size(version),
                    Self::MirrorFormat(s) => s.write_size(version),
                }
        }

        fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), std::io::Error>
        where
            T: BufMut,
        {
            self.type_value().encode(dest, version)?;
            match self {
                Self::Topic(s) => s.encode(dest, version)?,
                Self::CustomSpu(s) => s.encode(dest, version)?,
                Self::SmartModule(s) => s.encode(dest, version)?,
                Self::SpuGroup(s) => s.encode(dest, version)?,
                Self::TableFormat(s) => s.encode(dest, version)?,
                Self::MirrorFormat(s) => s.encode(dest, version)?,
            }

            Ok(())
        }
    }

    // We implement decode signature even thought this will be never called.
    // RequestMessage use decode_object.  But in order to provide backward compatibility, we pretend
    // to provide decode implementation but should be never called
    impl Decoder for ClassicObjectCreateRequest {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), std::io::Error>
        where
            T: Buf,
        {
            let mut typ: u8 = 0;
            typ.decode(src, version)?;
            tracing::trace!("decoded type: {}", typ);

            match typ {
                TopicSpec::CREATE_TYPE => {
                    tracing::trace!("detected topic");
                    let mut request = TopicSpec::default();
                    request.decode(src, version)?;
                    *self = Self::Topic(request);
                    Ok(())
                }

                TableFormatSpec::CREATE_TYPE => {
                    tracing::trace!("detected table");
                    let mut request = TableFormatSpec::default();
                    request.decode(src, version)?;
                    *self = Self::TableFormat(request);
                    Ok(())
                }

                CustomSpuSpec::CREATE_TYPE => {
                    tracing::trace!("detected custom spu");
                    let mut request = CustomSpuSpec::default();
                    request.decode(src, version)?;
                    *self = Self::CustomSpu(request);
                    Ok(())
                }

                SpuGroupSpec::CREATE_TYPE => {
                    tracing::trace!("detected custom spu");
                    let mut request = SpuGroupSpec::default();
                    request.decode(src, version)?;
                    *self = Self::SpuGroup(request);
                    Ok(())
                }

                SmartModuleSpec::CREATE_TYPE => {
                    tracing::trace!("detected smartmodule");
                    let mut request = SmartModuleSpec::default();
                    request.decode(src, version)?;
                    *self = Self::SmartModule(request);
                    Ok(())
                }

                // Unexpected type
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid create type {typ:#?}"),
                )),
            }
        }
    }

    impl ClassicCreatableAdminSpec for TopicSpec {
        const CREATE_TYPE: u8 = 0;

        fn try_classic_convert(spec: Self) -> anyhow::Result<ClassicObjectCreateRequest> {
            Ok(ClassicObjectCreateRequest::Topic(spec))
        }

        fn try_convert_from_classic(request: ClassicObjectCreateRequest) -> Option<Self> {
            match request {
                ClassicObjectCreateRequest::Topic(spec) => Some(spec),
                _ => None,
            }
        }
    }

    // for classic protocol
    impl ClassicCreatableAdminSpec for TableFormatSpec {
        const CREATE_TYPE: u8 = 5;

        fn try_classic_convert(spec: Self) -> anyhow::Result<ClassicObjectCreateRequest> {
            Ok(ClassicObjectCreateRequest::TableFormat(spec))
        }

        fn try_convert_from_classic(request: ClassicObjectCreateRequest) -> Option<Self> {
            match request {
                ClassicObjectCreateRequest::TableFormat(spec) => Some(spec),
                _ => None,
            }
        }
    }

    impl ClassicCreatableAdminSpec for SpuGroupSpec {
        const CREATE_TYPE: u8 = 2;

        fn try_classic_convert(spec: Self) -> anyhow::Result<ClassicObjectCreateRequest> {
            Ok(ClassicObjectCreateRequest::SpuGroup(spec))
        }

        fn try_convert_from_classic(request: ClassicObjectCreateRequest) -> Option<Self> {
            match request {
                ClassicObjectCreateRequest::SpuGroup(spec) => Some(spec),
                _ => None,
            }
        }
    }

    impl ClassicCreatableAdminSpec for SmartModuleSpec {
        const CREATE_TYPE: u8 = 4;

        fn try_classic_convert(spec: Self) -> anyhow::Result<ClassicObjectCreateRequest> {
            Ok(ClassicObjectCreateRequest::SmartModule(spec))
        }

        fn try_convert_from_classic(request: ClassicObjectCreateRequest) -> Option<Self> {
            match request {
                ClassicObjectCreateRequest::SmartModule(spec) => Some(spec),
                _ => None,
            }
        }
    }

    impl ClassicCreatableAdminSpec for CustomSpuSpec {
        const CREATE_TYPE: u8 = 1;

        fn try_classic_convert(spec: Self) -> anyhow::Result<ClassicObjectCreateRequest> {
            Ok(ClassicObjectCreateRequest::CustomSpu(spec))
        }

        fn try_convert_from_classic(request: ClassicObjectCreateRequest) -> Option<Self> {
            match request {
                ClassicObjectCreateRequest::CustomSpu(spec) => Some(spec),
                _ => None,
            }
        }
    }

    impl ClassicCreatableAdminSpec for MirrorSpec {
        const CREATE_TYPE: u8 = 6;

        fn try_classic_convert(spec: Self) -> anyhow::Result<ClassicObjectCreateRequest> {
            Ok(ClassicObjectCreateRequest::MirrorFormat(spec))
        }

        fn try_convert_from_classic(request: ClassicObjectCreateRequest) -> Option<Self> {
            match request {
                ClassicObjectCreateRequest::MirrorFormat(spec) => Some(spec),
                _ => None,
            }
        }
    }
}
