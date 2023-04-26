use fluvio_protocol::{Encoder};

use crate::{
    objects::{ListRequest, ListResponse},
};

/// carry from prev version for compatibility test
macro_rules! ObjectApiEnum {
    ($api:ident) => {
        paste::paste! {


            #[derive(Debug)]
            pub enum [<ObjectApiOld $api>] {
                Topic($api<crate::topic::TopicSpec>),
                Spu($api<crate::spu::SpuSpec>),
                CustomSpu($api<crate::customspu::CustomSpuSpec>),
                SmartModule($api<crate::smartmodule::SmartModuleSpec>),
                Partition($api<crate::partition::PartitionSpec>),
                SpuGroup($api<crate::spg::SpuGroupSpec>),
                TableFormat($api<crate::tableformat::TableFormatSpec>),
            }


            impl [<ObjectApiOld $api>] {
                pub(crate) fn new(typ: &str) -> Result<Self,std::io::Error> {
                    use fluvio_controlplane_metadata::core::Spec;
                    match typ {
                        crate::topic::TopicSpec::LABEL => Ok(Self::Topic($api::default())),
                        crate::spu::SpuSpec::LABEL =>  Ok(Self::Spu($api::default())),
                        crate::tableformat::TableFormatSpec::LABEL => Ok(Self::TableFormat($api::default())),
                        crate::customspu::CustomSpuSpec::LABEL  => Ok(Self::CustomSpu($api::default())),
                        crate::spg::SpuGroupSpec::LABEL  => Ok(Self::SpuGroup($api::default())),
                        crate::smartmodule::SmartModuleSpec::LABEL => Ok(Self::SmartModule($api::default())),
                        crate::partition::PartitionSpec::LABEL => Ok(Self::Partition($api::default())),

                        // Unexpected type
                        _ => Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid object type {:#?}", typ),
                        ))

                    }
                }
            }

            impl   [<ObjectApiOld $api>] {

                pub(crate) fn write_size(&self, version: fluvio_protocol::Version) -> usize {
                   match self {
                                Self::Topic(s) => s.write_size(version),
                                Self::Spu(s) => s.write_size(version),
                                Self::CustomSpu(s) => s.write_size(version),
                                Self::Partition(s) => s.write_size(version),
                                Self::SmartModule(s) => s.write_size(version),
                                Self::SpuGroup(s) => s.write_size(version),
                                Self::TableFormat(s) => s.write_size(version),
                    }
                }


            }

        }
    };
}

ObjectApiEnum!(ListRequest);
ObjectApiEnum!(ListResponse);
