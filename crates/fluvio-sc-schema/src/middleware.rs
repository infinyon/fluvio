/// Middleware to encode and decode object types
/// There are 2 types of middleware used in the Admin
/// ObjectDecoder which uses string from Spec Label
/// CreateDecoder which uses u8 enum
use dataplane::api::RequestMiddleWare;
use dataplane::core::{Encoder, Decoder};

use crate::customspu::CustomSpuSpec;
use crate::AdminSpec;
use crate::topic::TopicSpec;
use crate::spu::{SpuSpec};
use crate::smartmodule::SmartModuleSpec;
use crate::partition::PartitionSpec;
use crate::table::TableSpec;
use crate::spg::SpuGroupSpec;
use crate::connector::ManagedConnectorSpec;

use crate::core::Spec;
pub(crate) trait AdminObjectDecoder {
    fn is_topic(&self) -> bool;
    fn is_table(&self) -> bool;
    fn is_spu(&self) -> bool;
    fn is_partition(&self) -> bool;
    fn is_smart_module(&self) -> bool;
    fn is_custom_spu(&self) -> bool;
    fn is_spg(&self) -> bool;
    fn is_connector(&self) -> bool;
}

#[derive(Debug, Clone, Default, PartialEq, Encoder, Decoder)]
pub struct ObjectDecoder {
    ty: String,
}

impl ObjectDecoder {
    pub fn new<S>() -> Self
    where
        S: AdminSpec,
    {
        Self {
            ty: S::LABEL.to_string(),
        }
    }
}

impl RequestMiddleWare for ObjectDecoder {}

impl AdminObjectDecoder for ObjectDecoder {
    fn is_topic(&self) -> bool {
        self.ty == TopicSpec::LABEL
    }

    fn is_spu(&self) -> bool {
        self.ty == SpuSpec::LABEL
    }

    fn is_partition(&self) -> bool {
        self.ty == PartitionSpec::LABEL
    }

    fn is_smart_module(&self) -> bool {
        self.ty == SmartModuleSpec::LABEL
    }

    fn is_custom_spu(&self) -> bool {
        self.ty == CustomSpuSpec::LABEL
    }

    fn is_table(&self) -> bool {
        self.ty == TableSpec::LABEL
    }

    fn is_spg(&self) -> bool {
        self.ty == SpuGroupSpec::LABEL
    }

    fn is_connector(&self) -> bool {
        self.ty == ManagedConnectorSpec::LABEL
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Encoder, Decoder)]
pub enum CreateDecoder {
    #[fluvio(tag = 0)]
    TOPIC,
    #[fluvio(tag = 1)]
    CustomSpu,
    #[fluvio(tag = 2)]
    SPG = 2,
    #[fluvio(tag = 3)]
    ManagedConnector,
    #[fluvio(tag = 4)]
    SmartModule,
    #[fluvio(tag = 5)]
    TABLE,
}

impl Default for CreateDecoder {
    fn default() -> Self {
        Self::TOPIC
    }
}

impl RequestMiddleWare for CreateDecoder {}

impl AdminObjectDecoder for CreateDecoder {
    fn is_topic(&self) -> bool {
        matches!(self, Self::TOPIC)
    }

    fn is_spu(&self) -> bool {
        false
    }

    fn is_partition(&self) -> bool {
        false
    }

    fn is_smart_module(&self) -> bool {
        matches!(self, Self::SmartModule)
    }

    fn is_custom_spu(&self) -> bool {
        matches!(self, Self::CustomSpu)
    }

    fn is_table(&self) -> bool {
        matches!(self, Self::TABLE)
    }

    fn is_spg(&self) -> bool {
        matches!(self, Self::SPG)
    }

    fn is_connector(&self) -> bool {
        matches!(self, Self::ManagedConnector)
    }
}
