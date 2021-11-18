mod msg_type;
mod replica_msg;
mod messages;

pub use self::msg_type::MsgType;
pub use self::msg_type::Message;
pub use messages::*;

pub use self::replica_msg::{ReplicaMsgs, ReplicaMsg};
pub use self::smartmodule_msg::{SmartModuleMsgs, SmartModuleMsg};

pub use spu_msg::*;
pub use smartmodule_msg::*;
pub use smart_stream_msg::*;

mod spu_msg {

    use crate::spu::SpuSpec;

    use super::Message;

    pub type SpuMsg = Message<SpuSpec>;
}

mod smartmodule_msg {

    use crate::smartmodule::SmartModule;

    use super::{Message, Messages};

    pub type SmartModuleMsg = Message<SmartModule>;
    pub type SmartModuleMsgs = Messages<SmartModule>;
}

mod smart_stream_msg {

    use std::fmt;

    use dataplane::core::{Encoder, Decoder};
    use fluvio_stream_model::{core::MetadataItem, store::MetadataStoreObject};

    use crate::smartstream::SmartStreamSpec;

    use super::{Message, Messages};

    pub type SmartStreamMsg = Message<SmartStreamControlData>;
    pub type SmartStreamMsgs = Messages<SmartStreamControlData>;

    #[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
    pub struct SmartStreamControlData {
        pub name: String,
        pub spec: SmartStreamSpec,
        pub valid: bool,
    }

    impl fmt::Display for SmartStreamControlData {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "SmartStream({})", self.name)
        }
    }

    impl<C> From<MetadataStoreObject<SmartStreamSpec, C>> for SmartStreamControlData
    where
        C: MetadataItem,
    {
        fn from(mso: MetadataStoreObject<SmartStreamSpec, C>) -> Self {
            let name = mso.key_owned();
            let spec = mso.spec;
            let valid = mso.status.is_deployable();
            Self { name, spec, valid }
        }
    }
}
