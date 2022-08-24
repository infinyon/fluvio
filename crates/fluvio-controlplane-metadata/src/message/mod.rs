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

    use fluvio_protocol::{Encoder, Decoder};
    use fluvio_stream_model::{core::MetadataItem, store::MetadataStoreObject};

    use crate::derivedstream::DerivedStreamSpec;

    use super::{Message, Messages};

    pub type DerivedStreamMsg = Message<DerivedStreamControlData>;
    pub type DerivedStreamMsgs = Messages<DerivedStreamControlData>;

    #[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
    pub struct DerivedStreamControlData {
        pub name: String,
        pub spec: DerivedStreamSpec,
        pub valid: bool,
    }

    impl fmt::Display for DerivedStreamControlData {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "DerivedStream({})", self.name)
        }
    }

    impl<C> From<MetadataStoreObject<DerivedStreamSpec, C>> for DerivedStreamControlData
    where
        C: MetadataItem,
    {
        fn from(mso: MetadataStoreObject<DerivedStreamSpec, C>) -> Self {
            let name = mso.key_owned();
            let spec = mso.spec;
            let valid = mso.status.is_deployable();
            Self { name, spec, valid }
        }
    }
}
