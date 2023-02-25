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
