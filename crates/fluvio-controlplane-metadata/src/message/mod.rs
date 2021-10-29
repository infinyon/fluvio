mod msg_type;
mod replica_msg;
mod smart_module_msg;

pub use self::msg_type::MsgType;
pub use self::msg_type::Message;

pub use self::replica_msg::{ReplicaMsgs, ReplicaMsg};
pub use self::smart_module_msg::{SmartModuleMsgs, SmartModuleMsg};

use crate::spu::SpuSpec;
pub type SpuMsg = Message<SpuSpec>;
