mod msg_type;
mod replica_msg;

pub use self::msg_type::MsgType;
pub use self::msg_type::Message;

pub use self::replica_msg::ReplicaMsg;
pub use self::replica_msg::ReplicaMsgs;

use crate::spu::SpuSpec;
pub type SpuMsg = Message<SpuSpec>;
