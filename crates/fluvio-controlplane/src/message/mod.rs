mod replica_msg;

pub use self::replica_msg::{ReplicaMsgs, ReplicaMsg};
pub use self::smartmodule_msg::{SmartModuleMsgs, SmartModuleMsg};

pub use spu_msg::*;
pub use smartmodule_msg::*;

mod spu_msg {
    use fluvio_controlplane_metadata::{spu::SpuSpec, message::Message};

    pub type SpuMsg = Message<SpuSpec>;
}

mod smartmodule_msg {
    use fluvio_controlplane_metadata::{
        smartmodule::SmartModule,
        message::{Message, Messages},
    };

    pub type SmartModuleMsg = Message<SmartModule>;
    pub type SmartModuleMsgs = Messages<SmartModule>;
}
