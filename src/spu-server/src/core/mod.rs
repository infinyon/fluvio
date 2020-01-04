
mod global_context;
mod store;
pub(crate) mod storage;

pub mod spus;
pub mod replica;

pub use self::global_context::GlobalContext;
pub use self::store::Spec;
pub use self::store::LocalStore;
pub use self::store::SpecChange;

pub use self::spus::SpuLocalStore;
pub use self::replica::SharedReplicaLocalStore;

use std::sync::Arc;
use ::flv_storage::FileReplica;
use kf_socket::SinkPool;
use types::SpuId;
use crate::config::SpuConfig;

pub type SharedGlobalContext<S> = Arc<GlobalContext<S>>;
pub type DefaultSharedGlobalContext = SharedGlobalContext<FileReplica>;
pub type SharedSpuSinks = Arc<SinkPool<SpuId>>;
pub type SharedSpuConfig = Arc<SpuConfig>;

pub use event::OffsetUpdateEvent;


mod event {

    use kf_protocol::api::Offset;
    use flv_metadata::partition::ReplicaKey;

        /// used for communicating change in offset for any replica
    #[derive(Debug)]
    pub struct OffsetUpdateEvent {
        pub replica_id: ReplicaKey,
        pub leo: Offset,            
        pub hw: Offset             
    }
}