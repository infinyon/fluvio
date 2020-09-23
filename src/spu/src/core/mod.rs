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
use ::fluvio_storage::FileReplica;
use kf_socket::SinkPool;
use fluvio_types::SpuId;
use crate::config::SpuConfig;

pub type SharedGlobalContext<S> = Arc<GlobalContext<S>>;
pub type DefaultSharedGlobalContext = SharedGlobalContext<FileReplica>;
pub type SharedSpuSinks = Arc<SinkPool<SpuId>>;
pub type SharedSpuConfig = Arc<SpuConfig>;

pub use event::OffsetUpdateEvent;

mod event {

    use dataplane::Offset;
    use fluvio_controlplane_metadata::partition::ReplicaKey;

    /// used for communicating change in offset for any replica
    #[derive(Debug, Clone)]
    pub struct OffsetUpdateEvent {
        pub replica_id: ReplicaKey,
        pub leo: Offset,
        pub hw: Offset,
    }
}

mod broadcast {

    pub use tokio::sync::broadcast::Receiver;
    pub use tokio::sync::broadcast::Sender;
    pub use tokio::sync::broadcast::channel;
    pub use tokio::sync::broadcast::RecvError;

    #[derive(Debug)]
    pub struct Channel<T> {
        receiver: Receiver<T>,
        sender: Sender<T>,
    }

    impl<T> Channel<T> {
        pub fn new(capacity: usize) -> Self {
            let (sender, receiver) = channel(capacity);
            Self { receiver, sender }
        }

        /// create new clone of sender
        pub fn receiver(&self) -> Receiver<T> {
            self.sender.subscribe()
        }

        pub fn sender(&self) -> Sender<T> {
            self.sender.clone()
        }
    }
}
