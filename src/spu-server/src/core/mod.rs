
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
use ::storage::FileReplica;
use kf_socket::SinkPool;
use types::SpuId;
use crate::config::SpuConfig;

pub type SharedGlobalContext<S> = Arc<GlobalContext<S>>;
pub type DefaultSharedGlobalContext = SharedGlobalContext<FileReplica>;
pub type SharedSpuSinks = Arc<SinkPool<SpuId>>;
pub type SharedSpuConfig = Arc<SpuConfig>;
