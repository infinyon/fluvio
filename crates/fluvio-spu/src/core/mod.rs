mod global_context;
mod store;
mod leader_client;

pub mod spus;
pub mod replica;
pub mod smartmodule;
pub mod metrics;

pub use self::global_context::{GlobalContext, ReplicaChange};
pub use self::store::Spec;
pub use self::store::LocalStore;
pub use self::store::SpecChange;

pub use self::spus::SpuLocalStore;
pub use self::replica::SharedReplicaLocalStore;

use std::sync::Arc;
use ::fluvio_storage::FileReplica;
use crate::config::SpuConfig;

pub type SharedGlobalContext<S> = Arc<GlobalContext<S>>;
pub type DefaultSharedGlobalContext = SharedGlobalContext<FileReplica>;
pub type SharedSpuConfig = Arc<SpuConfig>;
pub type FileGlobalContext = GlobalContext<FileReplica>;
