mod global_context;
mod store;
mod leader_client;

pub mod spus;
pub mod replica;
pub mod smartmodule;
pub mod derivedstream;

pub(crate) use self::global_context::*;
pub(crate) use self::store::Spec;
pub(crate) use self::store::LocalStore;
pub(crate) use self::store::SpecChange;

pub(crate) use self::spus::SpuLocalStore;
pub(crate) use self::replica::SharedReplicaLocalStore;

use std::sync::Arc;
use ::fluvio_storage::FileReplica;
use crate::config::SpuConfig;

pub(crate) type SharedGlobalContext<S> = Arc<GlobalContext<S>>;
pub(crate) type DefaultSharedGlobalContext = SharedGlobalContext<FileReplica>;
pub(crate) type SharedSpuConfig = Arc<SpuConfig>;
pub(crate) type FileGlobalContext = GlobalContext<FileReplica>;
