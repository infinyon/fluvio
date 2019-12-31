
mod init;
pub mod operator;
pub mod k8_operations;
pub mod k8_events_to_actions;
mod k8_dispatcher;
mod k8_ws_service;

use std::sync::Arc;
use std::convert::TryInto;
use std::convert::TryFrom;
use std::fmt::Display;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::io::ErrorKind;

use k8_client::K8Client;
use k8_client::ClientError;
use k8_metadata::client::TokenStreamResult as OrigTokenStreamResult;
use k8_metadata::core::Spec as K8Spec;
use k8_metadata::core::metadata::K8Obj;
use k8_config::K8Config;

use crate::core::common::KVObject;
use crate::core::common::KvContext;
use crate::core::Spec;

pub use k8_ws_service::K8WSUpdateService;
pub use k8_dispatcher::K8ClusterStateDispatcher;
pub use operator::K8AllChangeDispatcher;

pub type  SharedK8Client = Arc<K8Client>;
pub type  K8TokenStreamResult<S,P> = OrigTokenStreamResult<S,P,ClientError>;

pub fn new_shared(config: K8Config) -> SharedK8Client {
    let client= K8Client::new(config).expect("Error: K8 client failed to initialize!");
    Arc::new(client)
}

pub use init::main_k8_loop;


