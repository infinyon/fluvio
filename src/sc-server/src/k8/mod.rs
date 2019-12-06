
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
pub type  TokenStreamResult<S,P> = OrigTokenStreamResult<S,P,ClientError>;

pub fn new_shared(config: K8Config) -> SharedK8Client {
    let client= K8Client::new(config).expect("Error: K8 client failed to initialize!");
    Arc::new(client)
}



 pub fn default_convert_from_k8<S>(k8_obj: K8Obj<S::K8Spec,<S::K8Spec as K8Spec>::Status>) -> 
            Result<KVObject<S>,IoError>

    where
        S: Spec,
        <S::K8Spec as K8Spec>::Status : Into<S::Status>,
        S::K8Spec:  Into<S>,
        S::Key: TryFrom<String> + Display,
        <<S as Spec>::Key as TryFrom<String>>::Error: Debug
    {
        let k8_name = k8_obj.metadata.name.clone();
        let result: Result<S::Key,_>  = k8_name.try_into();
        match result {
            Ok(key) => {
                
                // convert K8 Spec/Status into Metadata Spec/Status
                let local_spec = k8_obj.spec.into();
                let local_status = if let Some(status) = k8_obj.status {
                    status.into()
                } else {
                    <S::Status>::default()
                };

                // grab KV ctx and create AuthToken
                let ctx = KvContext::default().with_ctx(k8_obj.metadata);
                let loca_kv = KVObject::new(key,local_spec, local_status).with_kv_ctx(ctx);

                Ok(loca_kv)
            
            },
            Err(err) => {
                Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "error converting key: {:#?}",
                        err)
                ))
            }
        }

}

