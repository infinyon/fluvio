mod k8_dispatcher;
mod k8_ws_service;
mod actions;

pub use k8_dispatcher::*;
use k8_ws_service::*;
pub use actions::*;

mod k8_actions {

    use flv_metadata_cluster::core::*;
    use flv_metadata_cluster::store::*;
    use crate::stores::*;

    /// Actions to update World States
    #[derive(Debug, PartialEq, Clone)]
    pub enum K8Action<S>
    where
        S: Spec,
    {
        Apply(MetadataStoreObject<S, K8MetaItem>),
        UpdateStatus((S::Status, K8MetaItem)),
        UpdateSpec((S, K8MetaItem)),
        Delete(K8MetaItem),
    }
}
