mod k8_dispatcher;
mod k8_ws_service;

pub use k8_dispatcher::*;
use k8_ws_service::*;

mod k8_actions {

    use crate::core::Spec;
    use crate::store::k8::K8MetaItem;
    use crate::store::MetadataStoreObject;

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
