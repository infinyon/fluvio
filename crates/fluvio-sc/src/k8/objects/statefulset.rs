use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::dispatcher::core::Spec;
use crate::dispatcher::core::Status;
use crate::stores::spg::SpuGroupSpec;

pub use k8_types::app::stateful::StatefulSetSpec as K8StatefulSetSpec;
pub use k8_types::app::stateful::StatefulSetStatus as K8StatefulSetStatus;

/// Statefulset Spec
#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(transparent)]
pub struct StatefulsetSpec(K8StatefulSetSpec);

impl Spec for StatefulsetSpec {
    const LABEL: &'static str = "StatefulSet";
    type IndexKey = String;
    type Status = StatefulsetStatus;
    type Owner = SpuGroupSpec;
}

impl From<K8StatefulSetSpec> for StatefulsetSpec {
    fn from(k8: K8StatefulSetSpec) -> Self {
        Self(k8)
    }
}

impl From<StatefulsetSpec> for K8StatefulSetSpec {
    fn from(spec: StatefulsetSpec) -> Self {
        spec.0
    }
}

/// Statefulset Spec
#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(transparent)]
pub struct StatefulsetStatus(K8StatefulSetStatus);

impl Status for StatefulsetStatus {}

impl fmt::Display for StatefulsetStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

impl From<K8StatefulSetStatus> for StatefulsetStatus {
    fn from(k8: K8StatefulSetStatus) -> Self {
        Self(k8)
    }
}

impl From<StatefulsetStatus> for K8StatefulSetStatus {
    fn from(status: StatefulsetStatus) -> Self {
        status.0
    }
}

mod extended {

    use k8_types::K8Obj;

    use crate::stores::k8::K8ConvertError;
    use crate::stores::k8::K8ExtendedSpec;
    use crate::stores::k8::K8MetaItem;
    use crate::stores::MetadataStoreObject;
    use crate::stores::k8::default_convert_from_k8;

    use super::*;

    impl K8ExtendedSpec for StatefulsetSpec {
        type K8Spec = K8StatefulSetSpec;
        type K8Status = K8StatefulSetStatus;

        fn convert_from_k8(
            k8_obj: K8Obj<Self::K8Spec>,
            multi_namespace_context: bool,
        ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>> {
            default_convert_from_k8(k8_obj, multi_namespace_context)
        }
    }
}
