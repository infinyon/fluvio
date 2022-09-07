use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::dispatcher::core::Spec;
use crate::dispatcher::core::Status;
use crate::stores::connector::ManagedConnectorSpec;

pub use k8_types::app::deployment::DeploymentStatus as K8DeploymentStatus;
pub use k8_types::app::deployment::DeploymentSpec as K8DeploymentSpec;

/// Statefulset Spec
#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(transparent)]
pub struct ManagedConnectorDeploymentSpec(K8DeploymentSpec);

impl Spec for ManagedConnectorDeploymentSpec {
    const LABEL: &'static str = "Deployment";
    type IndexKey = String;
    type Status = DeploymentStatus;
    type Owner = ManagedConnectorSpec;
}

impl From<K8DeploymentSpec> for ManagedConnectorDeploymentSpec {
    fn from(k8: K8DeploymentSpec) -> Self {
        Self(k8)
    }
}

impl From<ManagedConnectorDeploymentSpec> for K8DeploymentSpec {
    fn from(spec: ManagedConnectorDeploymentSpec) -> Self {
        spec.0
    }
}

/// Statefulset Spec
#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(transparent)]
pub struct DeploymentStatus(pub K8DeploymentStatus);

impl Status for DeploymentStatus {}

impl fmt::Display for DeploymentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

impl From<K8DeploymentStatus> for DeploymentStatus {
    fn from(k8: K8DeploymentStatus) -> Self {
        Self(k8)
    }
}

impl From<DeploymentStatus> for K8DeploymentStatus {
    fn from(status: DeploymentStatus) -> Self {
        status.0
    }
}

mod extended {

    use k8_types::K8Obj;

    use tracing::trace;

    use crate::stores::k8::K8ConvertError;
    use crate::stores::k8::K8ExtendedSpec;
    use crate::stores::k8::K8MetaItem;
    use crate::stores::MetadataStoreObject;
    use crate::stores::k8::default_convert_from_k8;
    use fluvio_controlplane_metadata::k8_types::Spec;
    use fluvio_controlplane_metadata::connector::K8ManagedConnectorSpec;

    use super::*;

    impl K8ExtendedSpec for ManagedConnectorDeploymentSpec {
        type K8Spec = K8DeploymentSpec;
        type K8Status = K8DeploymentStatus;

        fn convert_from_k8(
            k8_obj: K8Obj<Self::K8Spec>,
            multi_namespace_context: bool,
        ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>> {
            if k8_obj
                .metadata
                .owner_references
                .iter()
                .any(|v| v.kind == K8ManagedConnectorSpec::metadata().names.kind)
            {
                trace!("converting k8 managed connector: {:#?}", k8_obj);
                default_convert_from_k8(k8_obj, multi_namespace_context)
            } else {
                Err(K8ConvertError::Skip(k8_obj))
            }
        }
    }
}
