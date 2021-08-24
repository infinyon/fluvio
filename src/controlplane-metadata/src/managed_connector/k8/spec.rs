//!
//! # SPU Spec
//!
//! Interface to the SPU metadata spec in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use fluvio_types::defaults::{SPU_PUBLIC_PORT, SPU_LOG_BASE_DIR, SPU_LOG_SIZE};
use fluvio_types::defaults::SPU_PRIVATE_PORT;

use crate::spu::EncryptionEnum;
use super::super::ManagedConnectorStatus;
use crate::k8_types::{Spec, Crd, DefaultHeader, TemplateSpec, Env};

use crd::MANAGED_CONNECTOR_API;
mod crd {

    use crate::k8_types::{Crd, CrdNames, GROUP, V1};

    pub const MANAGED_CONNECTOR_API: Crd = Crd {
        group: GROUP,
        version: V1,
        names: CrdNames {
            kind: "ManagedConnector",
            plural: "managedconnectors",
            singular: "managedconnector",
        },
    };
}

impl Spec for K8ManagedConnectorSpec {
    type Status = ManagedConnectorStatus;
    type Header = DefaultHeader;
    fn metadata() -> &'static Crd {
        &MANAGED_CONNECTOR_API
    }
}
use super::ManagedConnectorConfig;

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct K8ManagedConnectorSpec {
    pub name: String,
    pub config: ManagedConnectorConfig,
}
mod convert {

    use crate::managed_connector::*;

    use super::*;

    impl From<K8ManagedConnectorSpec> for ManagedConnectorSpec {
        fn from(spec: K8ManagedConnectorSpec) -> Self {
            Self {
                name: spec.name,
                config: spec.config,
            }
        }
    }
    /*
    impl From<SpuTemplate> for SpuConfig {
        fn from(template: SpuTemplate) -> Self {
            Self {
                rack: template.rack,
                replication: template.replication.map(|r| r.into()),
                storage: template.storage.map(|s| s.into()),
                env: vec![], // doesn't really matter
            }
        }
    }
    */

    impl From<ManagedConnectorSpec> for K8ManagedConnectorSpec {
        fn from(spec: ManagedConnectorSpec) -> Self {
            Self {
                name: spec.name,
                config: spec.config,
            }
        }
    }


}
